"""Executable S3 compatibility and durability probes; credentials come from the environment."""
import argparse
import hashlib
import json
import os
import socket
import subprocess
import time
import urllib.request
from urllib.parse import urlparse
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

ENDPOINT = os.environ['AWS_ENDPOINT_URL'].rstrip('/')
REGION = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
BUCKET = os.environ.get('SMOKE_BUCKET', 'dwv-s3-smoke')
S3 = boto3.client('s3', endpoint_url=ENDPOINT, region_name=REGION,
                  config=Config(signature_version='s3v4', s3={'addressing_style': 'path'},
                                connect_timeout=3, read_timeout=20, retries={'max_attempts': 2}))


def report(case, **result):
    print(json.dumps({'time': datetime.now(timezone.utc).isoformat(), 'case': case, **result}), flush=True)


def read(response):
    stream = response['Body']
    try:
        return stream.read()
    finally:
        stream.close()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def payload(size):
    block = bytes(range(256))
    return (block * (size // len(block) + 1))[:size]


def expect_error(status, operation, **kwargs):
    try:
        operation(**kwargs)
    except ClientError as error:
        actual = error.response['ResponseMetadata']['HTTPStatusCode']
        assert actual == status, (actual, error.response['Error']['Code'])
        return error.response['Error']['Code']
    raise AssertionError('Expected HTTP ' + str(status))


def bootstrap():
    deadline = time.monotonic() + 180
    while True:
        try:
            S3.list_buckets()
            break
        except (EndpointConnectionError, ClientError, OSError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(2)
    for bucket in os.environ.get('BUCKETS', 'dwv-s3-smoke,dwv-accumulo-lab').split(','):
        try:
            S3.head_bucket(Bucket=bucket)
        except ClientError as error:
            if error.response['ResponseMetadata']['HTTPStatusCode'] != 404:
                raise
            try:
                settings = {'Bucket': bucket}
                if REGION != 'us-east-1':
                    settings['CreateBucketConfiguration'] = {'LocationConstraint': REGION}
                S3.create_bucket(**settings)
            except ClientError as conflict:
                if conflict.response['Error']['Code'] != 'BucketAlreadyOwnedByYou':
                    raise
            S3.head_bucket(Bucket=bucket)
        report('bootstrap-bucket', bucket=bucket, status='pass')
    # Use the actual AWS CLI in addition to boto3; no shell or credential arguments.
    result = subprocess.run(['aws', '--endpoint-url', ENDPOINT, '--region', REGION,
                             's3api', 'list-buckets', '--query', 'Buckets[].Name', '--output', 'json'],
                            capture_output=True, text=True, check=True, timeout=30)
    report('aws-cli-list-buckets', status='pass', buckets=json.loads(result.stdout))


def connectivity():
    host = urlparse(ENDPOINT).hostname
    addresses = sorted({item[4][0] for item in socket.getaddrinfo(host, None)})
    with urllib.request.urlopen(ENDPOINT + '/health', timeout=10) as response:
        health = json.load(response)
    S3.head_bucket(Bucket=BUCKET)
    report('node-dns-signed-s3-head', status='pass', node=os.environ.get('NODE_NAME', 'unknown'),
           endpoint=ENDPOINT, addresses=addresses, version=health.get('version'), services=health.get('services'))


def api():
    prefix = 'phase1-api/'
    body = payload(65536)
    key = prefix + 'payload.bin'
    S3.put_object(Bucket=BUCKET, Key=key, Body=body, Metadata={'sha256': digest(body)})
    head = S3.head_object(Bucket=BUCKET, Key=key)
    assert head['ContentLength'] == len(body) and head['Metadata']['sha256'] == digest(body)
    assert read(S3.get_object(Bucket=BUCKET, Key=key)) == body
    report('put-head-get-sha256', status='pass', sha256=digest(body), etag=head['ETag'])

    result = S3.get_object(Bucket=BUCKET, Key=key, Range='bytes=7-63')
    assert result['ResponseMetadata']['HTTPStatusCode'] == 206
    assert result['ContentRange'] == 'bytes 7-63/65536'
    assert result['ContentLength'] == 57 and result.get('AcceptRanges') == 'bytes'
    assert read(result) == body[7:64]
    expect_error(416, S3.get_object, Bucket=BUCKET, Key=key, Range='bytes=70000-71000')
    report('range-and-unsatisfiable-range', status='pass')

    assert read(S3.get_object(Bucket=BUCKET, Key=key, IfMatch=head['ETag'])) == body
    expect_error(412, S3.get_object, Bucket=BUCKET, Key=key, IfMatch='"not-the-current-etag"')
    expect_error(304, S3.get_object, Bucket=BUCKET, Key=key, IfNoneMatch=head['ETag'])
    report('etag-conditional-reads', status='pass')

    empty = prefix + 'empty'
    S3.put_object(Bucket=BUCKET, Key=empty, Body=b'')
    assert S3.head_object(Bucket=BUCKET, Key=empty)['ContentLength'] == 0
    assert read(S3.get_object(Bucket=BUCKET, Key=empty)) == b''
    report('zero-byte-object', status='pass')

    copied = prefix + 'copy.bin'
    S3.copy_object(Bucket=BUCKET, Key=copied, CopySource={'Bucket': BUCKET, 'Key': key}, CopySourceIfMatch=head['ETag'])
    assert read(S3.get_object(Bucket=BUCKET, Key=copied)) == body
    S3.delete_object(Bucket=BUCKET, Key=key)
    expect_error(404, S3.head_object, Bucket=BUCKET, Key=key)
    assert read(S3.get_object(Bucket=BUCKET, Key=copied)) == body
    report('conditional-copy-and-delete-rename', status='pass')

    for i in range(5):
        S3.put_object(Bucket=BUCKET, Key=prefix + 'list/' + str(i), Body=bytes([i]))
    pages = list(S3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=prefix+'list/', PaginationConfig={'PageSize': 2}))
    keys = [obj['Key'] for page in pages for obj in page.get('Contents', [])]
    assert sorted(keys) == [prefix + 'list/' + str(i) for i in range(5)]
    assert len(pages) == 3 and pages[0]['IsTruncated']
    for k in keys:
        assert S3.head_object(Bucket=BUCKET, Key=k)['ContentLength'] == 1
    report('list-v2-pagination-and-read-after-write', status='pass', pages=len(pages), keys=len(keys))

    multi = prefix + 'multipart.bin'
    upload = S3.create_multipart_upload(Bucket=BUCKET, Key=multi)['UploadId']
    listed = S3.list_multipart_uploads(Bucket=BUCKET, Prefix=prefix)
    assert any(item['UploadId'] == upload for item in listed.get('Uploads', []))
    parts, expected = [], bytearray()
    for number, size in [(1, 5*1024*1024), (2, 1024*1024)]:
        data = payload(size)
        response = S3.upload_part(Bucket=BUCKET, Key=multi, UploadId=upload, PartNumber=number, Body=data)
        parts.append({'PartNumber': number, 'ETag': response['ETag']})
        expected.extend(data)
    S3.complete_multipart_upload(Bucket=BUCKET, Key=multi, UploadId=upload, MultipartUpload={'Parts': parts})
    assert digest(read(S3.get_object(Bucket=BUCKET, Key=multi))) == digest(expected)
    S3.copy_object(Bucket=BUCKET, Key='durability/multipart.bin', CopySource={'Bucket': BUCKET, 'Key': multi})
    report('multipart-completion-sha256', status='pass', bytes=len(expected), sha256=digest(expected))

    aborted = S3.create_multipart_upload(Bucket=BUCKET, Key=prefix+'aborted')['UploadId']
    S3.upload_part(Bucket=BUCKET, Key=prefix+'aborted', UploadId=aborted, PartNumber=1, Body=b'abort-me')
    S3.abort_multipart_upload(Bucket=BUCKET, Key=prefix+'aborted', UploadId=aborted)
    assert all(x['UploadId'] != aborted for x in S3.list_multipart_uploads(Bucket=BUCKET, Prefix=prefix).get('Uploads', []))
    expect_error(404, S3.head_object, Bucket=BUCKET, Key=prefix+'aborted')
    report('multipart-abort-cleanup', status='pass')

    objects = [{'Key': x['Key']} for page in S3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=prefix) for x in page.get('Contents', [])]
    result = S3.delete_objects(Bucket=BUCKET, Delete={'Objects': objects})
    assert not result.get('Errors')
    assert S3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get('KeyCount', 0) == 0
    report('multi-delete-and-list-consistency', status='pass', deleted=len(objects))

    temporary = 'dwv-phase1-empty-check'
    S3.create_bucket(Bucket=temporary)
    S3.put_object(Bucket=temporary, Key='occupied', Body=b'x')
    expect_error(409, S3.delete_bucket, Bucket=temporary)
    S3.delete_object(Bucket=temporary, Key='occupied')
    S3.delete_bucket(Bucket=temporary)
    expect_error(404, S3.head_bucket, Bucket=temporary)
    report('bucket-lifecycle-and-errors', status='pass')
    report('api-suite', status='pass')


def seed(stage):
    entries = []
    for name, size in [('empty.bin', 0), ('small.bin', 65536), ('large.bin', 6*1024*1024)]:
        key, body = 'durability/'+stage+'/'+name, payload(size)
        S3.put_object(Bucket=BUCKET, Key=key, Body=body, Metadata={'sha256': digest(body)})
        entries.append({'key': key, 'size': size, 'sha256': digest(body)})
    report('sentinel-seed', status='pass', stage=stage, objects=entries)


def verify(stages):
    for bucket in os.environ.get('BUCKETS', 'dwv-s3-smoke,dwv-accumulo-lab').split(','):
        S3.head_bucket(Bucket=bucket)
    entries = []
    for stage in stages.split(','):
        for name, size in [('empty.bin', 0), ('small.bin', 65536), ('large.bin', 6*1024*1024)]:
            key, body = 'durability/'+stage+'/'+name, payload(size)
            head = S3.head_object(Bucket=BUCKET, Key=key)
            actual = read(S3.get_object(Bucket=BUCKET, Key=key))
            assert head['ContentLength'] == size and head['Metadata']['sha256'] == digest(body)
            assert digest(actual) == digest(body), key
            if size:
                assert read(S3.get_object(Bucket=BUCKET, Key=key, Range='bytes=11-31')) == body[11:32]
            entries.append({'key': key, 'size': size, 'sha256': digest(actual)})
    expected = payload(5*1024*1024) + payload(1024*1024)
    assert digest(read(S3.get_object(Bucket=BUCKET, Key='durability/multipart.bin'))) == digest(expected)
    report('sentinel-verify', status='pass', stages=stages, objects=entries, multipartSha256=digest(expected))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['bootstrap', 'connect', 'api', 'seed', 'verify'])
    parser.add_argument('--stage', default='baseline')
    parser.add_argument('--stages', default='baseline')
    args = parser.parse_args()
    if args.mode == 'bootstrap': bootstrap()
    elif args.mode == 'connect': connectivity()
    elif args.mode == 'api': api()
    elif args.mode == 'seed': seed(args.stage)
    else: verify(args.stages)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        report('probe-failure', status='fail', errorType=type(error).__name__, error=str(error))
        raise
