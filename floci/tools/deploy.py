#!/usr/bin/env python3
"""Deploy and verify the driver's independent Floci release without migrating data."""
import argparse
import base64
import json
import secrets
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

try:
    import yaml
except ImportError:
    print('Floci setup requires PyYAML: python3 -m pip install PyYAML', file=sys.stderr)
    sys.exit(1)

ROOT = Path(__file__).resolve().parents[1]


def run(*args, stdin=None):
    result = subprocess.run(args, input=stdin, text=True, capture_output=True)
    if result.returncode:
        # Never include stdin: it may contain Secret data.
        raise RuntimeError(f"{' '.join(args)} failed:\n{result.stderr}")
    return result.stdout


def get(kind, name, namespace):
    text = run('kubectl', '-n', namespace, 'get', kind, name, '--ignore-not-found', '-o', 'json')
    return json.loads(text) if text.strip() else None


def apply(obj):
    run('kubectl', 'apply', '-f', '-', stdin=json.dumps(obj))


def merge(base, overlay):
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            merge(base[key], value)
        else:
            base[key] = value
    return base


def credentials(secret):
    if not secret:
        return None
    result = {key: secret.get('data', {}).get(key, '') for key in
              ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']}
    if not all(base64.b64decode(value) for value in result.values()):
        raise RuntimeError('Existing AWS Secret must contain both nonempty credential keys')
    return result


def wait_job(namespace, name, timeout=360):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = get('job', name, namespace)
        conditions = job.get('status', {}).get('conditions', []) if job else []
        if any(c['type'] == 'Failed' and c['status'] == 'True' for c in conditions):
            raise RuntimeError(f'Job {namespace}/{name} failed; inspect kubectl logs -n {namespace} job/{name}')
        if any(c['type'] == 'Complete' and c['status'] == 'True' for c in conditions):
            return run('kubectl', '-n', namespace, 'logs', 'job/' + name)
        time.sleep(2)
    raise RuntimeError(f'Timed out waiting for Job {namespace}/{name}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--namespace', default='aws-emulation')
    parser.add_argument('--release', default='floci')
    parser.add_argument('--client-namespace', default='default')
    parser.add_argument('--values', type=Path)
    parser.add_argument('--bucket', action='append', default=[], help='Ensure an additional application bucket exists')
    parser.add_argument('--check', action='store_true', help='Read-only render/preflight; do not deploy or build images')
    parser.add_argument('--evidence', type=Path)
    args = parser.parse_args()
    values = yaml.safe_load((ROOT / 'values.yaml').read_text())
    releases = json.loads(run('helm', 'list', '-n', args.namespace, '--all', '-o', 'json'))
    if any(item['name'] == args.release for item in releases):
        merge(values, json.loads(run('helm', 'get', 'values', args.release, '-n', args.namespace, '-o', 'json')) or {})
    if args.values:
        merge(values, yaml.safe_load(args.values.read_text()) or {})
    values['bootstrap']['buckets'] = list(dict.fromkeys(values['bootstrap']['buckets'] + args.bucket))
    if not values['bootstrap']['buckets']:
        raise RuntimeError('At least one Floci bootstrap bucket is required')
    values['bootstrap']['enabled'] = True
    run_id = 'driver-' + uuid.uuid4().hex[:10]
    values['bootstrap']['runId'] = run_id
    with tempfile.TemporaryDirectory(prefix='datawave-floci-') as temporary:
        effective = Path(temporary) / 'values.yaml'
        effective.write_text(yaml.safe_dump(values))
        documents = list(yaml.safe_load_all(run('helm', 'template', args.release, str(ROOT), '-n', args.namespace, '-f', str(effective))))
        stateful = next(d for d in documents if d and d['kind'] == 'StatefulSet')
        bootstrap = next(d for d in documents if d and d['kind'] == 'Job')
        template = stateful['spec']['template']['spec']
        nodes = json.loads(run('kubectl', 'get', 'nodes', '-o', 'json'))['items']
        selected = [n for n in nodes if all(n['metadata']['labels'].get(k) == v for k, v in template['nodeSelector'].items())]
        if not selected or not any(any(c['type'] == 'Ready' and c['status'] == 'True' for c in n['status']['conditions']) for n in selected):
            raise RuntimeError('Floci nodeSelector must match a Ready node; configure FLOCI_VALUES_FILE for this cluster')
        profiles = {n['metadata']['labels'].get('minikube.k8s.io/name') for n in nodes}
        profile = next(iter(profiles)) if len(profiles) == 1 and None not in profiles else None
        if not profile and values['probeImage']['pullPolicy'] == 'Never':
            raise RuntimeError('Local probe image requires Minikube; configure a registry probeImage and pullPolicy for other clusters')
        secret_name = values['existingSecret']
        original = credentials(get('secret', secret_name, args.namespace))
        client = credentials(get('secret', secret_name, args.client_namespace))
        if original and client and original != client:
            raise RuntimeError('Existing Floci and client credentials differ; refusing to overwrite either Secret')
        pvc_name = args.release + '-data'
        previous_pvc = get('pvc', pvc_name, args.namespace)
        if previous_pvc and previous_pvc['spec']['storageClassName'] != values['persistence']['storageClass']:
            raise RuntimeError('Refusing to change the existing Floci PVC storage class')
        if values['persistence']['createStorageClass'] and not get('csidriver', 'hostpath.csi.k8s.io', args.namespace):
            raise RuntimeError('CSI hostpath driver is missing; enable csi-hostpath-driver or configure an existing storage class')
        if not values['persistence']['createStorageClass']:
            if not get('storageclass', values['persistence']['storageClass'], args.namespace):
                raise RuntimeError('Configured Floci storage class does not exist')
        summary = {'release': args.release, 'namespace': args.namespace,
                   'clientNamespace': args.client_namespace, 'secret': secret_name,
                   'endpoint': f"http://{args.release}.{args.namespace}.svc.cluster.local:{values['service']['port']}",
                   'region': values['region'], 'pathStyle': True,
                   'serverImage': template['containers'][0]['image'],
                   'probeImage': bootstrap['spec']['template']['spec']['containers'][0]['image'],
                   'minikubeProfile': profile, 'buckets': values['bootstrap']['buckets'],
                   'existingPVCUID': previous_pvc['metadata']['uid'] if previous_pvc else None,
                   'checkOnly': args.check}
        if args.check:
            print(json.dumps(summary, indent=2))
            return
        print('Preparing Floci probe image and namespace-local credentials', flush=True)
        if profile:
            image = summary['probeImage']
            inspected = subprocess.run(['docker', 'image', 'inspect', image], capture_output=True)
            if inspected.returncode:
                default = yaml.safe_load((ROOT / 'values.yaml').read_text())['probeImage']
                if image == default['repository'] + ':' + default['tag']:
                    run('docker', 'build', '-t', image, str(ROOT / 'tools'))
                else:
                    run('docker', 'pull', image)
            run('minikube', '-p', profile, 'image', 'load', image)
        for namespace in set([args.namespace, args.client_namespace]):
            if not get('namespace', namespace, namespace):
                apply({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': namespace}})
        data = original or client or {key: base64.b64encode(secrets.token_hex(24).encode()).decode()
                                     for key in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']}
        for namespace, existing in [(args.namespace, original), (args.client_namespace, client)]:
            if not existing:
                apply({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': secret_name, 'namespace': namespace}, 'type': 'Opaque', 'data': data})
        apply({'apiVersion': 'v1', 'kind': 'ConfigMap',
               'metadata': {'name': 'floci-aws-config', 'namespace': args.client_namespace},
               'data': {'AWS_ENDPOINT_URL': summary['endpoint'], 'AWS_ENDPOINT_URL_S3': summary['endpoint'],
                        'AWS_REGION': str(values['region']), 'AWS_DEFAULT_REGION': str(values['region'])}})
        print('Installing Floci and running fresh bucket bootstrap', flush=True)
        run('helm', 'upgrade', '--install', args.release, str(ROOT), '-n', args.namespace,
            '-f', str(effective), '--wait', '--wait-for-jobs', '--timeout', '6m')
        summary['bootstrapLog'] = wait_job(args.namespace, bootstrap['metadata']['name'])
        current_pvc = get('pvc', pvc_name, args.namespace)
        if previous_pvc and previous_pvc['metadata']['uid'] != current_pvc['metadata']['uid']:
            raise RuntimeError('Floci PVC identity changed')
        summary['pvcUID'] = current_pvc['metadata']['uid']
        results = []
        for index, node in enumerate(nodes):
            node_name = node['metadata']['name']
            name = f'{args.release}-{run_id}-{index}'
            code = f'''import boto3,hashlib,json,socket
from botocore.config import Config
endpoint={summary['endpoint']!r}
socket.getaddrinfo(endpoint.split('://')[1].split(':')[0],{values['service']['port']})
s=boto3.client('s3',endpoint_url=endpoint,region_name={values['region']!r},config=Config(signature_version='s3v4',connect_timeout=3,read_timeout=15,retries={{'max_attempts':2}},s3={{'addressing_style':'path'}}))
for bucket in {summary['buckets']!r}:s.head_bucket(Bucket=bucket)
bucket={summary['buckets'][0]!r};key={('driver-smoke/'+run_id+'/'+str(index))!r};body={('datawave-driver-'+run_id+'-'+node_name)!r}.encode()
s.put_object(Bucket=bucket,Key=key,Body=body)
try:
 actual=s.get_object(Bucket=bucket,Key=key)['Body'].read()
 assert actual==body
 print(json.dumps({{'node':{node_name!r},'signedS3WriteRead':'pass','sha256':hashlib.sha256(actual).hexdigest()}}))
finally:s.delete_object(Bucket=bucket,Key=key)
'''
            pod = {'restartPolicy': 'Never', 'automountServiceAccountToken': False, 'enableServiceLinks': False,
                   'nodeSelector': {'kubernetes.io/hostname': node['metadata']['labels']['kubernetes.io/hostname']},
                   'tolerations': [{'key': 'node-role.kubernetes.io/control-plane', 'operator': 'Exists', 'effect': 'NoSchedule'}],
                   'containers': [{'name': 'probe', 'image': summary['probeImage'], 'imagePullPolicy': values['probeImage']['pullPolicy'],
                                   'args': ['-c', code], 'envFrom': [{'secretRef': {'name': secret_name}}],
                                   'env': [{'name': 'AWS_EC2_METADATA_DISABLED', 'value': 'true'}],
                                   'resources': {'requests': {'cpu': '50m', 'memory': '64Mi'}, 'limits': {'memory': '192Mi'}}}]}
            apply({'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {'name': name, 'namespace': args.namespace},
                   'spec': {'backoffLimit': 0, 'activeDeadlineSeconds': 90, 'template': {'spec': pod}}})
            results.append(json.loads(wait_job(args.namespace, name, 120)))
            run('kubectl', '-n', args.namespace, 'delete', 'job', name, '--wait=false')
        summary['nodeProbes'] = results
        summary['status'] = 'pass'
        if args.evidence:
            args.evidence.mkdir(parents=True, exist_ok=True)
            (args.evidence / 'driver-floci-result.json').write_text(json.dumps(summary, indent=2) + '\n')
        print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, OSError, ValueError, KeyError, StopIteration) as error:
        print(f'Floci setup failed: {error}', file=sys.stderr)
        sys.exit(1)
