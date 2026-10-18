#!/usr/bin/env python3
"""Check sample ingestion and wait for authenticated WildFly cache reloads."""
import json
import os
from pathlib import Path
import ssl
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


def info(message):
    print(f"[INFO] {message}", flush=True)


def fetch_jobs():
    account = Path('/var/run/secrets/kubernetes.io/serviceaccount')
    selector = 'app.kubernetes.io/name=datawave-ingest-examples,app.kubernetes.io/instance=' + os.environ['INGESTION_RELEASE']
    query = urllib.parse.urlencode({'labelSelector': selector})
    namespace = urllib.parse.quote(os.environ['POD_NAMESPACE'], safe='')
    url = f"https://{os.environ['KUBERNETES_SERVICE_HOST']}:{os.environ['KUBERNETES_SERVICE_PORT_HTTPS']}/apis/batch/v1/namespaces/{namespace}/jobs?{query}"
    request = urllib.request.Request(url, headers={'Authorization': 'Bearer ' + (account / 'token').read_text().strip()})
    with urllib.request.urlopen(request, context=ssl.create_default_context(cafile=str(account / 'ca.crt')), timeout=15) as response:
        return json.load(response)['items']


def wait_for_ingestion(fetch=fetch_jobs):
    deadline = time.monotonic() + int(os.environ.get('PREREQUISITE_TIMEOUT', '300'))
    required = set(os.environ.get('REQUIRED_DATASETS', 'wikipedia,json,csv').split(','))
    while True:
        jobs = fetch()
        if jobs:
            job = max(jobs, key=lambda item: item['metadata']['creationTimestamp'])
            conditions = {item['type']: item['status'] for item in job.get('status', {}).get('conditions', [])}
            name = job['metadata']['name']
            if conditions.get('Failed') == 'True':
                raise RuntimeError(f'Latest ingestion Job {name} failed; complete ingestion before running queries.')
            if conditions.get('Complete') == 'True':
                datasets = set()
                for container in job['spec']['template']['spec']['containers']:
                    for variable in container.get('env', []):
                        if variable['name'] == 'EXAMPLE_DATASETS':
                            datasets.update(variable.get('value', '').split(','))
                missing = required - datasets
                if missing:
                    raise RuntimeError(f'Latest ingestion Job {name} did not load required datasets: {", ".join(sorted(missing))}')
                info(f'Sample ingestion prerequisite passed: {name}')
                return
        if time.monotonic() >= deadline:
            raise RuntimeError('Timed out waiting for a completed sample-ingestion Job.')
        time.sleep(int(os.environ.get('PREREQUISITE_POLL', '5')))


def web_request(path):
    command = ['curl', '--silent', '--show-error', '--insecure', '--connect-timeout', '5', '--max-time', '120',
               '--cert', os.environ['DW_CURL_CERT'], '--key', os.environ['DW_CURL_KEY_RSA'],
               '--header', 'Accept: application/xml', '--write-out', '\n%{http_code}']
    for variable, header in [('PROXIED_SUBJECT', 'X-ProxiedEntitiesChain'), ('PROXIED_ISSUER', 'X-ProxiedIssuersChain')]:
        if os.environ.get(variable):
            command += ['--header', f'{header}: <{os.environ[variable]}>']
    command += [os.environ['DW_BASE_URI'].rstrip('/') + path]
    response = subprocess.run(command, text=True, capture_output=True, timeout=125)
    if response.returncode:
        raise RuntimeError(f'WildFly request failed: {response.stderr.strip()}')
    body, status = response.stdout.rsplit('\n', 1)
    if status != '200':
        raise RuntimeError(f'WildFly {path} returned HTTP {status}; check readiness, client trust and roles.')
    root = ET.fromstring(body)
    if any(element.tag.split('}')[-1] == 'Exception' for element in root.iter()):
        raise RuntimeError(f'WildFly {path} returned an exception response.')
    return root


def cache_status(request=web_request):
    root = request('/Common/AccumuloTableCache/')
    return {element.attrib['tableName']: element.attrib for element in root.iter()
            if element.tag.split('}')[-1] == 'TableCache'}


def wait_for_reload(tables, before, request=web_request):
    deadline = time.monotonic() + int(os.environ.get('CACHE_TIMEOUT', '300'))
    while True:
        current = cache_status(request)
        ready = all(table in current and current[table].get('currentlyRefreshing') == 'false'
                    and current[table].get('lastRefresh', '').split('T')[0] not in ('', '1970-01-01')
                    and current[table].get('lastRefresh') != before[table].get('lastRefresh') for table in tables)
        if ready:
            info('Table cache reload completed: ' + ', '.join(tables))
            info('Cache refresh evidence: ' + json.dumps({table: {'before': before[table].get('lastRefresh'), 'after': current[table].get('lastRefresh'), 'currentlyRefreshing': current[table].get('currentlyRefreshing')} for table in tables}, sort_keys=True))
            return
        if time.monotonic() >= deadline:
            raise RuntimeError('Timed out waiting for a new completed table-cache refresh: ' + ', '.join(tables))
        time.sleep(int(os.environ.get('CACHE_POLL', '2')))


def reload_tables(request=web_request):
    tables = os.environ.get('CACHE_TABLES', 'datawave.metadata,datawave.queryMetrics_m,datawave.error_m').split(',')
    before = cache_status(request)
    missing = set(tables) - set(before)
    if missing:
        raise RuntimeError('Requested tables are not configured in WildFly cache: ' + ', '.join(sorted(missing)))
    for table in tables:
        request('/Common/AccumuloTableCache/reload/' + urllib.parse.quote(table, safe=''))
    wait_for_reload(tables, before, request)


def main():
    if sys.argv[1] == 'prepare' and os.environ.get('REQUIRE_INGESTION_JOB', 'true') == 'true':
        wait_for_ingestion()
    reload_tables()


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(f'[ERROR] {error}', file=sys.stderr, flush=True)
        sys.exit(1)
