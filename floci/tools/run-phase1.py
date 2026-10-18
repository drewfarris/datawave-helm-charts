#!/usr/bin/env python3
"""Run isolated Floci S3 probes and restart checks; never target DataWave workloads."""
import argparse
import json
from pathlib import Path
import subprocess
import time
from datetime import datetime, timezone

PARSER = argparse.ArgumentParser(description=__doc__)
PARSER.add_argument('--namespace', default='aws-emulation')
PARSER.add_argument('--release', default='floci')
PARSER.add_argument('--evidence', type=Path, required=True)
ARGS = PARSER.parse_args()
NS, RELEASE, OUT = ARGS.namespace, ARGS.release, ARGS.evidence
OUT.mkdir(parents=True, exist_ok=True)
PROBE = RELEASE + '-phase1-probe'
IMAGE = 'local/datawave/floci-probe:phase1-20261018'
COMMANDS = json.loads((OUT / 'probe-commands.json').read_text()) if (OUT / 'probe-commands.json').exists() else []


def run(command, name=None, check=True, timeout=120, stdin=None):
    start = datetime.now(timezone.utc).isoformat()
    result = subprocess.run(command, input=stdin, text=True, capture_output=True, timeout=timeout)
    COMMANDS.append({'command': command, 'startedAt': start, 'exitCode': result.returncode, 'file': name})
    if name:
        (OUT / name).write_text(result.stdout + ('\nSTDERR:\n' + result.stderr if result.stderr else ''))
    (OUT / 'probe-commands.json').write_text(json.dumps(COMMANDS, indent=2) + '\n')
    if check and result.returncode:
        raise RuntimeError('Command failed: ' + ' '.join(command) + '\n' + result.stdout + result.stderr)
    return result


def kube(*args, **kwargs):
    return run(['kubectl', '-n', NS, *args], **kwargs)


def pod(name):
    result = kube('get', 'pod', name, '-o', 'json', check=False)
    return json.loads(result.stdout) if result.returncode == 0 else None


def wait(name, previous_uid=None, previous_restarts=None):
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        state = pod(name)
        if state:
            statuses = state.get('status', {}).get('containerStatuses', [])
            ready = bool(statuses) and all(s.get('ready') for s in statuses)
            uid_ok = not previous_uid or state['metadata']['uid'] != previous_uid
            restart_ok = previous_restarts is None or statuses and statuses[0]['restartCount'] > previous_restarts
            if ready and uid_ok and restart_ok:
                return state
        time.sleep(2)
    raise TimeoutError('Pod did not recover: ' + name)


def make_probe(name, node, memory='192Mi', cpu='50m', storage=False):
    definition = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': name, 'namespace': NS,
                  'labels': {'app.kubernetes.io/name': 'floci-phase1-probe'}},
      'spec': {'restartPolicy': 'Never', 'automountServiceAccountToken': False, 'enableServiceLinks': False, 'terminationGracePeriodSeconds': 5,
        'nodeSelector': {'kubernetes.io/hostname': node},
        'securityContext': {'runAsNonRoot': True, 'runAsUser': 1001, 'runAsGroup': 1001},
        'containers': [{'name': 'probe', 'image': IMAGE, 'imagePullPolicy': 'Never',
          'args': ['-c', 'import signal,time; signal.signal(signal.SIGTERM, lambda *_: exit(0)); time.sleep(7200)'],
          'envFrom': [{'secretRef': {'name': RELEASE+'-aws-credentials'}}],
          'env': [{'name': 'AWS_ENDPOINT_URL', 'value': f'http://{RELEASE}.{NS}.svc.cluster.local:4566'},
                  {'name': 'AWS_DEFAULT_REGION', 'value': 'us-east-1'},
                  {'name': 'AWS_EC2_METADATA_DISABLED', 'value': 'true'},
                  {'name': 'NODE_NAME', 'valueFrom': {'fieldRef': {'fieldPath': 'spec.nodeName'}}}],
          'resources': {'requests': {'cpu': cpu, 'memory': '32Mi'}, 'limits': {'cpu': '1', 'memory': memory}},
          'securityContext': {'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']}},
          'volumeMounts': [{'name': 'probes', 'mountPath': '/probes', 'readOnly': True}]}],
        'volumes': [{'name': 'probes', 'configMap': {'name': RELEASE+'-probes'}}]}}
    if storage:
        definition['spec']['volumes'].append({'name': 'state', 'persistentVolumeClaim': {'claimName': RELEASE+'-data', 'readOnly': True}})
        definition['spec']['containers'][0]['volumeMounts'].append({'name': 'state', 'mountPath': '/state', 'readOnly': True})
    kube('apply', '-f', '-', stdin=json.dumps(definition))
    wait(name)


def probe(mode, *args, name=None, target=PROBE):
    return kube('exec', target, '--', 'python3', '/probes/s3_probe.py', mode, *args, name=name, timeout=180)


def snapshot(label):
    kube('get', 'pod', RELEASE+'-0', '-o', 'json', name=label+'-pod.json')
    kube('get', 'pvc', RELEASE+'-data', '-o', 'json', name=label+'-pvc.json')
    kube('logs', RELEASE+'-0', '--tail=100', name=label+'-server.log')


def main():
    server = wait(RELEASE+'-0')
    assert server['spec']['nodeName'] == 'minikube-m03'
    values = json.loads(run(['helm', 'get', 'values', RELEASE, '-n', NS, '-a', '-o', 'json']).stdout)
    kube('logs', 'job/'+RELEASE+'-bootstrap-'+str(values['bootstrap']['runId']), name='bootstrap.log')
    make_probe(PROBE, 'minikube-m03')
    for node in ['minikube', 'minikube-m02', 'minikube-m03']:
        name = RELEASE+'-connect-'+node
        make_probe(name, node, memory='128Mi')
        probe('connect', target=name, name='connect-'+node+'.jsonl')
        kube('get', 'pod', name, '-o', 'json', name='connect-'+node+'-pod.json')
        kube('delete', 'pod', name, '--wait=true', '--timeout=30s')
    probe('api', name='api-results.jsonl')
    # Re-run bootstrap to demonstrate bucket creation is safe to repeat.
    probe('bootstrap', name='bootstrap-repeat.jsonl')
    probe('seed', '--stage', 'baseline', name='sentinel-baseline.jsonl')
    probe('verify', '--stages', 'baseline', name='verify-before-restarts.jsonl')
    snapshot('before-restarts')

    current = pod(RELEASE+'-0')
    count = current['status']['containerStatuses'][0]['restartCount']
    kube('exec', RELEASE+'-0', '--', '/bin/sh', '-c', 'kill -TERM 1', name='graceful-signal.txt', check=False)
    wait(RELEASE+'-0', previous_restarts=count)
    kube('logs', RELEASE+'-0', '--previous', name='graceful-shutdown.log')
    probe('verify', '--stages', 'baseline', name='verify-after-graceful.jsonl')
    snapshot('after-graceful')

    probe('seed', '--stage', 'replacement', name='sentinel-replacement.jsonl')
    old_uid = pod(RELEASE+'-0')['metadata']['uid']
    kube('delete', 'pod', RELEASE+'-0', '--wait=true', '--timeout=90s', name='pod-replacement.txt')
    wait(RELEASE+'-0', previous_uid=old_uid)
    probe('verify', '--stages', 'baseline,replacement', name='verify-after-replacement.jsonl')
    snapshot('after-replacement')

    # A fresh acknowledged write must survive a forced kill within the index checkpoint interval.
    current = pod(RELEASE+'-0')
    status = current['status']['containerStatuses'][0]
    container_id = status['containerID'].removeprefix('containerd://')
    count = status['restartCount']
    probe('seed', '--stage', 'crash', name='sentinel-crash.jsonl')
    run(['docker', 'exec', 'minikube-m03', 'ctr', '-n', 'k8s.io', 'tasks', 'kill', '--signal', 'SIGKILL', container_id],
        name='forced-container-stop.txt', timeout=30)
    recovered = wait(RELEASE+'-0', previous_restarts=count)
    terminated = recovered['status']['containerStatuses'][0].get('lastState', {}).get('terminated', {})
    assert terminated.get('exitCode') == 137, 'Forced-stop check needs SIGKILL exit 137, got '+str(terminated)
    kube('logs', RELEASE+'-0', '--previous', name='forced-stop-previous.log')
    probe('verify', '--stages', 'baseline,replacement,crash', name='verify-after-crash.jsonl')
    probe('seed', '--stage', 'recovered', name='sentinel-recovered.jsonl')
    probe('verify', '--stages', 'baseline,replacement,crash,recovered', name='verify-final.jsonl')
    snapshot('after-crash')
    inspect_storage()
    finish_report()


def inspect_storage():
    name = RELEASE+'-storage-inspector'
    make_probe(name, 'minikube-m03', memory='128Mi', storage=True)
    code = """import os,json,hashlib
files=[]
for root,dirs,names in os.walk('/state'):
 for name in sorted(names):
  path=os.path.join(root,name)
  h=hashlib.sha256()
  with open(path,'rb') as f:
   for block in iter(lambda:f.read(1048576),b''):h.update(block)
  files.append({'path':path,'bytes':os.path.getsize(path),'sha256':h.hexdigest()})
print(json.dumps({'mounts':[line.strip() for line in open('/proc/mounts') if ' /state ' in line], 'files':files},indent=2))
"""
    kube('exec', name, '--', 'python3', '-c', code, name='persistent-storage-files.json')
    inventory = json.loads((OUT/'persistent-storage-files.json').read_text())
    assert inventory['mounts'] and any(' ro,' in line for line in inventory['mounts']), 'Inspector mount must be read-only'
    assert any('/s3-objects.' in f['path'] for f in inventory['files']), 'Object index/journal not on PVC'
    # Content may be deduplicated; require the expected nonempty payload hashes on this volume.
    expected = {entry['sha256'] for line in (OUT/'verify-final.jsonl').read_text().splitlines()
                for entry in json.loads(line).get('objects', []) if entry['size']}
    assert expected <= {f['sha256'] for f in inventory['files']}, 'Object bytes not found on mounted PVC'
    kube('get', 'pod', name, '-o', 'json', name='storage-inspector-pod.json')
    kube('delete', 'pod', name, '--wait=true', '--timeout=30s')


def finish_report():
    for node in ['minikube', 'minikube-m02', 'minikube-m03']:
        assert json.loads((OUT/('connect-'+node+'.jsonl')).read_text().splitlines()[0])['status'] == 'pass'
    assert json.loads((OUT/'api-results.jsonl').read_text().splitlines()[-1])['case'] == 'api-suite'
    assert json.loads((OUT/'verify-final.jsonl').read_text().splitlines()[-1])['status'] == 'pass'
    before = json.loads((OUT/'before-restarts-pvc.json').read_text())
    after = json.loads((OUT/'after-crash-pvc.json').read_text())
    assert before['metadata']['uid'] == after['metadata']['uid']
    assert before['spec']['volumeName'] == after['spec']['volumeName']
    if pod(PROBE):
        kube('delete', 'pod', PROBE, '--wait=true', '--timeout=45s')
    (OUT/'phase1-result.json').write_text(json.dumps({'status': 'pass', 'nodes': 3, 'apiSuite': 'pass',
        'durability': ['graceful SIGTERM', 'pod replacement', 'forced SIGKILL exit 137'],
        'completedAt': datetime.now(timezone.utc).isoformat(), 'bucket': 'dwv-s3-smoke',
        'pvcUID': after['metadata']['uid'], 'volume': after['spec']['volumeName'],
        'sentinelStages': ['baseline', 'replacement', 'crash', 'recovered']}, indent=2)+'\n')
    print('Phase 1 API, all-node connectivity and persistence probes passed.')



if __name__ == '__main__':
    main()
