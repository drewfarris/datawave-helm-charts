"""Driver flow and credential-preservation checks without cluster mutations."""
import base64
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('floci_deploy', ROOT / 'floci/tools/deploy.py')
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)
RENDERED = subprocess.check_output(['helm', 'template', 'floci', str(ROOT / 'floci'), '-n', 'aws-emulation'], text=True)


def secret(value):
    return {'data': {k: base64.b64encode(value.encode()).decode()
                     for k in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']}}


class DriverFlow(unittest.TestCase):
    def flow(self, fail=False, args=''):
        script = '''source "$DRIVER_SCRIPT"
for name in ready_helm_charts check_k8s_cluster guard_fresh_accumulo configure_repository_credentials ghcr_login create_secrets; do
 eval "$name() { echo CALL:$name; }"
done
set_namespace() { NAMESPACE=chosen; echo CALL:set_namespace; }
setup_mysql_operator() { echo CALL:mysql; }
setup_floci() { echo CALL:floci:$FLOCI_CLIENT_NAMESPACE; ''' + ('return 1;' if fail else ':;') + ''' }
helm_install() { echo CALL:datawave; }
main ''' + args
        result = subprocess.run(['bash', '-c', script], env={**os.environ, 'DRIVER_SCRIPT': str(ROOT / 'datawave-driver.sh')}, capture_output=True, text=True)
        return result, [x for x in result.stdout.splitlines() if x.startswith('CALL:')]

    def test_normal_flow_waits_for_floci_before_datawave(self):
        result, calls = self.flow()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls[-2:], ['CALL:floci:chosen', 'CALL:datawave'])

    def test_floci_failure_blocks_datawave(self):
        result, calls = self.flow(fail=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn('CALL:datawave', calls)

    def test_floci_only_bypasses_existing_stack(self):
        result, calls = self.flow(args='--floci-only')
        self.assertEqual(result.returncode, 0)
        self.assertEqual(len(calls), 1)
        self.assertTrue(calls[0].startswith('CALL:floci:'))

    def test_hdfs_mode_refuses_owned_hybrid_instance(self):
        script = r'''source "$DRIVER_SCRIPT"
DATAWAVE_ACCUMULO_STORAGE=hdfs
helm() {
 if [[ "$1" == list ]]; then echo '[{"name":"dwv"}]';
 else echo '{"dwv-accumulo":{"hybridStorage":{"enabled":true}}}'; fi
}
guard_fresh_accumulo
'''
        result = subprocess.run(['bash', '-c', script], env={**os.environ, 'DRIVER_SCRIPT':str(ROOT/'datawave-driver.sh')}, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('no migration path', result.stderr)

    def test_fresh_hybrid_mode_rejects_remote_charts(self):
        script = 'source "$DRIVER_SCRIPT"; DATAWAVE_ACCUMULO_STORAGE=floci; ready_helm_charts <<<remote'
        result = subprocess.run(['bash', '-c', script], env={**os.environ, 'DRIVER_SCRIPT':str(ROOT/'datawave-driver.sh')}, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('requires the local charts', result.stderr)


class Preflight(unittest.TestCase):
    def invoke(self, original=None, client=None, ready=True, minikube=True, check=True, fresh=False, missing_csi=False):
        nodes = {'items': [{'metadata': {'name': 'minikube-m03', 'labels': {
            'kubernetes.io/hostname': 'minikube-m03', **({'minikube.k8s.io/name': 'minikube'} if minikube else {})}},
            'status': {'conditions': [{'type': 'Ready', 'status': 'True' if ready else 'False'}]}}]}
        pvc = {'metadata': {'uid': 'retained'}, 'spec': {'storageClassName': 'floci-local'}}
        calls = []
        def run(*args, **kwargs):
            calls.append(args)
            if args[:2] == ('helm', 'template'):
                return RENDERED
            if args[:2] == ('helm', 'list'):
                return '[]'
            if args[:3] == ('kubectl', 'get', 'nodes'):
                return json.dumps(nodes)
            return ''
        pvc_reads = []
        def get(kind, name, namespace):
            if kind == 'secret':
                return original if namespace == 'aws-emulation' else client
            if kind == 'pvc':
                pvc_reads.append(name)
                return None if fresh and len(pvc_reads) == 1 else pvc
            if kind == 'csidriver' and missing_csi:
                return None
            if kind == 'namespace' and fresh:
                return None
            return {'metadata': {'name': name}}
        applied = []
        argv = ['deploy.py', *(['--check'] if check else [])]
        with patch.object(sys, 'argv', argv), patch.object(deploy, 'run', side_effect=run), \
             patch.object(deploy, 'get', side_effect=get), patch.object(deploy, 'apply', side_effect=applied.append), \
             patch.object(deploy.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0)), \
             patch.object(deploy, 'wait_job', side_effect=['bootstrap', '{"signedS3WriteRead":"pass"}']), \
             contextlib.redirect_stdout(io.StringIO()):
            deploy.main()
        return applied, calls

    def test_fresh_install_creates_namespaces_and_matching_credentials(self):
        applied, _ = self.invoke(check=False, fresh=True)
        namespaces = [o['metadata']['name'] for o in applied if o['kind'] == 'Namespace']
        created = [o for o in applied if o['kind'] == 'Secret']
        self.assertEqual(set(namespaces), {'default', 'aws-emulation'})
        self.assertEqual(len(created), 2)
        self.assertEqual(created[0]['data'], created[1]['data'])
        self.assertTrue(all(base64.b64decode(v) for v in created[0]['data'].values()))

    def test_credentials_mismatch_fails_before_mutation(self):
        with self.assertRaisesRegex(RuntimeError, 'differ'):
            self.invoke(secret('a'), secret('b'))

    def test_unready_target_fails_before_mutation(self):
        with self.assertRaisesRegex(RuntimeError, 'Ready node'):
            self.invoke(ready=False)

    def test_missing_csi_driver_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, 'CSI hostpath driver is missing'):
            self.invoke(missing_csi=True)

    def test_non_minikube_local_image_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, 'requires Minikube'):
            self.invoke(minikube=False)

    def test_check_is_read_only(self):
        applied, calls = self.invoke(secret('same'), secret('same'))
        self.assertFalse(applied)
        self.assertFalse(any('upgrade' in call for call in calls))

    def test_existing_credentials_are_not_overwritten(self):
        applied, _ = self.invoke(secret('same'), secret('same'), check=False)
        self.assertFalse(any(obj['kind'] == 'Secret' for obj in applied))
        self.assertTrue(any(obj['kind'] == 'ConfigMap' for obj in applied))

    def test_client_credentials_are_reused_for_missing_emulator_secret(self):
        existing = secret('keep-me')
        applied, _ = self.invoke(None, existing, check=False)
        created = [obj for obj in applied if obj['kind'] == 'Secret']
        self.assertEqual(len(created), 1)
        self.assertEqual(created[0]['data'], existing['data'])
        self.assertEqual(created[0]['metadata']['namespace'], 'aws-emulation')


if __name__ == '__main__':
    unittest.main()
