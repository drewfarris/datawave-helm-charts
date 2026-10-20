import importlib.util
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import yaml

ROOT = Path(__file__).resolve().parents[3]
def load(name):
    spec = importlib.util.spec_from_file_location('fresh_' + name, ROOT / 'floci/fresh' / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

prepare = load('prepare')
guard = load('guard')

class Fresh(unittest.TestCase):
    def test_overlay_preserves_hdfs_properties_and_wires_every_reader(self):
        base = {'dwv-hadoop-config': {'config': {'coreSiteProperties': [{'name':'fs.defaultFS','value':'hdfs://custom:9000/'},{'name':'custom.option','value':'keep'}]}}}
        with patch.dict(os.environ, {}, clear=True):
            values = prepare.overlay(base, 'http://floci:4566', 'us-east-1', 's3a://fresh/accumulo')
        acc = values['dwv-accumulo']
        self.assertFalse(acc['initialization']['auto'])
        self.assertTrue(acc['hybridStorage']['enabled'])
        self.assertEqual(len(acc['storage']['volumes']), 2)
        self.assertEqual(acc['config']['accumuloSite']['general.custom.volume.preferred.default'], 's3a://fresh/accumulo')
        self.assertTrue(acc['config']['accumuloSite']['general.custom.volume.preferred.logger'].startswith('hdfs://'))
        props = {p['name']:p['value'] for p in values['dwv-hadoop-config']['config']['coreSiteProperties']}
        self.assertEqual(props['fs.defaultFS'], 'hdfs://custom:9000/')
        self.assertEqual(props['custom.option'], 'keep')
        self.assertEqual(props['fs.s3a.change.detection.mode'], 'server')
        for component in [acc, values['dwv-ingest'], values['datawave-monolith-umbrella']['dwv-web']]:
            self.assertTrue(component['extraVolumes'])
            self.assertTrue(component['extraVolumeMounts'])
            self.assertIn('HADOOP_CREDSTORE_PASSWORD', [e['name'] for e in component['extraEnv']])

    def test_global_password_is_aligned_across_bootstrap_and_clients(self):
        v = prepare.overlay({'global':{'accumulo':{'password':'fixture-password'}}}, 'http://floci', 'us-east-1', 's3a://fresh/root')
        self.assertEqual(v['dwv-accumulo']['config']['userManagement']['rootPassword'], 'fixture-password')
        self.assertEqual(v['dwv-ingest']['config']['accumuloPassword'], 'fixture-password')
        self.assertEqual(v['global']['accumulo']['password'], 'fixture-password')

    def test_s3_default_filesystem_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, 'remain HDFS'):
            prepare.overlay({'dwv-hadoop-config':{'config':{'coreSiteProperties':[{'name':'fs.defaultFS','value':'s3a://bad'}]}}}, 'http://floci', 'us-east-1', 's3a://fresh/root')

    def test_matching_runtime_image_ids_skip_reload(self):
        def run(*args):
            if args[:3] == ('docker', 'image', 'inspect'):return json.dumps([{'Id':'sha256:expected'}])
            return json.dumps({'images':[{'id':'sha256:expected','repoTags':['docker.io/local/test:v1']}]})
        with patch.object(prepare, 'run', side_effect=run) as mocked:
            prepare.ensure_images('minikube', [{'metadata':{'name':'node'}}], ['local/test:v1'])
        self.assertFalse(any('load' in c.args for c in mocked.call_args_list))

    def test_stale_runtime_tag_is_reloaded_and_id_verified(self):
        loaded = []
        def run(*args):
            if args[:3] == ('docker', 'image', 'inspect'):return json.dumps([{'Id':'sha256:expected'}])
            if 'load' in args:loaded.append(True);return ''
            return json.dumps({'images':[{'id':'sha256:expected' if loaded else 'sha256:old','repoTags':['docker.io/local/test:v1']}]})
        with patch.object(prepare, 'run', side_effect=run), patch.object(prepare, 'load_image', side_effect=lambda *args: loaded.append(True)):
            prepare.ensure_images('minikube', [{'metadata':{'name':'node'}}], ['local/test:v1'])
        self.assertTrue(loaded)

    def test_failed_import_identity_is_rejected(self):
        def run(*args):
            if args[:3] == ('docker', 'image', 'inspect'):return json.dumps([{'Id':'sha256:expected'}])
            if 'load' in args:return ''
            return json.dumps({'images':[{'id':'sha256:old','repoTags':['docker.io/local/test:v1']}]})
        with patch.object(prepare, 'run', side_effect=run), patch.object(prepare, 'load_image'), self.assertRaisesRegex(RuntimeError, 'does not match'):
            prepare.ensure_images('minikube', [{'metadata':{'name':'node'}}], ['local/test:v1'])

    def test_legacy_release_is_rejected(self):
        with patch.object(guard, 'run', side_effect=[json.dumps([{'name':'dwv'}]), '{}']):
            with self.assertRaisesRegex(RuntimeError, 'Fresh instances only'):
                guard.guard('default')

    def test_owned_hybrid_release_can_restart(self):
        with patch.object(guard, 'run', side_effect=[json.dumps([{'name':'dwv'}]), json.dumps({'dwv-accumulo':{'hybridStorage':{'enabled':True}}})]):
            guard.guard('default')

    def test_orphaned_legacy_resources_are_rejected(self):
        with patch.object(guard, 'run', return_value='[]'), patch.object(guard, 'get', return_value={'metadata':{}}):
            with self.assertRaisesRegex(RuntimeError, 'Existing Accumulo resources'):
                guard.guard('default')

    def test_empty_namespace_is_allowed(self):
        with patch.object(guard, 'run', return_value='[]'), patch.object(guard, 'get', return_value=None):
            guard.guard('fresh')

if __name__ == '__main__':
    unittest.main()
