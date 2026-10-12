import importlib.util
import os
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

SCRIPT = Path(__file__).resolve().parents[1] / 'files/prepare-queries.py'
SPEC = importlib.util.spec_from_file_location('prepare_queries', SCRIPT)
prepare = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(prepare)


def job(name='ingest', created='2026-10-11T00:00:00Z', condition='Complete', datasets='wikipedia,json,csv'):
    return {'metadata': {'name': name, 'creationTimestamp': created},
            'status': {'conditions': [{'type': condition, 'status': 'True'}]} if condition else {},
            'spec': {'template': {'spec': {'containers': [{'env': [{'name': 'EXAMPLE_DATASETS', 'value': datasets}]}]}}}}


def cache(refresh='2026-10-11T01:00:00Z', refreshing='false'):
    return ET.fromstring(f'<Status><TableCache tableName="datawave.metadata" lastRefresh="{refresh}" currentlyRefreshing="{refreshing}"/></Status>')


class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.environment = patch.dict(os.environ, {'PREREQUISITE_TIMEOUT': '0', 'CACHE_TIMEOUT': '0', 'CACHE_TABLES': 'datawave.metadata'}, clear=False)
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def test_completed_ingestion_with_required_samples(self):
        prepare.wait_for_ingestion(lambda: [job()])

    def test_missing_samples_rejected(self):
        with self.assertRaisesRegex(RuntimeError, 'required datasets'):
            prepare.wait_for_ingestion(lambda: [job(datasets='json,csv')])

    def test_failed_ingestion_rejected(self):
        with self.assertRaisesRegex(RuntimeError, 'failed'):
            prepare.wait_for_ingestion(lambda: [job(condition='Failed')])

    def test_latest_pending_job_does_not_use_old_success(self):
        with self.assertRaisesRegex(RuntimeError, 'Timed out'):
            prepare.wait_for_ingestion(lambda: [job(), job('new', '2026-10-12T00:00:00Z', None)])

    def test_no_ingestion_job_times_out(self):
        with self.assertRaisesRegex(RuntimeError, 'Timed out'):
            prepare.wait_for_ingestion(lambda: [])

    def test_reload_waits_for_new_finished_refresh(self):
        calls = []
        states = iter([cache(), cache('2026-10-11T02:00:00Z')])
        def request(path):
            calls.append(path)
            return ET.fromstring('<VoidResponse/>') if '/reload/' in path else next(states)
        prepare.reload_tables(request)
        self.assertEqual(calls, ['/Common/AccumuloTableCache/', '/Common/AccumuloTableCache/reload/datawave.metadata', '/Common/AccumuloTableCache/'])

    def test_old_refresh_does_not_satisfy_reload(self):
        before = prepare.cache_status(lambda _: cache())
        with self.assertRaisesRegex(RuntimeError, 'Timed out'):
            prepare.wait_for_reload(['datawave.metadata'], before, lambda _: cache())

    def test_in_progress_refresh_does_not_satisfy_reload(self):
        before = prepare.cache_status(lambda _: cache())
        with self.assertRaisesRegex(RuntimeError, 'Timed out'):
            prepare.wait_for_reload(['datawave.metadata'], before, lambda _: cache('2026-10-11T02:00:00Z', 'true'))

    def test_missing_configured_cache_fails(self):
        with self.assertRaisesRegex(RuntimeError, 'not configured'):
            prepare.reload_tables(lambda _: ET.fromstring('<Status/>'))

    def test_http_auth_failure_is_not_treated_as_reload_success(self):
        with patch.dict(os.environ, {'DW_CURL_CERT': 'cert', 'DW_CURL_KEY_RSA': 'key', 'DW_BASE_URI': 'https://example/DataWave'}):
            with patch.object(subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, '<Forbidden/>\n403', '')):
                with self.assertRaisesRegex(RuntimeError, 'HTTP 403'):
                    prepare.web_request('/Common/AccumuloTableCache/reload/datawave.metadata')


if __name__ == '__main__':
    unittest.main()
