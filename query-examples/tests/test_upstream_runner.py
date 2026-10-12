"""Exercise the real upstream runner with the chart adapters and mocked curl."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

CHART = Path(__file__).resolve().parents[1]
SOURCE = Path(os.environ.get('DATAWAVE_SOURCE', str(CHART.parent.parent / 'datawave')))
UPSTREAM = SOURCE / 'contrib/datawave-quickstart/bin'


class UpstreamRunnerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        binary = self.root / 'bin'
        web = binary / 'services/datawave/test-web'
        (web / 'tests').mkdir(parents=True)
        (web / 'responses').mkdir()
        shutil.copy(UPSTREAM / 'services/datawave/test-web/run.sh', web / 'run.sh')
        shutil.copy(UPSTREAM / 'query.sh', binary / 'query.sh')
        shutil.copy(CHART / 'files/env.sh', binary / 'env.sh')
        shutil.copy(CHART / 'files/bootstrap.sh', web.parent / 'bootstrap.sh')
        for filename in ['GetEffectiveAuthorizations.test', 'EventQuery400BadRequest.test']:
            shutil.copy(UPSTREAM / 'services/datawave/test-web/tests' / filename, web / 'tests' / filename)
        mockbin = self.root / 'mockbin'
        mockbin.mkdir()
        curl = mockbin / 'curl'
        curl.write_text('''#!/usr/bin/env bash
printf '<Response/>HTTP_STATUS_CODE:%s;TOTAL_TIME:0.01;CONTENT_TYPE:application/xml;charset=UTF-8' "$MOCK_HTTP_STATUS"
''')
        curl.chmod(0o755)
        certificate = self.root / 'identity.pem'
        certificate.write_text('mock identity; never used by real curl')
        self.env = dict(os.environ, PATH=f'{mockbin}:{os.environ["PATH"]}',
                        DW_CURL_CERT=str(certificate), DW_CURL_KEY_RSA=str(certificate), DW_CURL_CA=str(certificate))
        self.runner = web / 'run.sh'

    def run_test(self, filename, status):
        return subprocess.run(['bash', str(self.runner), '--whitelist-files', filename],
                              env=dict(self.env, MOCK_HTTP_STATUS=status), text=True, capture_output=True)

    def test_original_assertions_pass(self):
        result = self.run_test('GetEffectiveAuthorizations', '200')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('Tests Passed: 1', result.stdout)

    def test_original_failures_propagate(self):
        result = self.run_test('GetEffectiveAuthorizations', '403')
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn('Tests Failed: 1', result.stdout)

    def test_upstream_expected_http_error_is_a_passing_test(self):
        result = self.run_test('EventQuery400BadRequest', '400')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('Tests Passed: 1', result.stdout)


if __name__ == '__main__':
    unittest.main()
