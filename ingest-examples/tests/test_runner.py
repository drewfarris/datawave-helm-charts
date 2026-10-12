"""Exercise the actual quickstart helpers with mocked Hadoop submissions."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET

CHART = Path(__file__).resolve().parents[1]
SOURCE = Path(os.environ.get("DATAWAVE_SOURCE", str(CHART.parent.parent / "datawave")))
BOOTSTRAP = SOURCE / "contrib/datawave-quickstart/bin/services/datawave/bootstrap-ingest.sh"


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        examples = self.root / "examples"
        examples.mkdir()
        (examples / "source").symlink_to(SOURCE)
        (examples / "bootstrap-ingest.sh").symlink_to(BOOTSTRAP)
        defaults = examples / "default-config"
        defaults.mkdir()
        (defaults / "mycsv-ingest-config.xml").write_text("<configuration/>\n")
        shared = self.root / "shared"
        shared.mkdir()
        (shared / "ingest-config.xml").write_text("<configuration>\n<property>\n<name>ingest.data.types</name>\n<value>existing</value>\n</property>\n</configuration>\n")
        (shared / "mycsv-ingest-config.xml").write_text("<wrong-shared-csv-config/>\n")
        self.cache = self.root / "cache"
        self.cache.mkdir()
        self.jar = self.root / "library.jar"
        self.jar.write_text("matching jar contents")
        (self.cache / self.jar.name).write_bytes(self.jar.read_bytes())
        ingest = self.root / "ingest"
        binary = ingest / "bin/ingest"
        binary.mkdir(parents=True)
        live = binary / "live-ingest.sh"
        live.write_text('''#!/usr/bin/env bash
printf 'JOB %s\\n' "$*" >> "$TRACE_FILE"
printf 'LIBRARIES %s\\n' "$ADDITIONAL_INGEST_LIBS" >> "$TRACE_FILE"
source ./job-cache-env.sh
printf 'CACHE %s %s\\n' "$JOB_CACHE_DIR" "$BASE_WORK_DIR" >> "$TRACE_FILE"
if [[ "${1##*/}" == "tvmaze-api.json" ]]; then exit "${JSON_STATUS:-0}"; fi
if [[ "${1##*/}" == "my.csv" ]]; then exit "${CSV_STATUS:-251}"; fi
exit 0
''')
        live.chmod(0o755)
        (binary / "job-cache-env.sh").write_text('JOB_CACHE_DIR="/old/shared/cache"\n')
        (binary / "ingest-libs.sh").write_text('CLASSPATH="$TEST_CLASSPATH${ADDITIONAL_INGEST_LIBS:+:$ADDITIONAL_INGEST_LIBS}"\n')
        (binary / "load-job-cache.sh").write_text('exit 99\n')
        (binary / "check-job-cache.sh").write_text('exit 99\n')
        stub = self.root / "bin"
        stub.mkdir()
        hdfs = stub / "hdfs"
        hdfs.write_text('''#!/usr/bin/env bash
printf 'HDFS %s\\n' "$*" >> "$TRACE_FILE"
if [[ "$2" == "-copyFromLocal" ]]; then exit "${PUT_STATUS:-0}"; fi
if [[ "$2" == "-test" ]]; then exit "${CACHE_MISSING_STATUS:-0}"; fi
if [[ "$2" == "-get" ]]; then
    [[ "${CACHE_STATUS:-0}" == 0 ]] || exit "$CACHE_STATUS"
    cp -R "$CACHE_SOURCE" "$4"
fi
''')
        hdfs.chmod(0o755)
        self.trace = self.root / "trace"
        self.ingest = ingest
        self.env = dict(os.environ, PATH=f"{stub}:{os.environ['PATH']}",
                        EXAMPLES_ROOT=str(examples), INGEST_HOME=str(ingest),
                        STACK_CONFIG=str(shared), EXAMPLES_HDFS_DIRECTORY="/examples/test",
                        DW_DATAWAVE_DATA_DIR=str(self.root / "data"),
                        INGEST_LOG_DIR=str(self.root / "logs"),
                        INGEST_LOCK_DIR=str(self.root / "locks"), TRACE_FILE=str(self.trace),
                        EXISTING_JOB_CACHE_DIRECTORY="/existing/jobCacheB", CACHE_SOURCE=str(self.cache),
                        TEST_CLASSPATH=str(self.jar))

    def run_runner(self, **overrides):
        return subprocess.run(["bash", str(CHART / "files/run-examples.sh")],
                              cwd=self.ingest / "bin/ingest",
                              env=dict(self.env, **overrides), text=True, capture_output=True)

    def jobs(self):
        trace = self.trace.read_text() if self.trace.exists() else ""
        return [line for line in trace.splitlines() if line.startswith("JOB ")]

    def test_all_helpers_and_expected_csv_error(self):
        result = self.run_runner(EXAMPLE_DATASETS="wikipedia,annotation,json,csv,wikipedia-medium,wikipedia-de,wikipedia-es,wikipedia-fr")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(self.jobs()), 8)
        self.assertIn("-data.name.override=wikipedia", self.jobs()[0])
        self.assertIn("-createTables", self.jobs()[0])
        self.assertIn("-data.name.override=annotation", self.jobs()[1])
        self.assertIn("-data.name.override=myjson", self.jobs()[2])
        self.assertIn("-data.name.override=mycsv", self.jobs()[3])
        self.assertIn("documented exit 251", result.stderr)
        self.assertEqual((self.ingest / "config/mycsv-ingest-config.xml").read_text(), "<configuration/>\n")

    def test_unexpected_job_failure_stops_later_submissions(self):
        result = self.run_runner(EXAMPLE_DATASETS="wikipedia,json,csv", JSON_STATUS="7")
        self.assertEqual(result.returncode, 7)
        self.assertEqual(len(self.jobs()), 2)
        self.assertNotIn("All selected examples completed", result.stdout)

    def test_csv_can_be_strict(self):
        result = self.run_runner(EXAMPLE_DATASETS="csv", ALLOW_EXPECTED_CSV_ERRORS="false")
        self.assertEqual(result.returncode, 251)

    def test_hdfs_upload_failure_prevents_job_submission(self):
        result = self.run_runner(EXAMPLE_DATASETS="json", PUT_STATUS="3")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.jobs(), [])

    def test_unknown_dataset_fails(self):
        result = self.run_runner(EXAMPLE_DATASETS="unknown")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.jobs(), [])

    def test_read_only_rpm_and_shared_config_files(self):
        config = self.ingest / "config"
        config.mkdir()
        for name in ("ingest-config.xml", "mycsv-ingest-config.xml"):
            target = config / name
            target.write_text("<old-rpm-config/>\n")
            target.chmod(0o444)
            (self.root / "shared" / name).chmod(0o444)
        result = self.run_runner(EXAMPLE_DATASETS="json")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        tree = ET.parse(config / "ingest-config.xml")
        self.assertEqual(tree.findtext("property/value"), "existing,myjson")
        self.assertTrue((config / "ingest-config.xml").stat().st_mode & 0o200)
        self.assertEqual((config / "mycsv-ingest-config.xml").read_text(), "<configuration/>\n")

    def test_cache_failure_prevents_submissions(self):
        result = self.run_runner(CACHE_STATUS="4")
        self.assertEqual(result.returncode, 4)
        self.assertEqual(self.jobs(), [])

    def test_existing_cache_is_reused_and_annotation_is_enabled(self):
        result = self.run_runner(EXAMPLE_DATASETS="annotation")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        cache_lines = [line for line in self.trace.read_text().splitlines() if line.startswith("CACHE ")]
        self.assertEqual(cache_lines, ["CACHE /existing/jobCacheB /examples/test/work"])
        self.assertNotIn("jobCacheA", self.trace.read_text())
        self.assertNotIn("-copyFromLocal -t", self.trace.read_text())
        self.assertIn("EXISTING_JOB_CACHE_DIRECTORY", (self.ingest / "bin/ingest/job-cache-env.sh").read_text())
        tree = ET.parse(self.ingest / "config/ingest-config.xml")
        self.assertEqual(tree.findtext("property/value"), "existing,annotation")

    def test_same_name_different_jar_contents_prevents_submission(self):
        (self.cache / self.jar.name).write_text("stale jar contents")
        result = self.run_runner()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("library differs", result.stderr)
        self.assertEqual(self.jobs(), [])

    def test_optional_library_probe_failure_does_not_abort_validation(self):
        (self.ingest / "bin/ingest/ingest-libs.sh").write_text('false\nCLASSPATH="$TEST_CLASSPATH"\n')
        result = self.run_runner(EXAMPLE_DATASETS="annotation")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("matches all 1 required JARs", result.stdout)

    def test_legacy_classpath_discovery_uses_ingest_script_directory(self):
        binary = self.ingest / "bin/ingest"
        (binary / "legacy-env.sh").write_text('cd "$(dirname -- "$0")"\n')
        (binary / "findJars.sh").write_text('CLASSPATH="$TEST_CLASSPATH"\n')
        (binary / "ingest-libs.sh").write_text('source ../ingest/legacy-env.sh\nsource ../ingest/findJars.sh\n')
        result = self.run_runner(EXAMPLE_DATASETS="annotation")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("matches all 1 required JARs", result.stdout)

    def test_cache_checker_does_not_require_ingest_home_environment(self):
        checker = self.ingest / "bin/ingest/check-existing-cache.sh"
        checker.write_text((CHART / "files/check-existing-cache.sh").read_text())
        env = dict(self.env)
        env.pop("INGEST_HOME")
        result = subprocess.run(["bash", str(checker)], cwd=self.root, env=env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("matches all 1 required JARs", result.stdout)

    def test_missing_cached_jar_prevents_submission(self):
        (self.cache / self.jar.name).unlink()
        result = self.run_runner()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing a required library", result.stderr)
        self.assertEqual(self.jobs(), [])

    def test_missing_existing_cache_prevents_submission(self):
        result = self.run_runner(CACHE_MISSING_STATUS="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("cache does not exist", result.stderr)
        self.assertEqual(self.jobs(), [])

    def test_missing_cache_selection_prevents_submission(self):
        result = self.run_runner(EXISTING_JOB_CACHE_DIRECTORY="")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.jobs(), [])

    def test_missing_other_library_fails_before_submission(self):
        result = self.run_runner(ADDITIONAL_INGEST_LIBS="/old/missing-hadoop-client.jar")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.jobs(), [])

    def test_table_initialization_can_be_disabled(self):
        result = self.run_runner(EXAMPLE_DATASETS="json", CREATE_EXAMPLE_TABLES="false")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn("-createTables", self.jobs()[0])

    def test_additional_application_library_is_validated_and_preserved(self):
        library = self.root / "custom-application.jar"
        library.write_text("matching application library")
        (self.cache / library.name).write_bytes(library.read_bytes())
        result = self.run_runner(EXAMPLE_DATASETS="json", ADDITIONAL_INGEST_LIBS=str(library))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(f"LIBRARIES {library}", self.trace.read_text())
        self.assertIn("matches all 2 required JARs", result.stdout)


if __name__ == "__main__":
    unittest.main()
