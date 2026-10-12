#!/usr/bin/env bash
set -eo pipefail

# bootstrap-ingest.sh is sourced by the quickstart environment. Provide its
# logging helpers without starting any of the quickstart's local services.
info() { printf '[INFO] %s\n' "$*"; }
warn() { printf '[WARN] %s\n' "$*" >&2; }
error() { printf '[ERROR] %s\n' "$*" >&2; }
printYellow() { printf '%s' "$*"; }
printGreen() { printf '%s' "$*"; }

EXAMPLES_ROOT=${EXAMPLES_ROOT:-/opt/datawave-examples}
INGEST_HOME=${INGEST_HOME:-/opt/datawave-ingest/current}
STACK_CONFIG=${STACK_CONFIG:-/stack-config}
export DW_CLOUD_HOME=/opt/datawave-ingest
export DW_DATAWAVE_SOURCE_DIR="$EXAMPLES_ROOT/source"
export DW_DATAWAVE_DATA_DIR=${DW_DATAWAVE_DATA_DIR:-/srv/data/datawave}
export DW_DATAWAVE_INGEST_HDFS_BASEDIR=${EXAMPLES_HDFS_DIRECTORY:?EXAMPLES_HDFS_DIRECTORY is required}

if [[ ! -s "$STACK_CONFIG/ingest-config.xml" ]]; then
    error "Shared ingest configuration is missing. Wait for the main ingest deployment to finish starting."
    exit 1
fi
mkdir -p "$INGEST_HOME/config" "${INGEST_LOG_DIR:-/srv/logs/ingest}" \
    "$DW_DATAWAVE_DATA_DIR/flags" "${INGEST_LOCK_DIR:-/var/run/datawave}"
# The RPM installs these files as owner-readable (0444). The Job's datawave
# user owns them and can make its private copy writable before replacing it.
chmod -R u+w "$INGEST_HOME/config"
cp -Lr "$STACK_CONFIG/." "$INGEST_HOME/config/"
# Keep shared connection/table settings, but use the stock datatype definitions
# that match the quickstart fixtures (particularly the intentionally invalid CSV).
chmod -R u+w "$INGEST_HOME/config"
cp "$EXAMPLES_ROOT/default-config/"*-ingest-config.xml "$INGEST_HOME/config/"

# The main stack may omit annotation from its enabled ingest types. Add the
# selected examples to this container's configuration, preserving existing types.
IFS=',' read -r -a datasets <<< "${EXAMPLE_DATASETS:-wikipedia,annotation,json,csv}"
selected_types=""
for dataset in "${datasets[@]}"; do
    case "$dataset" in
        wikipedia|wikipedia-medium|wikipedia-de|wikipedia-es|wikipedia-fr) datatype=wikipedia ;;
        annotation) datatype=annotation ;;
        json) datatype=myjson ;;
        csv) datatype=mycsv ;;
        *) error "Unknown dataset: $dataset"; exit 1 ;;
    esac
    selected_types="${selected_types:+$selected_types,}$datatype"
done
# This property is emitted with a single-line value by the main ingest chart.
if ! grep -q '<name>ingest.data.types</name>' "$INGEST_HOME/config/ingest-config.xml"; then
    error "Shared ingest configuration has no ingest.data.types property."
    exit 1
fi
sed -i "/<name>ingest.data.types<\/name>/,/<\/property>/s|<value>\(.*\)</value>|<value>\1,$selected_types</value>|" \
    "$INGEST_HOME/config/ingest-config.xml"

source "$EXAMPLES_ROOT/bootstrap-ingest.sh"
DW_DATAWAVE_INGEST_HOME="$INGEST_HOME"

# Resolve Hadoop and WebSocket dependencies from the aligned ingest image.
resolved_libraries=""
IFS=':' read -r -a configured_libraries <<< "${ADDITIONAL_INGEST_LIBS:-}"
for library in "${configured_libraries[@]}"; do
    [[ -z "$library" ]] && continue
    if [[ ! -f "$library" ]]; then
        error "Additional ingest library does not exist: $library"
        exit 1
    fi
    resolved_libraries="${resolved_libraries:+$resolved_libraries:}$library"
done
for library in /lib/hadoop/hadoop-common.jar \
               /lib/hadoop/hadoop-annotations.jar \
               /lib/hadoop/lib/commons-collections-*.jar \
               /lib/hadoop-hdfs/hadoop-hdfs-client.jar \
               /lib/hadoop-mapreduce/hadoop-mapreduce-client-{core,common,jobclient}.jar \
               /lib/hadoop-yarn/hadoop-yarn-{client,common,api}.jar \
               /lib/hadoop-yarn/lib/websocket-api-*.jar \
               /lib/hadoop-yarn/lib/websocket-client-*.jar \
               /lib/hadoop-yarn/lib/websocket-common-*.jar; do
    if [[ -f "$library" && ":$resolved_libraries:" != *":$library:"* ]]; then
        resolved_libraries="${resolved_libraries:+$resolved_libraries:}$library"
    fi
done
export ADDITIONAL_INGEST_LIBS="$resolved_libraries"

hdfs dfs -mkdir -p "$DW_DATAWAVE_INGEST_HDFS_BASEDIR"

# Keep per-run work isolated, but select the existing service cache explicitly.
export BASE_WORK_DIR="$DW_DATAWAVE_INGEST_HDFS_BASEDIR/work"
export EXISTING_JOB_CACHE_DIRECTORY=${EXISTING_JOB_CACHE_DIRECTORY:?An existing job cache directory is required}
chmod u+w "$INGEST_HOME/bin/ingest/job-cache-env.sh"
cat > "$INGEST_HOME/bin/ingest/job-cache-env.sh" <<'CACHE'
JOB_CACHE_DIR=${EXISTING_JOB_CACHE_DIRECTORY:?An existing job cache directory is required}
CACHE
info "Checking existing MapReduce job cache at $EXISTING_JOB_CACHE_DIRECTORY"
# The legacy environment uses $0 to choose its working directory. Run the
# checker from bin/ingest so relative library discovery uses the same directory.
runner_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cp "$runner_directory/check-existing-cache.sh" "$INGEST_HOME/bin/ingest/check-existing-cache.sh"
bash "$INGEST_HOME/bin/ingest/check-existing-cache.sh"

run_example() {
    local dataset="$1" helper file status
    case "$dataset" in
        wikipedia) helper=datawaveIngestWikipedia; file="$DW_DATAWAVE_INGEST_TEST_FILE_WIKI" ;;
        annotation) helper=datawaveIngestAnnotation; file="$DW_DATAWAVE_INGEST_TEST_FILE_ANNOTATION" ;;
        json) helper=datawaveIngestJson; file="$DW_DATAWAVE_INGEST_TEST_FILE_JSON" ;;
        csv) helper=datawaveIngestCsv; file="$DW_DATAWAVE_INGEST_TEST_FILE_CSV" ;;
        wikipedia-medium) helper=datawaveIngestWikipedia; file="$DW_DATAWAVE_INGEST_MEDIUM_FILE_WIKI" ;;
        wikipedia-de) helper=datawaveIngestWikipedia; file="$DW_DATAWAVE_INGEST_DE_TEST_FILE_WIKI" ;;
        wikipedia-es) helper=datawaveIngestWikipedia; file="$DW_DATAWAVE_INGEST_ES_TEST_FILE_WIKI" ;;
        wikipedia-fr) helper=datawaveIngestWikipedia; file="$DW_DATAWAVE_INGEST_FR_TEST_FILE_WIKI" ;;
        *) error "Unknown dataset: $dataset"; return 1 ;;
    esac
    info "Running example: $dataset"
    local options="${EXAMPLE_INGEST_OPTIONS:-}"
    if [[ "${CREATE_EXAMPLE_TABLES:-true}" == true ]]; then
        options="$options -createTables"
    fi
    if "$helper" "$file" "$options"; then
        status=0
    else
        status=$?
    fi
    if [[ "$dataset" == csv && "$status" == 251 && "${ALLOW_EXPECTED_CSV_ERRORS:-true}" == true ]]; then
        warn "CSV returned its documented exit 251 for the intentionally invalid fixture record."
        status=0
    fi
    if [[ "$status" != 0 ]]; then
        error "$dataset failed with exit $status; remaining examples were not submitted."
        return "$status"
    fi
    info "Completed example: $dataset"
}

for dataset in "${datasets[@]}"; do
    run_example "$dataset"
done
info "All selected examples completed."
