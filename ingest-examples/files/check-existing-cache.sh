#!/usr/bin/env bash
set -eo pipefail
info() { printf '[INFO] %s\n' "$*"; }
error() { printf '[ERROR] %s\n' "$*" >&2; }

cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
# Legacy discovery probes optional libraries without errexit. Keep that
# behavior while still checking the resulting required JARs below.
if ! source ./ingest-libs.sh; then
    error "Could not resolve the ingest classpath for cache validation."
    exit 1
fi
source ./job-cache-env.sh
if ! hdfs dfs -test -d "$JOB_CACHE_DIR"; then
    error "Existing job cache does not exist: $JOB_CACHE_DIR"
    exit 1
fi
# Compare actual bytes, including SNAPSHOT JARs whose names may be unchanged.
# Read the cache in one transfer rather than starting Hadoop for every JAR.
cached_files=$(mktemp -d)
trap 'rm -rf -- "$cached_files"' EXIT
hdfs dfs -get "$JOB_CACHE_DIR" "$cached_files/cache"
IFS=':' read -r -a classpath_entries <<< "$CLASSPATH"
checked_jars=0
for library in "${classpath_entries[@]}"; do
    [[ "$library" == *.jar ]] || continue
    cached_library="$cached_files/cache/${library##*/}"
    if [[ ! -f "$library" || ! -f "$cached_library" ]]; then
        error "Existing cache is missing a required library: ${library##*/}"
        exit 1
    fi
    local_hash=$(sha256sum "$library")
    cached_hash=$(sha256sum "$cached_library")
    if [[ "${local_hash%% *}" != "${cached_hash%% *}" ]]; then
        error "Existing cache library differs from this image: ${library##*/}. Use a matching ingest image and cache."
        exit 1
    fi
    ((checked_jars+=1))
done
if ((checked_jars == 0)); then
    error "No ingest JARs were found for existing-cache validation."
    exit 1
fi
info "Existing cache matches all $checked_jars required JARs."
