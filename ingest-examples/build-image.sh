#!/usr/bin/env bash
set -euo pipefail
chart_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
source_dir=${1:?Usage: build-image.sh /path/to/datawave/source}
image=${EXAMPLES_IMAGE:-local/datawave/ingest-examples:8.2.1-SNAPSHOT-annotationfix}
base=${INGEST_IMAGE:-local/datawave/ingest-kubernetes:sandbox-20261011-hadoop341}
context_dir=$(mktemp -d "${TMPDIR:-/tmp}/datawave-examples-build.XXXXXX")
trap 'rm -rf -- "$context_dir"' EXIT
files=(
    contrib/datawave-quickstart/bin/services/datawave/bootstrap-ingest.sh
    warehouse/ingest-wikipedia/src/test/resources/input/enwiki-20130305-pages-articles-brief.xml
    warehouse/ingest-wikipedia/src/test/resources/input/enwiki-20250519-pages-articles-medium.xml.gz
    warehouse/ingest-wikipedia/src/test/resources/input/dewiki-20250520-pages-articles-brief.xml
    warehouse/ingest-wikipedia/src/test/resources/input/eswiki-20250520-pages-articles-brief.xml
    warehouse/ingest-wikipedia/src/test/resources/input/frwiki-20250520-pages-articles-brief.xml
    warehouse/ingest-csv/src/test/resources/input/my.csv
    warehouse/ingest-json/src/test/resources/input/tvmaze-api.json
    warehouse/ingest-annotation/src/test/resources/input/doubleAnnotation.json
)
for file in "${files[@]}"; do
    mkdir -p "$context_dir/$(dirname -- "$file")"
    cp -- "$source_dir/$file" "$context_dir/$file"
done
docker build --build-arg "INGEST_IMAGE=$base" --file "$chart_dir/Dockerfile" \
    --tag "$image" "$context_dir"
