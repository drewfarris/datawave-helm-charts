#!/usr/bin/env bash
set -euo pipefail
chart_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
source_dir=${1:?Usage: build-image.sh /path/to/datawave/source}
context_dir=$(mktemp -d "${TMPDIR:-/tmp}/datawave-query-build.XXXXXX")
trap 'rm -rf -- "$context_dir"' EXIT
source_tests="$source_dir/contrib/datawave-quickstart/bin/services/datawave/test-web"
cp "$source_dir/contrib/datawave-quickstart/bin/query.sh" "$context_dir/query.sh"
cp "$chart_dir/files/env.sh" "$chart_dir/files/bootstrap.sh" "$context_dir/"
mkdir -p "$context_dir/test-web/tests" "$context_dir/test-web/responses"
cp "$source_tests/run.sh" "$context_dir/test-web/run.sh"
cp "$source_tests/tests/"*.test "$context_dir/test-web/tests/"
while IFS= read -r file; do
    relative=${file#"$source_tests/responses/"}
    mkdir -p "$context_dir/test-web/responses/$(dirname -- "$relative")"
    cp "$file" "$context_dir/test-web/responses/$relative"
done < <(find "$source_tests/responses" -type f -name '*.expected')
docker build -f "$chart_dir/Dockerfile" -t "${QUERIES_IMAGE:-local/datawave/query-examples:8.2.1-SNAPSHOT}" "$context_dir"
