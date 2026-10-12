#!/usr/bin/env bash
set -eo pipefail
export DW_BASE_URI="https://${HOST_PORT:?HOST_PORT is required}/DataWave"
export DW_QUERY_URI="$DW_BASE_URI/Query"
export DW_CURL_CERT=/client/client.crt
export DW_CURL_KEY_RSA=/client/client.key
# Upstream quickstart tests use --insecure for development service certificates.
export DW_CURL_CA="$DW_CURL_CERT"
if [[ -n "${PROXIED_SUBJECT:-}" || -n "${PROXIED_ISSUER:-}" ]]; then
    [[ -n "$PROXIED_SUBJECT" && -n "$PROXIED_ISSUER" ]] || { echo '[ERROR] Both proxy identity fields are required.' >&2; exit 1; }
    export CURL_ADDITIONAL_OPTS="$(python3 -c 'import os, shlex; args=["--header", "X-ProxiedEntitiesChain: <"+os.environ["PROXIED_SUBJECT"]+">", "--header", "X-ProxiedIssuersChain: <"+os.environ["PROXIED_ISSUER"]+">"]; print(" ".join(map(shlex.quote,args)))')"
fi
python3 /runner/prepare-queries.py prepare
args=(--no-cleanup)
[[ "${TEST_VERBOSE:-false}" != true ]] || args+=(--verbose)
[[ -z "${WHITELIST_FILES:-}" ]] || args+=(--whitelist-files "$WHITELIST_FILES")
[[ -z "${BLACKLIST_FILES:-}" ]] || args+=(--blacklist-files "$BLACKLIST_FILES")
[[ -z "${WHITELIST_TESTS:-}" ]] || args+=(--whitelist-tests "$WHITELIST_TESTS")
[[ -z "${BLACKLIST_TESTS:-}" ]] || args+=(--blacklist-tests "$BLACKLIST_TESTS")
test_runner=/opt/query-examples/bin/services/datawave/test-web/run.sh
listing=$(bash "$test_runner" --list-tests "${args[@]}")
if ! grep -Eq '^[[:space:]]+[0-9]+:' <<< "$listing"; then
    echo '[ERROR] Test filters selected no upstream tests.' >&2
    exit 1
fi
exec bash "$test_runner" "${args[@]}"
