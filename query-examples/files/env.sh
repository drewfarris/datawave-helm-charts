# Minimal environment for the unmodified upstream remote test runner.
info() { printf '[INFO] %s\n' "$*"; }
warn() { printf '[WARN] %s\n' "$*" >&2; }
error() { printf '[ERROR] %s\n' "$*" >&2; }
fatal() { error "$@"; }
printGreen() { printf '%s' "$*"; }
printRed() { printf '%s' "$*"; }
printYellow() { printf '%s' "$*"; }
export DW_DATAWAVE_WEB_REMOTE=true
export DW_DATAWAVE_DATA_DIR=/tmp/query-examples
export CURL_ADDITIONAL_OPTS="--connect-timeout 5 --max-time 120 ${CURL_ADDITIONAL_OPTS:-}"
