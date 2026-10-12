# The original test runner imports this after query.sh. Use mounted PEM identity.
configureUserIdentity() {
    [[ -r "$DW_CURL_CERT" && -r "$DW_CURL_KEY_RSA" && -r "$DW_CURL_CA" ]]
}
reloadDataWaveTableCache() {
    python3 /runner/prepare-queries.py reload
}
