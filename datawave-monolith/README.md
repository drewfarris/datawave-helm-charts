# DataWave Web Helm Chart

Manages All Helm Resources Required to Support a Deployment of DataWave Web

The web umbrella enables `cachedResults` to connect `CachedResultsDS` to its MySQL
cluster. Standalone web installs leave the image's datasource configuration intact
unless `cachedResults.enabled` is true. Configure `host`, `port`, and `database`
and reference a namespace-local Secret through `credentials.secretName`,
`usernameKey`, and `passwordKey`. Credentials are supplied through Secret-backed
environment variables; runtime ConfigMaps contain only environment references.
Runtime configuration changes trigger a web pod rollout. Rotate credentials by
restarting the web deployment after updating the Secret.
