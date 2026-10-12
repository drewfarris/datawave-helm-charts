# DataWave query examples

This independent Helm chart runs the DataWave quickstart's original
`contrib/datawave-quickstart/bin/services/datawave/test-web/run.sh`, all 14 `.test`
files, and their expected response files against an existing WildFly service.
The build helper copies these from your source checkout. No test assertions are
rewritten, and the main stack/driver does not install this chart.

The Job first waits for the newest `ingest-examples` Job in the same namespace
to complete with the required datasets. A failed or incomplete newest run cannot
be replaced by an older success. The full suite requires the medium English,
German, Spanish and French Wikipedia fixtures in addition to the brief English,
JSON and CSV samples. For an existing ingest release:

```bash
helm upgrade examples ./ingest-examples -n default --reuse-values \
  --set-string runId=16 \
  --set 'examples.datasets={wikipedia,annotation,json,csv,wikipedia-medium,wikipedia-de,wikipedia-es,wikipedia-fr}'
kubectl wait -n default --for=condition=complete \
  job/examples-datawave-ingest-examples-16 --timeout=30m
```

Choose a new runId for your environment. If you loaded samples outside the
chart, disable `prerequisites.requireIngestionJob` only after ensuring the
required fixtures are loaded. For a restricted test selection, adjust
`prerequisites.requiredDatasets` to the samples those tests actually need.

## Build and authenticate

```bash
./query-examples/build-image.sh ../datawave
minikube image load local/datawave/query-examples:8.2.1-SNAPSHOT
```

The image contains scripts and expected responses, with no certificates or
private keys. Supply an existing Secret containing PEM client certificate and
private key, default Secret `query-examples-client` with keys `client.crt` and
`client.key`. The identity must be trusted by WildFly and have `AuthorizedUser`
and `Administrator` or `JBossAdministrator` privileges for queries/cache reloads.
The upstream tests request PUBLIC, PRIVATE, BAR and FOO authorizations.

For the bundled Helm development certificates, create the Secret from the
existing test identity and use the optional proxy overlay:

```bash
kubectl create secret generic query-examples-client -n default \
  --from-file=client.crt=python_deployment_tests/resources/test.crt.pem \
  --from-file=client.key=python_deployment_tests/resources/test.key.pem
helm install queries ./query-examples -n default \
  -f ./query-examples/values-local.yaml
```

For the development mock identities, the main stack must also map the extra
roles to BAR and FOO for both the proxied user and server. The local stack image
overlay includes this configuration:

```yaml
datawave-monolith-umbrella:
  dwv-configuration:
    mockAuthorization:
      additionalRolesToAuths:
        barRole: BAR
        fooRole: FOO
```

After changing these mappings, restart the authorization service and invalidate
its cached users through `/authorization/v1/admin/evictAll`. Clear WildFly's local
credential cache through `/DataWave/Security/Admin/Credentials/flushAll` and verify
`/DataWave/Security/User/listEffectiveAuthorizations` before testing. The development
AuthorizedUser mapping uses AUTH_USER to match the Accumulo root authorizations.

The bundled certificate represents an AuthorizedServer; the overlay proxies the
configured development test user to obtain its query/admin roles. Your server
must permit this proxy identity. Leave the proxy values empty for direct client
authentication. The source checkout's `testUser.p12` may be signed by a different
CA than the Helm repository's bundled truststore; trust must match your cluster.
The original quickstart runner uses `--insecure` for development HTTPS service
certificates. This chart preserves that behavior and requires client TLS
identity authentication.

## Cache reload and test execution

The Job requests reloads of `datawave.metadata`, `datawave.queryMetrics_m` and
`datawave.error_m`. A reload's HTTP 200 only starts asynchronous work. Before
running tests, the Job polls `/Common/AccumuloTableCache/` until every requested
table has a changed, non-epoch lastRefresh and currentlyRefreshing=false.
Authorization failures, absent configured caches, and refresh timeouts fail the
Job. The upstream QueryMetrics tests also call the same checked reload helper.
The service account can only list Jobs within this namespace; it cannot read
Secrets or change workloads. Its token/RBAC are disabled when the ingestion
Job prerequisite is disabled.

The original test runner asserts HTTP status and content type, and compares
expected bodies where upstream tests enable that assertion. Failures propagate
as a nonzero Job exit. Filters selecting zero tests fail rather than passing.
Actual response files are retained inside the completed Pod for diagnosis.

```bash
kubectl logs -n default job/queries-datawave-query-examples-1 -f
kubectl get jobs -n default -l app.kubernetes.io/instance=queries
helm upgrade queries ./query-examples -n default --reuse-values \
  --set-string runId=2
```

Use `tests.whitelistFiles`, `blacklistFiles`, `whitelistTests` and
`blacklistTests` for the upstream filters; individual test IDs have the form
`EventQueryJexlSyntax/CreateJexlFielded`. Keep an entire file selected when its
later tests depend on earlier query creation. `tests.verbose` prints responses.
The default endpoint is `web-datawave:8443`; adjust `endpoint.hostPort` for another
service. Uninstalling `queries` deletes only this release's Job, scripts, service
account and Role/RoleBinding. The externally supplied client Secret and sample
data remain.

## Local compatibility workaround and validation

The Accumulo chart defaults to the following JVM option for the old
generated `EdgeMetadata` Protobuf code:

```yaml
dwv-accumulo:
  javaOptions:
    - -Dcom.google.protobuf.use_unsafe_pre22_gencode=true
```

This is a temporary development workaround. The option disables the old-code
runtime guard; regenerate the generated classes to remove the underlying issue.
Override `dwv-accumulo.javaOptions` with `[]` to disable it. See the
[Protobuf runtime implementation](https://github.com/protocolbuffers/protobuf/blob/v25.6/java/core/src/main/java/com/google/protobuf/GeneratedMessage.java)
and [security advisory](https://github.com/protocolbuffers/protobuf/security/advisories/GHSA-h4h5-3hr4-j3g2).
Accumulo configuration changes trigger pod rollouts through a configuration
checksum. The query chart still requires a completed cache refresh.

Live validation successfully ingested all eight datasets and completed the three
table-cache reloads. The empty-keyword test now expects HTTP 204 with no content
type, following the API's normal empty-result behavior.

The error records were present in `datawave.error_s`, but the ERROR field lacked
global-index metadata and entries. Added the source defaults
`error.data.category.index=*` and `error.data.category.index.reverse=*` to the main
ingest chart, then re-ingested CSV through a separate `error-examples` release
using the existing shared job cache. The original complete all-dataset Job remains
available for the prerequisite. Both ErrorEventQuery tests now return HTTP 200
and pass. An ingest configuration checksum triggers a rollout to synchronize
changed general configuration into the shared PVC; wait for the startup log's
shared-config synchronization before running ingest Jobs after such changes.

The full repaired rerun completed successfully: **56 tests passed, zero failed**.
The query Job completed in 3 minutes 53 seconds, including cache refreshes.

The source JAXB fix adds the concrete AccessExpressionMarkings type to
DefaultTagCloud's markings element. Its round-trip regression test and all 17
web-services/client tests pass. The rebuilt image
`local/datawave/webservice-kubernetes:sandbox-20261012-jaxb` resolves the other
four KeywordExtraction failures.

The development mock maps AuthorizedServer to DW_SERV. Mock lookup converts this
mapping into both a role and an authorization, and query persistence can pass
that authorization to an Accumulo scanner. Upstream quickstart and Docker setup
scripts grant DW_SERV to the Accumulo user. The Accumulo chart root defaults now include DW_SERV to match upstream
development setup. Existing releases using `--reuse-values` need an explicit
`dwv-accumulo.config.auths` override to adopt the updated list.
