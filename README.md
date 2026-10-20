## Generate Image Pull Secrets ##
Running this stack uses images in the ghcr repository which are not public. The following generates a template to use for imagePullSecrets in helm charts.


Through the GitHub UI, generate a PAT with at least read packages

```bash
./create-image-pull-secrets.sh <github username> <gihub PAT>
```


### DataWave Stack Helm Deployment ###

This repository holds Helm charts and Docker files used to deploy Datawave locally for testing. 

Prerequisites:

* docker
* helm
* kubectl
* minikube (for local testing)


If you already have the prerequisites installed you can simply run `./datawave-driver.sh`

The driver installs or upgrades the MySQL Operator as release `dwv-mysql-operator`
in namespace `mysql-operator`, separately from the DataWave release `dwv` in your
selected namespace. The operator chart cannot be installed in `default`. The
driver waits up to five minutes for the operator and stops if setup fails. It pins
the tested operator chart version to `2.3.0`; override it with
`MYSQL_OPERATOR_CHART_VERSION` when validating another version.

### Floci emulated AWS

The driver deploys the independent Floci release before DataWave by default.
Use `./datawave-driver.sh --check-floci` for read-only preflight or
`./datawave-driver.sh --floci-only` to install and verify it on the current
cluster without upgrading DataWave. Set `DATAWAVE_ENABLE_FLOCI=false` to skip
this step. Python 3 with PyYAML is required when Floci is enabled.
See [Floci driver options and client configuration](floci/README.md#driver-integration).
Normal deployment now configures a **fresh Accumulo instance** with user RFiles
on Floci S3 and system tables/WALs on HDFS. Local chart mode is required and
is the default. Existing unowned Accumulo instances are rejected; no migration
is provided. See the [fresh-instance workflow](floci/fresh/README.md).

### Using locally built application images

Choose `local` chart mode to use changes in this checkout. Local packaging uses a
temporary copy without existing `.tgz` archives and stops on packaging errors.
It does not publish charts or images.

The local charts select `local/datawave/ingest-kubernetes:8.4.0-SNAPSHOT` and
`local/datawave/webservice-kubernetes:8.4.0-SNAPSHOT` with `IfNotPresent`.
Build both images from the sibling DataWave repository using Java 11, Maven,
Docker, and RPM tooling:

```bash
cd ../datawave
mvn -Pkubernetes -Ddeploy -Dtar -DskipTests -DskipITs -Ddocker-release \
  -Dregistry.namespace=local/ \
  -pl web-services/deploy/configuration,warehouse/assemble/datawave,web-services/deploy/application \
  -am install
cd ../datawave-helm-charts
```

Load the images into every Minikube node before deployment:

```bash
minikube image load local/datawave/ingest-kubernetes:8.4.0-SNAPSHOT
minikube image load local/datawave/webservice-kubernetes:8.4.0-SNAPSHOT
./datawave-driver.sh
```

For the local three-node cluster with 8 GiB per node, keep WildFly on the primary
node to avoid crowding the stateful services on `minikube-m02`:

```bash
DATAWAVE_VALUES_OVERLAY="$PWD/datawave-stack/values-minikube.yaml" ./datawave-driver.sh
```

The ingest build inherits Commons Lang from the root DataWave POM. Keeping an
older warehouse override can prevent table creation with an Accumulo launcher
`NoSuchMethodError`.

Choose `local` chart mode to use these image settings. When reusing an existing
cluster, the driver skips Minikube setup and image loading. The optional
`datawave-stack/values-local-images.yaml` overlay selects the same image tags
and adds test authorization roles. Pass its path through `DATAWAVE_VALUES_OVERLAY`
when those roles are needed. A full ingest/query round trip should be checked
after installation.





## Standalone test-data ingestion

The [ingest-examples chart](ingest-examples/README.md) runs the quickstart
Wikipedia, annotation, JSON, and CSV examples as a separate Helm release against
an existing DataWave stack. It is not installed by `datawave-driver.sh`.

The [query-examples chart](query-examples/README.md) runs the quickstart WildFly
API tests independently. It requires completed sample ingestion and waits for
metadata table-cache reloads before executing queries.

## Testing of images and helm charts ##

Images are now created in [Datawave Stack Docker Images](https://github.com/nationalSecurityAgency/datawave-stack-docker-images)

If you want to use an external Hadoop and/or Zookeeper instance then set the appropriate env var(s) to true.
```bash
export USE_EXISTING_ZOOKEEPER=true
export USE_EXISTING_HADOOP=true
```

And set the path to the installation(s) appropriately for your system.
```bash
export ZOOKEEPER_HOME=/opt/zookeeper
export HADOOP_HOME=/opt/hadoop
```

In order to test changes to helm charts, you can run the following script:
```bash
./datawave-driver.sh
```
This will package all the helm charts from the local directories, and launch the cluster, either in minikube or using a preconfigured k8s cluster.

## Updating Helm schemas
```bash
helm plugin install https://github.com/losisin/helm-values-schema-json.git
find * -maxdepth 0 -type d -exec sh -c "cd {}; helm schema" \;
```

## Local AWS emulation

The independent [Floci chart](floci/README.md) provides persistent S3 emulation
inside Minikube. Its installation and compatibility probes are separate from
the driver; configuring Accumulo to use S3 is a later integration step.

## Troubleshooting Datawave

### Recovery after a host reboot

Start the existing cluster with `minikube start`. If worker `kube-proxy` pods
crash with `fsnotify watcher init: too many open files`, check the Linux host's
`fs.inotify.max_user_instances`. The default of 128 can be exhausted by this
three-node Docker cluster. Persist a larger limit on the host:

```bash
printf 'fs.inotify.max_user_instances = 1024\n' | sudo tee /etc/sysctl.d/90-datawave-minikube.conf
sudo sysctl -p /etc/sysctl.d/90-datawave-minikube.conf
```

Restart the failed worker kube-proxy pods after applying the limit. Until they
recover, worker pods can lose access to cluster DNS and the Kubernetes API,
which also causes CSI provisioner crashes and stalls application init containers.
Retry CSI pods still in backoff after service routing is restored. Existing
volumes and locally loaded images should be preserved.

### Authorization
The first thing to check is that certificates trusted by the Datawave stack are being used. This may require adding public certificates to the truststore Datawave utilizes or configuring Datawave with a new truststore that contains the correct certificates.

A Python library is available to provide easy CLI interactions with Datawave and can be found at the [Datawave CLI](https://github.com/AFMC-MAJCOM/datawave-cli). It utilizes Python's request library on the backend.

Python's request library (at least the version used in the Datawave CLI) does not work with non-pem certificate formats. If modifications are being made to use custom certificates, ensure they are converted to pem format, or avoid using Python. Accessing a Datawave endpoint from a browser allows selection of trusted certificates loaded into the cert store.

If authorization is successful but the expected results are not being returned, try hitting the `whoami` endpoint either in a browser or by using `datawave authorization` from the Datawave CLI Python library. This will indicate what authorizations and roles the user has in Datawave. Datawave users must have AuthorizedUser at a minimum to make queries.

Datawave may cache users, so if updates to a user are not reflected, the cache may need to be cleared. This can be done by an admin user with the evict endpoints of the authorization endpoint: `dwv-authorization.***/authorization/v2/admin/evictAll` or `/evictUser`.

#### Logs
**Pod:** dwv-web-authorization-*

**Log:** ~/logs/authorization-service.log

### Ingest
The biggest issues with ingesting new data types are often related to formatting the `values.yaml` of the new datatype or the JSON being uploaded. Datawave's documentation provides a detailed outline of what everything needs to be. Ensure everything is set up correctly and that the JSON file being ingested is formatted properly with all required fields (i.e., the indexed ones).

It should also be noted that dashes in the datatype name were not functional. There does not appear to be a clear reason for this, but it did not work.

The ingest job status can be monitored by checking the YARN status on the Hadoop pod. There are three ways to do this:
1) Use `datawave ingest`. If no file is passed for ingest, it will display the YARN job statuses.
1) Execute `yarn node -list` manually in the pod `*-hadoop-yarn-rm-0`.
1) Access the web GUI by port-forwarding the `*-hadoop-yarn-rm-0` pod's 8088 port.

#### Logs
**Pod:** [helm release name]-dwv-ingest-ingest-*

**Log:**
Several log files are present. Only the live ingest service logs are relevant.
* `flag_maker_flag-maker-live.log` can help identify why an ingest isn't starting if it's not being flagged.
* `live-ingest.log` updates on a timer and indicates when new data is detected but may not be useful in most cases due to constant updates.
* File-specific logs for ingested files contain errors and the results of the ingest job. For log-level details, start here.

### Query
The following is a list of common errors and their usual causes:
* **ERROR: The query contained fields which do not exist in the data dictionary for any specified datatype**
  * After redeploying and uploading data, a cache refresh might be needed. Use the `datawave accumulo` command to do this.
    * It may need to be invoked twice for a timely refresh. The reason for this is unclear.
    * Once completed, the cache should not need refreshing again unless a new datatype is uploaded or an existing datatype structure is changed.
  * Other common causes include:
    1) Searching for a column that does not exist. Verify the spelling. Datawave normalizes it, so capitalization is not important.
    1) Forgetting to wrap the value in quotes. For example, `GENRES == IntegrationTest` would return this error since IntegrationTest is not quoted.

* **ERROR: User requested authorizations that they don't have**
  * This error indicates what authorizations are missing.
  * Use `datawave authorization` to check the authorizations the user has, then resolve the issue from there.

* **ERROR: Full table scan required but not enabled**
  * This occurs when querying a field that is not indexed. When creating a new datatype, specify which fields should be queriable. Attempting to search a field that is not set up as queriable triggers this error.
  * It can also result from incorrect JEXL formatting. Here are two examples:
    1) `GENRES == 'IntegrationTest' & LANGUAGE == 'English'` — the issue is using a single `&` instead of `&&`.
    1) `GENRES = 'IntegrationTest'` — in this case, a single `=` is used instead of `==`.

* **If no results are returned when some are expected:**
  1) Confirm the ingest job completed. Check by calling `datawave ingest` without passing a file.
  1) The cache may need refreshing. Use `datawave accumulo` to request a refresh and wait a moment.
  1) Double-check that the requested authorizations match the visibility uploaded.
  1) If none of these resolve the issue, check the `Query.log` at the location listed below. The logs are dense, so using `grep` with `-B` and `-A` to show lines before and after the match might help.

#### Logs
**Pod:** dwv-web-datawave

**Log:** *$WILDFLY_HOME/standalone/log/*
  * `Query.log` - logs the query and the steps performed for it.
  * `Security.log` - logs any security requests made through the Datawave app.
