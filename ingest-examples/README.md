# DataWave example ingestion Job

This standalone chart submits quickstart test data to an **existing** DataWave
stack. It is not a dependency of `datawave-stack` and is not installed by the
driver. Its release owns one Job and a runner ConfigMap; existing stack resources
are referenced by name and are not modified by Helm.

The container sources the DataWave source checkout's
`contrib/datawave-quickstart/bin/services/datawave/bootstrap-ingest.sh` and calls
its `datawaveIngestWikipedia`, `datawaveIngestAnnotation`, `datawaveIngestJson`,
and `datawaveIngestCsv` functions. These upload fixtures to HDFS and execute
`live-ingest.sh` to submit MapReduce jobs that write directly to Accumulo. It does
not install or start local quickstart services.

## Build the image

From this chart repository, with the corrected ingest image available in Docker:

```bash
./ingest-examples/build-image.sh ../datawave
minikube image load local/datawave/ingest-examples:8.2.1-SNAPSHOT-annotationfix
```

The ingest base must contain the same Hadoop version as DataWave and the cluster's
NodeManagers (currently 3.4.1). Hadoop clients come from that image; no separate
init container or runtime framework download is required.

The helper copies only the bootstrap script and eight test fixtures into a small
temporary build context. No Maven rebuild or runtime download is needed. It
preserves the base image's four stock example datatype configs before the Job
loads deployed settings. To use different tags:

```bash
INGEST_IMAGE=your-registry/ingest:your-tag \
EXAMPLES_IMAGE=your-registry/ingest-examples:your-tag \
  ./ingest-examples/build-image.sh /path/to/datawave
```

Use the source checkout and ingest base image from the same DataWave version.
The default base is the locally tested `sandbox-20261011-hadoop341` image, which
includes Hadoop 3.4.1, the Commons Lang alignment, and `lsof` fixes. Update chart `image` values
when using a different examples image. For a remote cluster, publish the examples
image to your registry and configure `imagePullSecrets` if needed.

## Run independently

Wait until the main ingest deployment has finished starting and copied its
configuration to the shared PVC. Install into the **same namespace** as that stack:

```bash
helm install examples ./ingest-examples --namespace default \
  --wait --wait-for-jobs --timeout 60m
```

The default run ingests the brief English Wikipedia, annotation, TVMaze JSON,
and CSV examples sequentially. To select only Wikipedia and JSON:

```bash
helm install examples ./ingest-examples --namespace default \
  --set 'examples.datasets={wikipedia,json}' \
  --wait --wait-for-jobs --timeout 60m
```

All supported datasets are `wikipedia`, `annotation`, `json`, `csv`,
`wikipedia-medium`, `wikipedia-de`, `wikipedia-es`, and `wikipedia-fr`.
The last four reproduce the additional content examples in `datawaveIngestExamples`.

The bundled annotation fixture includes a value for every segment and a point
for every POINTS boundary. Use the corrected source fixture when building the
image; older copies of `doubleAnnotation.json` are rejected by validation.

Fixtures are included in the image rather than Kubernetes ConfigMaps, including
the compressed medium Wikipedia sample. Keep `examples.numShards` aligned with
the deployed shard configuration (default 10).

Defaults reference the `dwv` stack's shared ingest PVC, ingest-env/genders/Hadoop/
Accumulo ConfigMaps, and the `dwv-dwv-ingest-cmds` Secret. Adjust `existing` values
for another stack. The Secret's password script is mounted directly; credentials
are not copied into this chart's values or runner ConfigMap. `additionalLibraries`
is reserved for extra application dependencies. Standard Hadoop/WebSocket clients
come from the ingest image; do not add mismatched Hadoop framework JARs.

The Job reads shared configuration, then restores its image's stock Wikipedia,
annotation, JSON, and CSV datatype definitions **inside its own container**. This
keeps deployed connection/table settings while matching the quickstart samples;
the current main chart's `mycsv` override describes JSON, not the CSV fixture.
The shared PVC is mounted read-only. RPM configuration files are made writable
in the Job container before staging, and selected example types (including
annotation) are enabled in that private configuration. The main stack must have
completed its basic initialization. By default `examples.createTables=true` passes
`-createTables` to IngestJob so it initializes/configures required example tables,
including annotation tables omitted by the main stack. Disable it to require all
example tables and their configuration to exist already.

Each run reuses the main ingest service's existing HDFS job cache. Set
`jobCache.directory` to `JOB_CACHE_DIR` from the running service's
`bin/ingest/job-cache-env.sh`; the default is `/datawave/ingest/work/jobCacheB`,
created by a fresh default stack installation. Do not rely on the example image's
baked A/B cache selection after upgrades or cache rotation.

```bash
kubectl exec -n default deployment/dwv-dwv-ingest-ingest -- \
  cat /opt/datawave-ingest/current/bin/ingest/job-cache-env.sh
helm install examples ./ingest-examples --namespace default \
  --set jobCache.directory=/datawave/ingest/work/jobCacheB
```

The example image must use the same ingest build and Hadoop libraries as the
cache producer. Before submitting jobs, the runner downloads a temporary local
copy of the selected cache and compares SHA-256 hashes of every required JAR.
Missing or different libraries fail the Job before any dataset is submitted;
version labels alone are insufficient for rebuilt SNAPSHOT artifacts. The
verification download adds local disk use and an HDFS read, then is deleted.

Example-specific XML configuration is loaded by the submission process and
passed in the MapReduce job configuration; it is not compared byte-for-byte with
the service's cached XML. Per-run inputs and job work remain isolated. The Job
does not upload, rotate, or delete the shared cache or update its ZooKeeper
selection. Keep the selected cache available and unchanged while the jobs run.
If a cache rotates or an ingest image is rebuilt, select the matching cache and
image before another run.

## Results and reruns

```bash
kubectl get jobs -n default -l app.kubernetes.io/instance=examples
kubectl logs -n default job/examples-datawave-ingest-examples-1 -f
```

An unexpected ingest/upload failure stops later examples and fails the Kubernetes
Job. By default, the CSV fixture's documented exit 251 is accepted: its deliberate
invalid record demonstrates the `error*` tables, while valid records are ingested.
Set `examples.allowExpectedCsvErrors=false` to treat that exit as failure too.

Job retries are disabled by default to avoid resubmitting successful earlier
examples after a failure. A run may partially ingest data before failing. Reruns
may write the same data again; removing this release does not remove Accumulo data
or uploaded HDFS fixtures.

The completed Job is retained by default for log inspection. To run again, change
`runId` (at most 10 lowercase letters/digits/hyphens):

```bash
helm upgrade examples ./ingest-examples --namespace default --set-string runId=2 \
  --wait --wait-for-jobs --timeout 60m
```

Changing other Job settings without changing `runId` hits Kubernetes's immutable
Job template restriction. A new run replaces the old Job in this Helm release;
export logs first if needed. Alternatively, uninstall/reinstall or use a new
release name. Every Job uses its own HDFS input subdirectory.

```bash
helm uninstall examples --namespace default
```

This removes only the example release's Job and runner ConfigMap. Set
`job.ttlSecondsAfterFinished` to opt into automatic Job cleanup.
Successful ingestion is separate from query validation; check the data through
DataWave's query API afterward.

## Validation

```bash
helm lint ./ingest-examples
helm template examples ./ingest-examples
python3 ingest-examples/tests/test_runner.py
```

Runner tests use the real checkout's quickstart helper functions with mocked HDFS
and MapReduce submissions. Set `DATAWAVE_SOURCE` if that checkout is not `../datawave`.
They cover all eight dataset mappings, expected/strict CSV failures, failed uploads,
and stopping after unexpected job failures.
