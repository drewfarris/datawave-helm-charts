# Fresh Accumulo with Floci S3

Normal `datawave-driver.sh` deployments now select the fresh hybrid layout when
Floci is enabled (the default). Choose **local** chart mode; it is the prompted
default for this layout. Remote charts do not contain this bootstrap yet.

## Placement

| Data | Storage |
| --- | --- |
| New user-table RFiles | `s3a://dwv-accumulo/accumulo` by default |
| Root and metadata table files | HDFS, pinned before the manager starts |
| WALs and initialization files | HDFS |
| Filesystem default, ingest staging, job cache and iterator JARs | HDFS |

The driver first verifies Floci and bootstraps the application bucket. It then
prepares matching S3A images, namespace-local JCEKS credentials, and a generated
umbrella values overlay. The manager's init container serializes initialization:

1. Require an absent instance and empty HDFS/S3 directories before claiming a
   persistent ZooKeeper ownership record and ephemeral initialization lock.
2. Initialize on HDFS only, configure both volumes, then run `accumulo init --add-volumes`.
3. Verify identical instance UUIDs and data versions in ZooKeeper/HDFS/S3.
4. Persist HDFS preferences for root and metadata using the pinned server's
   property-store API. Only then start the manager and allow dependent clients.

Ordinary restarts of a driver-created instance verify its ownership, volume
configuration and identities and skip initialization/registration already done.
A persistent completed-registration marker prevents missing S3/HDFS state on a
later restart from being mistaken for an unfinished first initialization.
Partial, mismatched or unowned existing data stops startup. A legacy `dwv`
release or orphaned Accumulo resources is also rejected by the driver before
MySQL/Floci/DataWave deployment changes. This workflow provides no migration of
existing Accumulo instances. It never clears data to recover a failed bootstrap.

## Images and prerequisites

The validated local Minikube images must exist in host Docker:

- `local/datawave/accumulo-s3a:2.1.4-hadoop3.4.1`
- `local/datawave/ingest-s3a:8.4.0-SNAPSHOT-hadoop3.4.2`
- `local/datawave/webservice-s3a:8.4.0-SNAPSHOT-hadoop3.4.1`

Use the [S3A build workflow](../s3a/README.md) to build these from the validated
local baseline. The driver compiles its bootstrap inside the actual Accumulo
image (no host Java compiler required), producing
`local/datawave/accumulo-s3a-bootstrap:2.1.4-hadoop3.4.1`.
It compares host Docker IDs with every node's CRI image ID and tag, loading or
replacing images when necessary. Large first-time imports can take minutes;
matching cached images are reused. Docker-backed Minikube, Python 3/PyYAML,
Helm, kubectl and the local chart packager's `yq` are required.

The fresh overlay mounts the credential store and its Secret-backed password in
all Accumulo roles, ingest and web. It uses the corresponding Hadoop 3.4.1/3.4.2
libraries and retains ETag change detection. JCEKS bytes and password remain in
Kubernetes Secrets; a fingerprint prevents reuse with changed AWS credentials.
The generated values file is temporary and removed by the driver's cleanup trap.

## Settings and operation

```bash
./datawave-driver.sh                    # Default: fresh Floci-backed Accumulo
./datawave-driver.sh --check-floci      # Read-only Floci preflight
./datawave-driver.sh --floci-only       # Emulator setup; no Accumulo deployment
```

- `DATAWAVE_ACCUMULO_STORAGE=floci` selects this fresh-only path. It is the
  default when `DATAWAVE_ENABLE_FLOCI=true`.
- `FLOCI_ACCUMULO_VOLUME` changes the S3 URI; the driver bootstraps its bucket.
  Use a dedicated prefix for each independent instance.
- `FLOCI_ACCUMULO_HDFS_VOLUME` overrides the HDFS URI; otherwise it uses the
  selected Accumulo chart's HDFS namenode and storage directory.
- Instance name, ZooKeeper address and root password come from the selected
  values; the generated overlay aligns the Accumulo, ingest and web clients.
- `DATAWAVE_ACCUMULO_STORAGE=hdfs` selects the original HDFS-only setup. Disabling
  Floci defaults to that mode; selecting Floci storage while disabling Floci is
  rejected. Switching a driver-owned hybrid instance to HDFS-only is also rejected.

Keep deployments sequential within the three 8 GiB node limits. Default hybrid
settings use a 768 MiB tablet-server heap, smaller other server heaps, a 1 GiB web
heap/2 GiB web container and a 2 GiB ingest container. Ingest becomes Ready after
table setup and job-cache loading, so web cannot start on a merely Running ingest
container. Existing custom Hadoop properties and extra credential/config mounts
are preserved when generating the overlay.

Diagnostics:

```bash
kubectl -n default logs deployment/accumulo-manager -c initialize-hybrid-storage
kubectl -n default describe pod -l app.kubernetes.io/component=manager
kubectl -n default logs deployment/dwv-dwv-ingest-ingest
```

A successful bootstrap prints `HYBRID_BOOTSTRAP=PASS` and the retained UUID.
An initial `--add-volumes` attempt may report that the new S3 volume has no instance
marker before registering it. Acceptance requires the final identity/version
check and success marker. The pinned Accumulo runtime can also log a VFS cache
cleanup warning on helper JVM shutdown; it is separate from the bootstrap result.

The acceptance probe `VerifyHybrid.java` is restricted to disposable
`driver-fresh-test` volumes. It checks 128 exact records, metadata-referenced S3
RFile bytes, HDFS system-table preferences/files and WALs, and absence of S3
system/WAL paths. It is not run against development tables by the driver.
