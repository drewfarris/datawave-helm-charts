# Phase 5: DataWave with Accumulo RFiles on Floci S3

This workflow targets `floci-lab` in `datawave-s3-lab`. It reuses the existing
HDFS, ZooKeeper and MySQL cluster. A namespace-local authorization service
uses the existing mock profile and certificate, with a 512 MiB heap on node02. DataWave configuration,
credentials, job cache, iterator classpath and cached-results database are scoped
to the lab. Existing `dev` storage and its Helm release are not upgraded.

## Verified layout and dependencies

- Accumulo servers: phase 2 S3A image, Accumulo 2.1.4 / Hadoop 3.4.1.
- Ingest examples: annotation-corrected 8.4.0-SNAPSHOT plus Hadoop 3.4.2 S3A
  libraries copied from the verified ingest derivative. The actual Hadoop
  launcher resolved Accumulo's 2.1.6 distribution JAR during direct-reader checks;
  inventory also contains 2.1.5 application dependencies. Code origins are logged.
- Web: the phase 2 8.4.0-SNAPSHOT S3A image, EAR Accumulo client 2.1.5 and the
  WildFly Hadoop 3.4.1 module. Its EAR dependency/module probe reads a real RFile.
- Endpoint: `http://floci.aws-emulation.svc.cluster.local:4566`, region
  `us-east-1`, path style. JCEKS aliases and its password are mounted from
  namespace-local Secrets. Filesystem default, WALs and system tables stay HDFS.
- HDFS staging/cache: `/datawave/floci-lab/ingest`; iterator JARs:
  `/datawave/floci-lab/accumulo-vfs-classpath`. Cached results use database
  `floci_cached_results` and a restricted `floci_lab` MySQL user.

The 256 MiB tablet-server heap used for phase 4's small canary is insufficient
for DataWave compactions. `accumulo-values.yaml` appends a **768 MiB tablet-server
heap** after global options, with a 1536 MiB container limit. Manager and GC
retain their smaller heaps. The web JVM has a 1 GiB heap and 2 GiB container
limit. Ingest clients run on node zero; the tablet server stays on node03.
Run ingestion and query jobs sequentially and inspect actual Docker node memory.
Kubernetes still advertises host-scale memory despite each node's 8 GiB limit.

YARN checks the utilization of the shared host filesystem. Crossing its 90%
threshold makes both NodeManagers unhealthy even when tens of GB remain free.
Record `df`, YARN node health and reclaimable build cache before ingestion.
Reclaiming unused build cache resolved this validation's admission stalls.
Allow space for job-cache localization; keep that margin below the threshold.
`mapred-site.xml` scopes the lab ApplicationMaster to 1024 MiB / 512 MiB heap.

## Preparation and execution

Run from the Helm repository. Keep one evidence directory throughout a session:
its `original-replicas.json` is the restoration snapshot. This validation uses
`../datawave-working-notes/floci-integration/evidence/phase-5`. New sessions need
new identifiers and an evidence directory created while the original stack is
restored. Preserve earlier logs before replacing a test Job.

```bash
FLOCI_EVIDENCE=../datawave-working-notes/floci-integration/evidence/phase-5
# The original stack must be Ready for this fresh baseline.
helm install floci-phase5-baseline-query query-examples -n default \
  -f floci/datawave/baseline-query-values.yaml
python3 floci/datawave/run-phase5.py --action baseline --evidence "$FLOCI_EVIDENCE"

# Snapshot original ingest config read-only before pausing its service.
python3 floci/datawave/run-phase5.py --action prepare --evidence "$FLOCI_EVIDENCE"
python3 floci/lab/run-phase3.py --action activate --evidence "$FLOCI_EVIDENCE" \
  --overlay floci/lab/promoted-values.yaml --overlay floci/datawave/accumulo-values.yaml
```

Build the examples derivative with `floci/datawave/examples.Dockerfile` and load
it on node zero and node03. Load the phase 2 web derivative on node zero. Capture
host Docker IDs and compare the running Pod IDs; use `imagePullPolicy: Never`.
The baseline image IDs and dependency lock are in the phase 2/5 evidence.

```bash
python3 floci/datawave/run-phase5.py --action setup --evidence "$FLOCI_EVIDENCE"
helm install floci-phase5-ingest ingest-examples -n datawave-s3-lab \
  -f floci/datawave/ingest-values.yaml
python3 floci/datawave/run-phase5.py --action ingest-result --evidence "$FLOCI_EVIDENCE"
# Start web after ingestion so the resource envelope stays bounded.
python3 floci/datawave/run-phase5.py --action auth --evidence "$FLOCI_EVIDENCE"
python3 floci/datawave/run-phase5.py --action web --evidence "$FLOCI_EVIDENCE"
python3 floci/datawave/read-files.py --compact --evidence "$FLOCI_EVIDENCE"
helm install floci-phase5-query query-examples -n datawave-s3-lab \
  -f floci/datawave/query-values.yaml
python3 floci/datawave/run-phase5.py --action query-result --evidence "$FLOCI_EVIDENCE"
python3 floci/datawave/direct-readers.py --evidence "$FLOCI_EVIDENCE"
```

The setup guard targets the lab instance before table mutations. It reuses the
namespace and `TableCreator` handles existing tables. Failed setup Jobs require
inspection before deletion/retry. Completed ingestion data is retained; replay
can affect aggregate counts, so avoid replaying fixtures as a recovery step.
The query runner keeps upstream assertions unchanged, records completed
`lastRefresh` advances for three caches and requires all eight datasets.
The known `ExtractEmptyKeywordsByPageId` 204-versus-200 mismatch remains recorded.

## Backup, restore and placement rollback

With lab web writes parked, `read-files.py --rollback` exports an offline,
deterministic 128-row S3 table. It copies its RFiles to HDFS, compares SHA-256,
restores a working copy to a new HDFS table while the user default is HDFS, and
checks every row. Existing DataWave table policies are temporarily pinned to S3.
It restores the S3 default in `finally`, restores prior DataWave preferences,
then verifies a fresh S3 control table. The source table is retained and read
again; both registered volumes remain configured throughout.

The retained export is under
`hdfs://hdfs-nn.default.svc.cluster.local:9000/accumulo-s3-lab/phase5-backup/verified-canary`.
Import consumes a separate `import-work` copy, preserving backup metadata/RFiles.
The helper refuses existing destination/control tables and validates an existing
source canary's 128 exact records before resuming an inspected failed export.

This exercises **one offline table**, not a whole-instance disaster recovery
backup. A coordinated instance backup must preserve ZooKeeper instance state,
all HDFS system/WAL data, S3 object bytes and emulator bucket/service metadata,
configuration and credentials. Quiesce clients and Accumulo, save all parts with
checksums, and validate a separate restore before migrating existing data. An
inventory, retained PVC or a copied RFile alone is not that backup.

## Minikube and host recovery

After stable query, compaction and rollback checks, remove temporary JVM Pods,
create a checkpoint, and restart Minikube:

```bash
kubectl -n datawave-s3-lab delete pod floci-phase5-files --ignore-not-found
python3 floci/datawave/recovery.py --action checkpoint --evidence "$FLOCI_EVIDENCE"
python3 floci/datawave/recovery.py --action restart-minikube --evidence "$FLOCI_EVIDENCE"
```

Verification checks three Ready nodes, 8 GiB Docker limits, kube-proxy and CSI,
retained Floci PVC/PV identity, retained role image IDs, registered Accumulo
identity, immutable canary bytes, and signed S3/DNS access from all three nodes.
It records inotify limits, memory, CPU and disk usage. Run a new query Job after
recovery (for example `--set runId=p5mk`); a previous completed Job is insufficient.

A host reboot ends the active work session. Keep `recovery-checkpoint.json` and
this evidence directory. After reboot, start the existing profile if necessary:

```bash
minikube start -p minikube --driver=docker --nodes=3 --memory=8192
kubectl -n default get innodbcluster mysql-cluster
# If all members are OFFLINE: review dry run, then recover without force.
python3 floci/datawave/recover-mysql.py
# Only after its successful dry run:
python3 floci/datawave/recover-mysql.py --apply
# Clear Router backoff if it has not retried after database recovery.
kubectl -n default delete pod -l component=mysqlrouter
kubectl -n datawave-s3-lab rollout status deployment/authorization --timeout=300s
kubectl -n datawave-s3-lab rollout status deployment/datawave-monolith --timeout=600s
python3 floci/datawave/recovery.py --action verify-host --evidence "$FLOCI_EVIDENCE"
helm upgrade floci-phase5-query query-examples -n datawave-s3-lab \
  -f floci/datawave/query-values.yaml --set runId=p5host
python3 floci/datawave/run-phase5.py --action query-result --evidence "$FLOCI_EVIDENCE"
```

`verify-host` refuses to pass unless the host boot ID differs from the saved
checkpoint. If the lab was parked before reboot, activate it with both overlays
and start lab web before verification. Do not rerun setup, clear the instance,
replace PVCs, or ingest fixtures to repair startup. Inspect node/CSI/DNS/PVC
state, retained image IDs and the captured dependency lock first.

## Restore normal operation

```bash
python3 floci/datawave/run-phase5.py --action park --evidence "$FLOCI_EVIDENCE"
python3 floci/lab/run-phase3.py --action park --evidence "$FLOCI_EVIDENCE" \
  --overlay floci/lab/promoted-values.yaml --overlay floci/datawave/accumulo-values.yaml
```

Parking removes this workflow's temporary JVM Pods, stops lab web, then parks
lab Accumulo and restores the original compute snapshot. Keep lab data, Secrets,
services, source-matched images and the S3 default for later investigation.
Floci remains running. The original release, instance and PVCs are retained.

## Shared database recovery after a complete outage

A Minikube stop/start left all three MySQL members OFFLINE and Router unavailable.
`recover-mysql.py` reads the operator administrator Secret in memory and runs
MySQL Shell complete-outage recovery, using a private temporary container file.
Its default action is a dry run. Review the validation output, then pass `--apply`
to recover the existing `mysql_cluster` without `force`, reinitializing PVCs, or
resetting application credentials. This validation rejoined all three members
using incremental recovery. The helper is specific to this existing cluster.

The original authorization Deployment waits for the intentionally paused original
Accumulo service at startup. The lab's separate authorization Deployment waits
for the lab manager and uses aliases for shared configuration/cache/RabbitMQ.
Load the existing authorization 4.0.3 image on node02 before `--action auth`.
Restart lab web after replacing an old authorization ExternalName alias so its
JVM resolves the new Service address. Park scales both lab web and authorization
to zero before restoring original compute.
