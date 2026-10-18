# Disposable Accumulo hybrid-storage lab

The lab uses namespace `datawave-s3-lab`, instance name `floci-lab`, and the
runtime-matched S3A server image validated in phase 2. It shares the existing HDFS
and ZooKeeper services using separate HDFS and instance identities:

- HDFS volume: `hdfs://hdfs-nn.default.svc.cluster.local:9000/accumulo-s3-lab`
- S3 volume: `s3a://dwv-accumulo-lab/accumulo`
- ZooKeeper name mapping: `/accumulo/instances/floci-lab`

Root/metadata tables and WALs prefer HDFS. The default preference remains HDFS;
only the `floci_canary` table explicitly selects S3. This does not migrate or
reconfigure the existing `dev` instance.

## Run phase 3

After the phase 2 image/configuration/credential probes pass, from the Helm root:

```bash
python3 floci/lab/run-phase3.py \
  --evidence ../datawave-working-notes/floci-integration/evidence/phase-3
```

The helper first installs an inactive lab and compiles a read-only identity
checker. It records original Accumulo/application replica counts, pauses those
controllers, preserves HDFS/ZooKeeper and checks at least 3 GiB of actual free
memory inside minikube-m03. It uses a single manager, tablet server and garbage
collector during verification. Heap is capped through the existing javaOptions
interface; optional monitor, scan servers and external compactors stay off.

Initialization is serialized by the fixed-name `floci-lab-storage-bootstrap`
Job, with parallelism/completions 1 and no automatic retry. Manager automatic
initialization and chart post-install commands are disabled. The read-only
`LabState` checker compares the ZooKeeper mapping, filesystem instance markers,
and format versions before any action. Ambiguous or partial state stops the
workflow; it is never automatically cleared or reinitialized. An existing Job
is reused rather than concurrently replaced. A failed Job needs inspection
before any intentional retry.

A fresh instance initializes on HDFS only. The Job then edits its private config
copy to include both volumes and runs `accumulo init --add-volumes` only when the
identity checker reports INITIALIZED. The REGISTERED state verifies matching
ZooKeeper, HDFS and S3 identities and matching format versions. Only then does
the helper start lab servers with the final hybrid configuration.

Verification pins root/metadata to HDFS, creates a canary, waits for table policy
propagation, inserts one cell and forces flushes. It checks the actual S3A URI in
Accumulo metadata, the corresponding object in Floci, a successful scan, HDFS
system files/WALs, the unchanged original instance ID and a manager restart.
The S3 inventory must contain no root/metadata-table files or WALs. Commands are
submitted through the shell's batch-file interface, not semicolon-separated `-e`.

At completion the helper parks the lab and restores original replica counts,
waiting for the original Pods to be Ready. This preserves memory for normal
DataWave use. Lab data, services and configuration remain available for the next
phase; original Helm release values and images are not upgraded.

## Continue a verified lab session

Use a **new evidence directory** for another work session so its restoration
snapshot reflects current desired replica counts:

```bash
python3 floci/lab/run-phase3.py --action activate \
  --evidence ../datawave-working-notes/floci-integration/evidence/phase-4
# Run the next phase's tests while original compute is paused.
python3 floci/lab/run-phase3.py --action park \
  --evidence ../datawave-working-notes/floci-integration/evidence/phase-4
```

Activation requires the completed bootstrap Job and a registered lab identity.
Parking keeps both registered volume URIs configured and restores the original
controllers from that session's recorded snapshot. Never delete the original
PVCs or change its storage directory to switch test sessions.

## Chart changes

- `storage.volumes` renders a single effective `instance.volumes` property;
  the legacy `config.accumuloSite.instance.volumes` override remains supported.
- `config.accumuloSite` properties are now actually applied without duplicating
  hard-coded defaults. Review existing overrides before a future original-stack
  upgrade; this session upgraded only the isolated lab.
- `initialization.auto`, `manager.replicaCount` and `cmds.enabled` allow controlled
  bootstrap with no competing automatic initializer or post-install hook.
- Manager HDFS DNS checks use the configured host. The init image/pull policy is
  configurable; the lab reuses the verified local server image and client classpath.
- All server roles support the optional credential mounts and environment entries.
  GC/coordinator now mount the Hadoop configuration as well.

The current canary proves initial hybrid placement. Broad compaction/split/GC,
endpoint outages and tablet-server WAL recovery remain phase 4. DataWave ingest
and the full query suite on the new instance remain phase 5.
