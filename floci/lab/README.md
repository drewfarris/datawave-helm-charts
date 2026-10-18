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
memory inside minikube-m03 for a parked lab. Reconfiguring an already running
core lab requires 1 GiB of free headroom for rolling replacement; optional roles
are enabled separately. It uses a single manager, tablet server and garbage
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

The phase 3 canary proves initial hybrid placement. Phase 4 extends verification
to compaction/split/GC, endpoint outages and tablet-server WAL recovery. DataWave ingest
and the full query suite on the new instance remain phase 5.

## Phase 4 storage semantics

The phase 4 helpers use `StorageSemantics.java` against the existing lab. The Java
probe refuses any instance other than `floci-lab` and any table without the
`phase4_` prefix. Creation fails if the table already exists; inspect an earlier
run rather than automatically deleting its data. The runner compiles against
Accumulo 2.1.4/Hadoop 3.4.1 and copies the probe JAR into a dedicated 512 MiB client Pod's temporary
directory; rebuilding the runtime image is unnecessary.

Use a fresh session evidence directory and activate the lab as above. Then run:

```bash
EVIDENCE=../datawave-working-notes/floci-integration/evidence/phase-4
python3 floci/lab/run-phase4.py --evidence "$EVIDENCE" --stage baseline
python3 floci/lab/run-phase4.py --evidence "$EVIDENCE" --stage lifecycle
```

Baseline writes 2,000 deterministic 1 KiB values in four synchronous WAL batches,
flushes each batch, checks every scanned value and 40 seeded point reads, then
retrieves/hashes every metadata-referenced S3 RFile. Lifecycle forces an internal
major compaction and three splits. `external-values.yaml` enables one coordinator
and one external compactor; `scan-values.yaml` enables one default-group scan
server. Apply each overlay **separately**, with both registered volumes and the
three core replicas enabled, to respect the node's actual memory limit:

```bash
python3 floci/lab/run-phase3.py --action activate --evidence "$EVIDENCE" \
  --overlay floci/lab/external-values.yaml
python3 floci/lab/run-phase4.py --evidence "$EVIDENCE" --stage external
# Capture compactor logs before switching away from the role.
python3 floci/lab/run-phase3.py --action activate --evidence "$EVIDENCE" \
  --overlay floci/lab/scan-values.yaml
python3 floci/lab/run-phase4.py --evidence "$EVIDENCE" --stage scanserver
python3 floci/lab/run-phase4.py --evidence "$EVIDENCE" --stage delete
python3 floci/lab/run-phase4.py --evidence "$EVIDENCE" --stage gc
```

The eventual-read probe waits for scan servers instead of falling back to tablet
servers. GC verification checks that the four baseline files, the internal
compaction output and the deleted table's file disappear while live references
and all records remain readable. The GC polling window is three minutes; the lab
uses a five-second GC cycle during this phase.

Return to the three core roles before failure tests:

```bash
python3 floci/lab/run-phase3.py --action activate --evidence "$EVIDENCE"
python3 floci/lab/run-phase4-failures.py --evidence "$EVIDENCE" --stage restarts
python3 floci/lab/run-phase4-failures.py --evidence "$EVIDENCE" --stage crash
python3 floci/lab/run-phase4-failures.py --evidence "$EVIDENCE" --stage outage
```

Restarts replaces Floci and the lab tablet server separately, then checks all
records and S3 RFiles. Crash acknowledges 200 additional mutations with SYNC WAL
durability, records the unchanged pre-crash files, sends SIGKILL to the tablet
server JVM and verifies 2,200 records after recovery. Outage temporarily stops the Floci backend (retaining its PVC) and removes
Service endpoints, tests a fresh S3 read and a flush with a 90-second client
bound, checks that no user RFile appeared in HDFS, restores the backend and routing in `finally`,
and verifies flush/scan/S3 placement after recovery. Accumulo may keep retrying
the server-side flush while the client is bounded; the test does not claim that
server background retries terminate after 90 seconds. Floci is shared by the lab
probes, so avoid other emulator work during the outage test.

After all gates pass, promote the **lab user-table default** to S3 and verify a
new table without a table-level volume override:

```bash
python3 floci/lab/run-phase3.py --action activate --evidence "$EVIDENCE" \
  --overlay floci/lab/promoted-values.yaml
python3 floci/lab/run-phase4.py --evidence "$EVIDENCE" --stage default
kubectl -n datawave-s3-lab delete pod floci-phase4-client
python3 floci/lab/run-phase3.py --action park --evidence "$EVIDENCE" \
  --overlay floci/lab/promoted-values.yaml
```

Keep `--overlay floci/lab/promoted-values.yaml` when resuming for phase 5 so the
verified S3 default persists. Explicit root/metadata preferences, initialization
and WAL placement remain HDFS. The original instance is not upgraded.
