# S3A runtime images and probes

These tools extend the captured local baseline with runtime-matched S3A libraries.
They do not upgrade the existing DataWave release or change Accumulo volumes.

| Runtime | Hadoop | hadoop-aws | SDK v2 bundle |
| --- | --- | --- | --- |
| Accumulo 2.1.4 servers | 3.4.1 | 3.4.1 | 2.24.6 |
| Ingest 8.4.0-SNAPSHOT / Accumulo distribution 2.1.6 | 3.4.2 | 3.4.2 | 2.29.52 |
| WildFly / application Accumulo client 2.1.5 | 3.4.1 | 3.4.1 | 2.24.6 |

`build-images.py` verifies the source image identities against the baseline,
resolves the published runtime dependency sets, hashes each JAR, and records the
resulting image IDs. Large build artifacts remain outside Git. Hadoop images
receive libraries in `/usr/lib/hadoop/lib`. WildFly receives explicit resource
roots in its existing Hadoop module and a configuration link to `/etc/hadoop/conf`.

From the Helm repository root:

```bash
python3 floci/s3a/build-images.py \
  --evidence ../datawave-working-notes/floci-integration/evidence/phase-2
```

Load these derived images only into the node selected for the lab. The phase 2
run used `docker save ... | docker exec -i minikube-m03 ctr -n k8s.io images import -`
to avoid loading gigabytes of optional SDK libraries into every node.

```bash
python3 floci/s3a/run-phase2.py \
  --evidence ../datawave-working-notes/floci-integration/evidence/phase-2
```

Requires Java 11 javac/jar, Python with PyYAML, kubectl and the built/imported images.
The helper preserves existing Hadoop properties, keeps HDFS as `fs.defaultFS`,
and creates a separate `floci-s3a-hadoop-config` in `aws-emulation`.
`hadoop-values.yaml` describes the scoped settings. Explicit `fs.file.impl`,
`fs.hdfs.impl`, and `fs.s3a.impl` avoid JBoss Modules service-loader ambiguity.
Do not blindly replace an existing Helm property list containing extra settings.

Synthetic credentials from `floci-aws-credentials` are written into a Hadoop JCEKS
store using a generated password. Store and password remain in Kubernetes Secret
`floci-s3a-credentials`, copied into `aws-emulation` and `default`. No credential
values are saved as evidence. Application Pods need both a read-only mount at
`/etc/hadoop/credentials` and `HADOOP_CREDSTORE_PASSWORD` from Secret key
`store-password`. Only Secret key `floci.jceks` should be projected as a file.

The Java probe runs through the Accumulo launcher (UID 1001), Hadoop launcher
(UID 1002), and JBoss Modules (UID 998). It asserts mounted configuration is visible
through the classloader before explicitly loading it. Tests cover an 11 MiB
multipart write, SHA-256, random seek across part boundaries, copy/delete rename,
empty objects, list/delete and server-side stale-ETag detection. It records actual
JAR origins and requires `RemoteFileChangedException` after concurrent overwrite.

Use `--roles webservice` to resume only that runtime after resolving a failure;
completion still requires valid passing evidence for all three locked images.
Transient probe Pods are deleted on success and retained after a failure for
diagnosis. No live application image or configuration is changed by the probes.
Full DataWave ingest/query behavior on S3 remains a later phase.
