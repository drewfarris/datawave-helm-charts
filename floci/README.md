# Floci for the local DataWave environment

This independent chart exposes an emulated S3 endpoint. It is not installed by
`datawave-driver.sh`, and it does not change Accumulo volumes or DataWave settings.

## Local defaults

- Namespace/release: `aws-emulation` / `floci`.
- Endpoint: `http://floci.aws-emulation.svc.cluster.local:4566`.
- Region: `us-east-1`; use path-style S3 requests.
- Server image: Floci 2.3.0, pinned by OCI index digest in `values.yaml`.
  This published image is native, not the alternate JVM image in the upstream
  `docker/Dockerfile`; native heap is capped at 192 MiB, container memory at 512 MiB.
- Single replica on `minikube-m03`; no Docker socket, ingress, or service-account token.
- Persistent mode, with `/app/data` on a 10 GiB PVC. Both metadata/journals and
  object bytes must remain on this mount.
- A dedicated CSI StorageClass uses `WaitForFirstConsumer` and the selected
  node's topology. The cluster's default Immediate class can bind the volume
  to another node. Set `persistence.createStorageClass=false` and supply an
  appropriate existing class when adapting this chart to another environment.
- PVC and StorageClass survive Helm uninstall; the volume has Retain policy.
- S3 is enabled; other configured services are disabled. S3 authentication
  enforcement is disabled for this local emulation. Synthetic client credentials
  select the test account; this deployment does not validate AWS IAM policies.

The cluster currently advertises host-scale memory to the scheduler despite
8 GiB Docker limits per node. Check Docker/cgroup usage before adding clients.
Do not place additional persistent workloads on the nearly full `minikube-m02`.
A second full DataWave stack does not fit the measured budget comfortably.

## Build and install

Run from the Helm repository root. Docker, Minikube, Helm and kubectl are required.
The Python probe image contains pinned AWS CLI and boto3 dependencies; Python is
not required inside the Floci server image.

```bash
docker build -t local/datawave/floci-probe:phase1-20261018 ./floci/tools
minikube image load local/datawave/floci-probe:phase1-20261018
```

Create namespace `aws-emulation` and an external Kubernetes Secret named
`floci-aws-credentials` in it, with keys `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` containing synthetic development credentials. Keep the
values outside the repository. Clients in `default` need a Secret with the same
keys and values in their own namespace; Secrets cannot be mounted across
namespaces. Existing DataWave services are not automatically wired to this Secret.

```bash
helm lint ./floci
helm template floci ./floci -n aws-emulation
helm install floci ./floci -n aws-emulation --wait --wait-for-jobs --timeout 5m
```

The bootstrap Job creates `dwv-s3-smoke` and `dwv-accumulo-lab`, verifies them,
and runs an AWS CLI bucket listing. It tolerates only verified already-owned
buckets. For an upgrade that changes the bootstrap Pod template, use a new ID:

```bash
helm upgrade floci ./floci -n aws-emulation \
  --set-string bootstrap.runId=2 --wait --wait-for-jobs --timeout 5m
```

Kubernetes service links are disabled and `FLOCI_PORT` is set explicitly. Without
this, a Service named `floci` injects a `tcp://…` value into `FLOCI_PORT`, which
Floci cannot parse as a numeric port. Startup/liveness use `/health`; readiness
uses the S3 root bucket-listing endpoint.

## Phase 1 verification

Before connecting application clients, run the isolated compatibility and
restart checks. This deliberately interrupts **Floci only** and should run while
no other workloads depend on this emulator. The helper requires local Docker
access to force-stop the identified Floci container inside `minikube-m03`.
It does not restart Minikube or any DataWave/Hadoop/Accumulo services.

```bash
python3 ./floci/tools/run-phase1.py \
  --evidence ../datawave-working-notes/floci-integration/evidence/phase-1
```

The helper uses transient probe Pods, signed S3 requests from all three nodes,
the API probes in `files/s3_probe.py`, and durable sentinel hashes. It checks
SIGTERM recovery, Pod replacement, and a forced stop with recorded exit 137.
It preserves bucket/object metadata and content evidence, including a multipart
object. Failures stop the run and leave probe Pods for diagnosis; inspect and
remove only Pods labeled `app.kubernetes.io/name=floci-phase1-probe` when done.
The test prefix `phase1-api/` is cleared by the API suite; `durability/` objects
are retained as restart sentinels. Do not use these prefixes for application data.
Host reboot persistence and Hadoop S3A compatibility are separate later gates.

## Access and diagnostics

From an in-cluster client, supply the endpoint, region and the namespace-local
credential Secret explicitly. A host client can use:

```bash
kubectl port-forward -n aws-emulation service/floci 4566:4566
# In another terminal, with the synthetic credentials configured:
aws --endpoint-url http://127.0.0.1:4566 --region us-east-1 s3api list-buckets
```

Useful checks:

```bash
kubectl get pods,pvc -n aws-emulation -o wide
kubectl logs -n aws-emulation floci-0
kubectl logs -n aws-emulation job/floci-bootstrap-2
kubectl get pv -o wide
docker stats --no-stream minikube minikube-m02 minikube-m03
```

The native build may log one-time Docker reconciliation warnings from disabled
service components at startup. S3 does not require the Docker socket; API,
storage and readiness evidence determine this deployment's acceptance.

## Retention and recovery

A normal Pod restart reuses `floci-data`. Preserve the original PVC/PV when
troubleshooting; a replacement empty PVC is not recovery. Retained volumes are
local to their Minikube node, and requested size is not an independently reserved
host disk. Deleting Minikube or the host's backing storage can destroy this data.
Do not use cluster-reset scripts for an application rollback.

`helm uninstall floci -n aws-emulation` removes compute/service resources while
keeping the PVC and StorageClass. Keep namespace/Secret/PV metadata and object
backups when planning an emulator upgrade. Once Accumulo references S3 files,
keep Floci and its data available until a verified migration removes those
references; reverting Helm values does not move RFiles.
