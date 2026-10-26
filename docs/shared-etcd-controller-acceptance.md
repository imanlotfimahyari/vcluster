# Controller-managed migration and recovery acceptance

`TestWorkerControllerMigrationAndRecovery` runs in the disposable offline VM.
A real K3s Deployment/ReplicaSet controller, scheduler, kubelet and containerd
manage the vCluster runtime Pod. The Pod runs the real API server and syncer
from the static integration executable. Readiness makes a CA-verified,
authenticated request; no controller status is seeded or synthesized.

The legacy fixture explicitly disables global etcd compaction and DB metric
polling. Migration consumes only those zero-duration options at cutover, where
tenant-root mode supplies the same disabled settings. Other etcd flag overrides
remain rejected. Virtual controllers are disabled, so the fixture seeds its
default ServiceAccount after backing-API readiness, before proxy readiness.

The controller run uses four emulated CPUs and the 7 GiB memory. A cheap TCP startup probe delays
the authenticated exec readiness probe until the API port is listening; final
readiness still requires a successful authenticated API request. VM probe
request deadlines use the existing VM timeout multiplier. The default worker
runner remains at two CPUs unless `--cpus` is supplied.

## Assertions

1. A real Deployment-managed legacy tenant starts with API data, actual
   application Pods, DNS and a bound, mounted static node-local PVC.
2. The actual migration CLI scales down that Deployment, observes shutdown,
   retires the old writer and reaches verified copy. The retired writer cannot
   write, while a second authenticated etcd identity still writes and reads its own
   root. The separate two-runtime worker suite covers another live tenant API
   and application; this fixture avoids a third API-server process.
3. Resume/commit rotates the mounted etcd credential and updates the existing
   configuration Secret. A new control-plane Pod must reach controller/API
   readiness. Its saved API value, application DNS and durable bytes survive;
   configuration/TLS Secret UIDs and PVC/PV bindings remain unchanged.
4. A logical backup is followed by a deliberate API-data change. A new recovery
   operation recreates another control-plane Pod and restores the saved value.
   The TLS certificate changes again without replacing the Secret UID.
5. Both application Pods are replaced. A new Service DNS record must return the
   original volume bytes, the foreign-root client still progresses, and the old legacy source
   remains retained.

The durable file is written once and is not recreated at server startup. This
checks retained storage; logical recovery does not back up or restore volume
contents.

## Reproduce

Build the static integration executable and CLI from this checkout. Build the
normal workload image as described in the integration README, then build the
additional runtime fixture image:

```sh
go run -mod=vendor ./hack/shared-etcd-image \
  --busybox=/absolute/path/to/static-linux-amd64-busybox \
  --runtime-runner=/absolute/path/to/assets/sharedetcd-tests \
  --kube-apiserver=/absolute/path/to/assets/kube-apiserver \
  --output=/absolute/path/to/assets/runtime-image.tar
```

Add these options to the existing offline VM invocation:

```sh
--test-run='^TestWorkerControllerMigrationAndRecovery$' \
--cli=/absolute/path/to/vcluster \
--runtime-image=/absolute/path/to/assets/runtime-image.tar \
--timeout=1800 --cpus=4 --memory=7G
```

The image contains only supplied static binaries. Generated credentials and data
are supplied at runtime inside the guest, never baked into the image. Raw guest
logs remain private (0600) and must not be published with source checkpoints.

## Boundaries

The runtime image and host-network/hostPath adapters are test fixtures. This is
not a qualification of the production chart/image, normal host RBAC, virtual
controller-manager/scheduler, external issuers, CSI, multiple nodes/CNIs, HA or
load/fault behavior. Fixture runtimes still use host administrator access.
Production image packaging, Helm reconciliation and RBAC remain separate gates.

See [current verified results](shared-etcd-status.md) for completed runs.

## Completed qualification

The full controller VM test passed in **1300.33 seconds**, including migration
cutover, subsequent automated recovery and both application-Pod replacements.
The native regression suite passed in **219.908 seconds**. CLI/runner build,
tagged vet, image platform/layer checks and tested-source hash verification
passed. The repository custom lint command remains unavailable (`just` and its
custom lint executable are not installed). No image or generated credentials
are published.
