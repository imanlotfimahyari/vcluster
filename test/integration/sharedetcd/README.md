# Shared etcd integration tests

Opt-in Linux tests use real etcd, Kubernetes APIs, and vCluster runtime
processes with disposable data and generated credentials. They never read the
caller's kubeconfig or contact an existing cluster. Telemetry is disabled.

## Native process suite

Provide verified `etcd` **3.6.16**, `kube-apiserver`, and `helm` executables.
The harness and fixtures reject a different etcd version:

```sh
export VCLUSTER_SHARED_ETCD_ASSETS=/absolute/path/to/test-assets
# Set this when using the rebuilt API server described in contrib/kubernetes.
export VCLUSTER_SHARED_ETCD_REQUIRE_STATUS_FALLBACK=1
bash hack/test-shared-etcd.sh
```

The script builds the CLI, vets the integration packages, and runs the suite
without the Go result cache. The `integration && linux` build constraint excludes
it from ordinary package tests. Helm coverage is skipped by a direct `go test`
invocation if Helm is absent; the script requires it to avoid that gap.

| Test | Coverage |
| --- | --- |
| `TestRealEtcdTenantIsolation` | Authenticated etcd; tenant mTLS identities restricted to individual root ranges; real mappings CRUD/watch/reconnect/clear; bidirectional denial of foreign reads/writes/deletes/root scans/watches |
| `TestVClustersShareAuthenticatedEtcd` | Two actual vCluster runtimes; identical ConfigMap names; host synchronization; restart; immediate update; deletion; real scoped debug handlers |
| `TestWritesDuringControllerStartup` | Same immediate-update regression path; always enabled, with no synchronization probe between proxy readiness and update |
| `TestTenantRootCannotBeClaimedByAnotherDeployment` | Real durable layout marker/claim; idempotent restart; another deployment UID denied; unclaimed existing data rejected |
| `TestHelmUpgradePreservesStorageLayout` | Actual Helm install and same-layout upgrade; server-side dry-run rejects changed/removed roots; retained marker unchanged |
| `TestTenantBackupFixedRevisionAndValidation` | Paginated, fixed-revision backup during concurrent writes; private file and no-overwrite publication; active-recovery refusal |
| `TestTenantRecoveryFencingResumeAndIsolation` | Existing-client revocation; ownership validation; lost copy response and resume; startup barrier; wrong-archive and revived-writer refusal; other tenant progress and raw-data comparison |
| `TestTenantRecoveryAuthAuditRefusesUnsafeGrants` | Foreign recovery grants, retired administrator, unknown writer and disabled auth rejected |
| `TestTenantRecoveryLostCompletionResponse` | Lost completion response confirmed through receipt and current-data verification, without another destructive restore |
| `TestTenantArchiveCLIWithRealHostAPI` | Built CLI backup/inspection/restore with real etcd and disposable host API; synthetic observed zero-replica Deployment |
| `TestTenantLeasedBackup` | Lease-group metadata, explicit TTL-policy gate, fresh IDs, lost copy response, expired lease recreation, replay without another range clear |
| `TestRestoredTenantLeaseExpiresAfterSuccess` | Restored leased values actually expire while persistent values remain |
| `TestTenantBackupRefusesForeignSharedLease` | Attached-key authorization refuses a lease shared across tenant roots |
| `TestLeaseAuthorizationBoundaries` | Own/foreign/read-only renewal; TTL metadata versus attached keys; global listing, revoke and Put/nested-Txn lease attachment |
| `TestLeaseRevocationOnExistingStream` | Revocation on the same already-open bidi stream, new-request denial and explicit regrant |
| `TestLeaseAuthorizationDuringPermissionChanges` | Concurrent renewal requests overlapping permission removal and regrant |
| `TestEmptyLeaseHasNoCreatorOwnership` | Explicit upstream empty-lease semantics without claiming persistent tenant ownership |
| `TestLeaseAuthorizationAcrossLeaderChangesAndRestarts` | Three mTLS peers; leader/follower renewal, leader transfer/loss, stream revocation and full-cluster restart |

Repeat the startup regression:

```sh
go test -mod=vendor -tags=integration -timeout=4m -count=3 \
  -run '^TestWritesDuringControllerStartup$' -v ./test/integration/sharedetcd
```

Earlier runs exercised the patched Kubernetes fallback and repeated the startup
regression. The latest status and current image/worker validation limits are
recorded in [the checkpoint status](../../../docs/shared-etcd-status.md).

## Offline scheduled-Pod/CNI suite

`TestScheduledPodsAndCNI` runs only in the disposable VM. It boots an actual
single-node K3s host with containerd and Flannel host-gw, starts two tenant
runtimes against restricted identities on shared etcd, creates identically named
server/client Pods, waits for real scheduling/status, checks Pod-IP and Service
HTTP traffic, restarts one control plane, and replaces a Pod while checking the
other tenant remains Ready.

Prepare a static test binary and an offline BusyBox workload image:

```sh
CGO_ENABLED=0 go test -mod=vendor -tags=integration -c \
  -o /absolute/path/to/assets/sharedetcd-tests ./test/integration/sharedetcd
go run -mod=vendor ./hack/shared-etcd-image \
  --busybox=/absolute/path/to/static-busybox \
  --output=/absolute/path/to/assets/workload-image.tar
python hack/run-shared-etcd-vm.py \
  --qemu-root=/absolute/path/to/qemu-package-root \
  --kernel=/absolute/path/to/vmlinuz \
  --busybox=/absolute/path/to/static-busybox \
  --modules=/absolute/path/to/lib/modules/kernel-version \
  --assets=/absolute/path/to/assets \
  --runner=/absolute/path/to/assets/sharedetcd-tests \
  --log=/absolute/path/to/disposable-test.log
```

The QEMU package root supplies `usr/bin/qemu-system-x86_64`, its libraries,
`usr/share/qemu`, and SeaBIOS. Host prerequisites are Python 3, libzstd, and
`mkfs.ext4`. The runner uses TCG, no host network, no host filesystem/socket
shares, an ephemeral ext4 disk, and explicitly selected test assets/modules.
Its default RAM allocation is 5 GiB. The guest must support cgroup v2, overlayfs,
virtio block, ext4, veth/bridge and required netfilter modules. Assets also include
`k3s` and `k3s-images.tar.zst` from the same release. Use the patched API server as
`assets/kube-apiserver` to exercise the Status fallback.

Execution checkpoint: the initial guest exposed two fixture requirements: an
ext4 root for kubelet filesystem inspection and nftables kernel modules for
kube-proxy. Both are implemented in the runner. The latest run reached a Ready
Kubernetes worker and started the authenticated-etcd phase, but a complete
Pod/CNI test pass was not obtained. Scheduled-Pod, traffic, and replacement
assertions therefore remain unverified; they must pass before this checkpoint
can be considered covered.

## Verified assets

| Asset | Official source | Download SHA-256 |
| --- | --- | --- |
| etcd 3.6.16 Linux amd64 archive | [etcd release](https://github.com/etcd-io/etcd/releases/tag/v3.6.16) | `c8c0d96d8292ef085deabbf735e9561d3d396768e4d606df005aa25964f21911` |
| envtest 1.36.0 Linux amd64 archive | [controller-tools release](https://github.com/kubernetes-sigs/controller-tools/releases/tag/envtest-v1.36.0) | `d84f910bcefa3f6ab0205a49a7255672150c73b14bca3c36ac627db65040edf0` |
| Helm 3.22.0 Linux amd64 archive | [Helm distribution](https://get.helm.sh/helm-v3.22.0-linux-amd64.tar.gz) | `1e4ab49e429626cf6c6958d914248b78c9730803c2751b87627e171dc800e7bb` |
| K3s v1.36.0+k3s1 Linux amd64 executable | [K3s release](https://github.com/k3s-io/k3s/releases/tag/v1.36.0%2Bk3s1) | `efa953454c8f7bf8d9d35c2dad2a76c9bdaac6860d60361629a3a2719009876b` |
| Matching K3s airgap image archive | Same release, `k3s-airgap-images-amd64.tar.zst` | `1d98b5f32c85ae41371851e6c14b81bdb8960177b28ccd1deda55c1a05586704` |

The patched API server is built from the exact Kubernetes commit documented in
[contrib/kubernetes](../../../contrib/kubernetes/README.md); it is not the stock
envtest executable. Certificates use independent etcd, host Kubernetes, tenant
Kubernetes and request-header CAs. Keys are temporary and mode 0600. Cleanup
terminates subprocess groups, including child API servers.

## Adaptations and limits

- Runtime paths/ports are remapped per process. A plugin adapter delegates to
  actual v1/v2 managers, changing only the legacy listener to a dynamic port.
- Virtual controller managers, schedulers, and CoreDNS are disabled. Fixtures
  seed default ServiceAccounts. In the VM, the host scheduler schedules the
  translated Pods; virtual DNS is not exercised.
- The native host API has no worker/controller manager. Helm tests exercise
  live release/configuration changes, not an actual chart-managed Pod rollout.
- The VM uses one node and one CNI configuration. It does not cover cross-node
  routing, other CNIs, NetworkPolicy, PVC/CSI, drains, or rolling upgrades.
- Fixture syncers have host administrator credentials. Production host RBAC,
  adversarial process isolation, and credential rotation are not validated.
- HA leader handoff, mutations made while syncers are offline, performance,
  etcd quorum/compaction/quota failures, and the supported-version matrix need
  separate acceptance tests.
- Built-in global snapshot/restore remains blocked. Dedicated tenant backup,
  inspection and same-identity recovery are available as described in the
  [recovery procedure](../../../docs/shared-etcd-recovery.md). Journaled
  legacy migration and automated recovery are described in the
  [operations procedure](../../../docs/shared-etcd-operations.md). Arbitrary
  adoption/relocation and older binary/chart rollback remain unsupported.

The harness script now builds the CLI and supplies `VCLUSTER_SHARED_ETCD_CLI` to
exercise the real commands. Direct `go test` skips that CLI test unless the
variable names an executable. To run only the recovery tests, provide both the
CLI and assets variables and select
`-run '^(TestTenant(Backup|Recovery|Archive|Leased)|TestRestoredTenantLease)'`.

The stock 3.6.16 suite rejects foreign/read-only renewal and revoked writers on
existing streams. It covers leader/follower endpoints, leader transfer/loss,
full-cluster restart, TTL/list/revoke/Put/nested transactions and overlapping
auth changes. TTL-only metadata and empty-lease creator ownership remain explicit
limits. See the [exact qualification](../../../docs/shared-etcd-lease-authorization.md).
The external test executable is upgraded; vendored libraries remain separate.

The journaled-operation tests also exercise actual CLI migration, automated
recovery, pre-cutover rollback, subsequent operations and credential rotation
with a real disposable host API. Backend tests cover interrupted migration,
legacy lease translation, preservation of source/foreign data and refusal of
collateral permission revocation. The host workloads remain zero-replica
fixtures; these tests do not substitute for worker/CNI/CSI acceptance.
