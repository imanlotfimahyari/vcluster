# External etcd tenant storage prefixes

This contribution supports **new deployments** using the managed K8s API server.
A parent `storePrefix` derives both the Kubernetes registry and vCluster mappings
ranges. A mappings-only prefix would leave Kubernetes objects colliding; the
parent root covers both without allowing independently inconsistent settings.
Tenant lifecycle features and the validation limits below still prevent a claim
of complete production support.

## Configuration

```yaml
controlPlane:
  backingStore:
    etcd:
      external:
        enabled: true
        endpoint: etcd.example.com:2379
        storePrefix: /tenants/tenant-id
        tls:
          caFile: /etc/etcd/ca.crt
          certFile: /etc/etcd/client.crt
          keyFile: /etc/etcd/client.key
```

Mount operator-provided certificates. Allocate a persistent tenant ID and etcd
identity per deployment; a release name alone is not globally unique.

| Setting | Kubernetes objects | Mappings | Ownership claim |
| --- | --- | --- | --- |
| Empty | Existing behavior (`/registry` by default) | `/vcluster/mappings/` | None |
| `/tenants/tenant-id` | `/tenants/tenant-id/registry/...` | `/tenants/tenant-id/mappings/...` | `/tenants/tenant-id/vcluster/storage-layout` |

One trailing slash is accepted. Relative/root paths, whitespace/control
characters, backslashes, noncanonical paths, and overlap with `/registry/` or
`/vcluster/` are rejected. The mappings prefix is an instance field, including
watch reconstruction and debug add/list/clear. In tenant mode, `debug etcd keys`
defaults to the tenant root and rejects ranges outside it.

Custom API-server commands and overrides of `--etcd-prefix`,
`--etcd-compaction-interval`, `--etcd-db-metric-poll-interval`, `--etcd-servers`,
and `--etcd-servers-overrides` are rejected. The managed server receives the
derived registry prefix and disables its compaction and database-size polling.
Central maintenance and metrics collection remain the etcd operator's job.

## Startup and upgrade protection

For tenant mode, the bidirectional object caches retain deep-copied initial
informer snapshots. Later informer events cannot advance an unsynchronized
baseline. In a single-replica deployment, the proxy starts after syncers have
registered and these initial events have synchronized. Tests update an existing
ConfigMap immediately after proxy readiness following a full process restart;
there is no extra newly-created-object readiness barrier.

Startup persists an immutable host ConfigMap named
`vc-storage-layout-<release>`, recording the layout version, normalized root,
endpoint, and configuration Secret UID. It has no garbage-collecting owner
reference and is deliberately separate from Helm-owned resources. Legacy
startups also record their empty root so a later direct edit cannot silently
switch their storage layout.

Tenant startup atomically claims an empty root in etcd. A matching claim permits
restarts/replicas; another deployment identity or existing unclaimed data causes
startup to fail before API-server writes. CLI validation, Helm's live lookup,
and runtime validation reject layout changes. An optional trailing slash is
layout-equivalent. Endpoint changes and replacement deployment identities also
require explicit recovery or migration.

The marker survives uninstall. Reinstalling under a replacement Secret UID is
intentionally blocked; operators must reconcile the retained data and identity
through an offline recovery/decommission procedure. The [journaled migration command](shared-etcd-operations.md) supports the
same-cluster legacy layout; arbitrary adoption and relocation remain unsupported. The claim catches exact root reuse and existing
nested data; it is **not** a global allocator and cannot exclude every future
nested-root collision. A trusted provisioner must allocate disjoint roots.
The markers are configuration safeguards, not protection against an etcd/host
administrator or a tenant identity deliberately rewriting its own root.

## Kubernetes Status fallback

Stock Kubernetes 1.36 retries a privileged etcd Status request during
RequestWatchProgress feature detection. Disabling size polling does not affect
that request. The supplied [Kubernetes patch](../contrib/kubernetes/README.md)
treats PermissionDenied as a terminal capability decision, disables the
optimization, and retains retries for transient connection failures. The
rebuilt API server passes the restricted-identity integration test without
repeated Status requests. One initial denied capability probe remains expected.

This fix belongs to the **external Kubernetes binary**. Production use requires
a matching rebuilt distro image and version-specific validation; the vCluster
Go changes alone do not fix stock images. No image or upstream PR was published.

## Verification scope

The [integration harness](../test/integration/sharedetcd/README.md) covers:

- Real authenticated etcd, separate tenant certificate identities, identical
  names, CRUD/watch/reconnect/clear, and foreign read/write/delete/watch denial.
- Two real vCluster processes and host ConfigMap synchronization, full restart,
  immediate post-readiness update, deletion, and scoped debug operations.
- Durable ownership with real Kubernetes/etcd, rejecting duplicate deployments
  and nonempty unclaimed keyspaces.
- Actual Helm install and same-layout upgrade against a disposable API server;
  server-side render rejection for changed/removed roots. This does not exercise
  a Helm-managed Pod rollout or old-version rollback.
- A separate offline VM test for a real Kubernetes worker, scheduled tenant
  Pods, Pod-IP/Service traffic, and control-plane restart with Pod replacement.
  Its execution result and exact limitations are recorded in the harness README.

Build, package tests, full Go vet, tagged integration vet, generated-schema
verification, formatting, and whitespace checks accompany these tests. The
repository's custom lint requires `just` and its golangci-lint binary, which are
not installed in this environment. Base revision:
`e702d7ba743bd03d9c572cc0d3807def27779317`; local branch:
`feat/shared-etcd-store-prefix`. No existing cluster was changed.

## Remaining work beyond those concerns

| Area | What remains | Can the solution cover it? |
| --- | --- | --- |
| Tenant backup/restore and cloning | Dedicated backup/inspection and same-identity recovery are implemented; cloning and identity transformations remain | [Recovery workflow](shared-etcd-recovery.md): one revision, bounded archives, complete validation, stopped workloads, audited writer revocation, resumable checkpoints, content verification. Built-in snapshot/restore stays blocked. |
| Existing-data migration and rollback | Same-cluster legacy migration, resumable copying, guarded cutover and pre-cutover rollback are implemented. Older binary/chart rollback and arbitrary relocation remain unsupported. | [Journaled operations](shared-etcd-operations.md); real-cluster/version acceptance remains required. |
| HA and offline edits | Leader handoff with multiple replicas; conflict semantics when both host and virtual objects change while the syncer is down | Requires additional design and fault tests. Baselines are in memory, not a persisted history of delivered changes. |
| Root allocation and credentials | Globally disjoint roots, least-privilege provisioning, rotation/revocation, retained-root decommission, host RBAC | Yes, through trusted operator/provisioner workflows. Exact-root claims do not replace them. |
| Broader cluster validation | Multi-node networking, other CNIs, virtual DNS, NetworkPolicy, PVC/CSI, drains, rolling upgrades, all resource kinds and supported Kubernetes versions | Testable, but outside the current harness coverage. |
| Resilience and operations | etcd quorum loss/recovery, compaction/watch recovery, quota alarms, load/noisy-neighbor behavior, observability, runbooks | Can be tested and mitigated; requires an operational acceptance suite. |
| Shared failure domain | Quorum, disks, global revisions/history, capacity, maintenance and etcd outages | Inherent in one shared etcd cluster. Prefixes cannot provide independent failure or capacity isolation. Separate clusters are needed for that guarantee. |
| Delivery | Upstream review, CI integration, supported distro image carrying the Kubernetes patch, published release | Required before production distribution. |
