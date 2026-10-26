# Journaled migration and automated recovery

These experimental commands coordinate host shutdown, certificate issuance,
etcd authorization changes, data copy, verification, configuration/TLS cutover
and restart. They operate on the same authenticated etcd cluster and preserve
the deployment configuration Secret UID. They do not clone deployments, move
application volumes, restore external services or move data across clusters.

## Supported deployment and authority

Use the rebuilt CLI and runtime containing these changes. The runtime reads
`vc-etcd-operation-<name>` before starting its API server or syncer. An active
journal blocks startup until the workflow reaches its forward-only `starting`
phase, or finishes rollback. An already-running/older binary is not stopped by
this check; observed shutdown and etcd permission revocation remain required.

The first implementation supports a named single-replica StatefulSet or
Deployment using managed K8s and external etcd. The etcd TLS files must come
from one mutable dedicated Kubernetes Secret mounted directly in the `syncer`
container. Projected/CSI/subPath mounts are refused. The configuration Secret
must contain the deployed `config.yaml`. Legacy etcd prefix/server overrides and nonzero maintenance options, root/admin
tenant identities, HA and relocation of an existing tenant root are refused.
Explicitly zero `etcd-compaction-interval` and `etcd-db-metric-poll-interval`
options are accepted for scoped legacy deployments and consumed at cutover;
tenant-root mode derives those disabled settings itself. Kubernetes underscore
aliases and separate values are validated equivalently. The original settings
remain in the journal for pre-cutover rollback. Migration translates only `/registry/` to `<root>/registry/` and
`/vcluster/mappings/` to `<root>/mappings/`.

Suspend GitOps, Helm reconciliations, autoscalers and competing administrators
for the operation. The workflow checks identity/configuration, observed zero
replicas, owned ReplicaSets and absence of control-plane Pods before copying.
It cannot atomically lock Kubernetes controllers, PKI and etcd authorization.
Etcd administrators must serialize administrative mutations. Old Kubernetes
API clients must reconnect after the control-plane restart; their watch/history
continuity across the new prefix is not preserved.

Provide an etcd administrator credential and an authorized **local client
signing CA**. The CLI generates an ECDSA client key and fresh random CN, signs
its certificate, creates a no-password etcd user/role scoped exactly to the
target root, and authenticates it before revoking old writer access. The local
issuer and administrator private keys are never uploaded. This is one issuer
implementation; cert-manager, Vault and external signing APIs are not integrated.
The replacement certificate must remain valid throughout the operation and must
be rotated before expiry during subsequent operation. Lifetime defaults to one
day, is capped at seven days and cannot exceed issuer expiry. Resume reuses the
journaled identity and credential; it does not silently mint another identity.

The original certificate CN is automatically retired. Explicitly identify any
additional source/target writers with repeated `--retire-user`. Planning refuses
unknown writers and relevant retired roles with permissions outside the managed
ranges. It revokes **user-role memberships**, never shared role definitions or
other users. Broad/multi-tenant roles must be split first. An already-connected
old client loses its key-write access; host/process/node fencing remains outside this guarantee. Lease renewal is
qualified on stock 3.6.16 as described in the
[lease authorization contract](shared-etcd-lease-authorization.md); empty leases
and TTL-only metadata retain separate limits.

Host access requires get/create/update/delete on operation Secrets and layout
ConfigMaps, get/update on the existing configuration/TLS Secrets and workload,
and list on StatefulSets, Deployments, ReplicaSets and Pods in the namespace.
The generated credential, original configuration and original TLS Secret data
are retained in the operation **Secret**. Treat it as recovery material and
restrict its RBAC. No certificate or Kubernetes object values appear in command
success output. The archive contains raw values and must be protected separately.

## Migrate an existing legacy layout

Choose a new empty, non-overlapping tenant root and a secure local archive path:

```sh
vcluster debug etcd tenant-migrate /secure/legacy-migration.gz \
  --name tenant-a --namespace tenant-a \
  --host-kubeconfig /secure/host.yaml --host-context explicit-host-context \
  --tls-secret dedicated-etcd-tls \
  --store-prefix /tenants/tenant-a \
  --admin-ca /secure/server-ca.crt \
  --admin-cert /secure/admin.crt --admin-key /secure/admin.key \
  --issuer-cert /secure/client-issuer.crt --issuer-key /secure/client-issuer.key \
  --restart-lease-ttl --commit
```

Secret item defaults are `ca.crt`, `tls.crt`, `tls.key`; override with `--ca-item`,
`--cert-item`, `--key-item`. The deployed configuration's mount paths must match
those items. Existing server trust is retained; the signing issuer may differ
from the server CA if etcd trusts it for client authentication.

Without `--commit`, the command stops after verification with the control plane
scaled to zero and its old writer revoked. With `--commit`, it proceeds through
cutover, restores the original replica count and waits for observed controller
readiness. An original zero-replica workload stays stopped. Readiness is a host
controller check; it does not prove DNS, CNI, application or volume recovery.

The host journal is created before shutdown/auth mutations. Source and empty
destination are then atomically reserved in etcd. Copy uses the existing bounded
archive/restore machinery, verifies raw values and lease grouping, and retains
its destination startup barrier until host cutover finishes. Source data remains
intact. A retained `/vcluster/migration-state` record prevents another migration
from treating the legacy source as unused after completion. Do not delete it to
reuse the source. Normal TTL expiration still applies to retained source keys.

Cutover updates the existing configuration and TLS Secret contents without
replacing their UIDs. It replaces the immutable layout marker under the host
journal barrier, using UID/resourceVersion delete preconditions. Interrupted
marker replacement is resumable. The copied data receives new etcd revisions;
the shared global revision never resets. Preserve host PKI, encryption-provider
configuration and application volumes independently.

## Automated tenant recovery

Use `tenant-recover ARCHIVE` with the same host/admin/issuer/TLS options. Omit
`--store-prefix`; the existing root, identity, endpoint and etcd cluster must
match the archive. It automates the preparation, scoped restore and credential
installation previously performed manually around `tenant-restore`. The original
`tenant-restore` command remains available for manually prepared recovery.

The full archive is validated before starting a new recovery operation. Its
digest is pinned in the journal and checked inside restore's private validated
spool before destination writes. A different archive cannot be substituted
between the coordinator's inspection and copying.

## Interruption, rollback and subsequent operations

Rerun the command with `--resume`, the same archive and lease policy. Once
cutover has started, include `--commit` to finish forward. Issuer flags are not
needed for resume because the replacement credential is journaled. Administrator
and explicit host access are still required. A timeout or lost response leaves
the operation recoverable; do not delete journals/barriers to force startup.

Before migration cutover starts, `--rollback` removes only that operation's
reserved target, releases its source reservation, restores the exact recorded
role memberships and original replica count, and retains the rollback receipt.
It checks that role definitions have not changed before reinstating access.
Configuration and TLS contents have not changed at this point. Recovery rollback
and rollback after cutover starts are refused; there is no promise that an old
copy includes writes accepted by the migrated deployment.

Lease-backed data requires `--restart-lease-ttl`: it uses fresh leases and
restarts captured remaining TTL, subject to etcd's server minimum. Source leases
are never renewed/reused/revoked. If a migration first discovers leased records
without the flag, it stays stopped; resume may explicitly enable the flag before
the archive digest is bound. Avoid pausing at `verified` beyond restored lease
lifetimes: confirmation intentionally refuses if data has expired or changed.
A pre-cutover migration can then be rolled back and started again. Resuming a copy whose final response was lost retains the conservative
data-match requirement. A completed host journal reports the recorded outcome;
it is not a fresh integrity check of subsequently changing runtime data.

After `complete` or `rolledback`, use `--new-operation` to retain an immutable
copy of the old operation Secret and start another operation. This is required
when recovering a tenant that was previously migrated. Active journals cannot
be archived this way. Completed operation Secrets are retained deliberately;
there is no automatic credential or recovery-material garbage collection.

## Verification scope

The current native integration baseline uses authenticated etcd 3.6.16 and a disposable Kubernetes
API 1.36.0. They cover automated certificate/user/role provisioning, old-connection
revocation, actual CLI migration/recovery/rollback, fixed-revision translation,
interrupted copy/resume, lease grouping, source/foreign-tenant preservation and
post-cutover ownership checks. Unit tests cover the runtime journal gate,
configuration preservation and interruption after immutable marker deletion.
Host fixture workloads start at zero replicas with seeded observed status;
there is no controller manager, worker, CNI or CSI. Full real-cluster rollout and
workload acceptance, patched Kubernetes images and the broader backup providers
remain separate required validation/work.
