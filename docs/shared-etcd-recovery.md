# Tenant-scoped backup and offline recovery

This experimental workflow restores an existing tenant on the **same external
etcd cluster**, with the same root, configuration Secret UID, layout marker and
endpoint. It supports the managed Kubernetes API server and the two data ranges
`<root>/registry/` and `<root>/mappings/`. Cloning, replacement deployment identities and cross-cluster recovery remain
unsupported. For legacy migration and automated shutdown, credential issuance,
writer retirement and cutover, see [journaled operations](shared-etcd-operations.md).
The commands below describe the manually prepared restore path.

## Backup and inspection

Run the rebuilt CLI with the deployed configuration and tenant credentials:

```sh
vcluster debug etcd tenant-backup /secure/tenant-backup.gz \
  --config /secure/deployed-values.yaml --name tenant-a

vcluster debug etcd tenant-inspect /secure/tenant-backup.gz
```

The backup is a logical export of both ranges at a single etcd MVCC revision.
Ownership is read at that revision too. Concurrent tenant writes may continue;
compaction that removes the chosen revision makes the export fail rather than
mixing revisions. No cluster-wide snapshot, maintenance, status or compaction
permission is needed. The file is gzip JSONL, published
atomically with mode 0600, and cannot overwrite an existing file. New backups
use format version 2; version 1 archives remain readable and restorable.

Inspection validates the entire file, including canonical JSON, strict key
ordering, duplicate keys, allowed ranges, entry and size limits, footer SHA-256,
gzip checksum, truncation and trailing streams. Its JSON output contains
metadata and counts, not object values. SHA-256 detects corruption; it does not
authenticate an archive supplied by an adversary. Protect archive provenance.

An archive includes raw Kubernetes and mapping values, which can contain
Secrets. Store it securely. It is not encrypted by this command. It does not
back up host resources, persistent volumes, certificates, configuration, or
Kubernetes encryption-provider keys. Retain those separately. Unknown data
ranges are rejected. Leased keys include their shared lease identity and a
sample of its remaining TTL, as described below.

Bounds: 1,000,000 records, 4 GiB decompressed, 1,400 KiB per value, 8 KiB per full
key, and 2 MiB per encoded record. Large exports can fail earlier if an
operator-provided etcd client or server imposes a smaller response limit.

## TTL-backed data and lease policy

Version 2 supports leased values, including Kubernetes TTL-backed events. The
key/value data is read at the fixed MVCC revision; remaining TTL is sampled
afterward through a separate lease RPC, once per shared lease group. TTL is not
historical MVCC data. An expired/unreadable lease causes the export to fail.
The attached-keys request makes etcd authorize every key on the lease, and the
export also rejects attached keys outside the tenant root, even with an
administrator client. No global lease enumeration is performed.

Restoring such an archive requires the explicit **`--restart-lease-ttl`** option.
It recreates each shared group with a **fresh lease ID** and restarts the captured
remaining TTL, subject to etcd's minimum grant duration. Values that have expired
since backup can therefore reappear for the restored lifetime. This is a
point-in-time recovery policy, not preservation of the original wall-clock
expiry deadline. Without that option, restore refuses before any destination
mutation rather than silently making leased values permanent.

Fresh leases are renewed while the command runs so that a slow copy can finish.
They are refreshed before completion and renewal stops when the invocation
returns. The remaining lifetime after return depends on the heartbeat interval
and server minimum. Shared grouping is preserved; source leases are never
reused, renewed or revoked by recovery. The checkpoint records fresh IDs.
If an interrupted attempt's leases expired, resume creates replacements and
replays the validated copy without repeating the destructive range clear.
Empty grants from failed/unknown operations expire naturally.

Bounds are 1,024 shared lease groups and a captured remaining TTL of at most
seven days. After a successful restore, natural TTL expiry changes the data;
completion confirmation may then report a data mismatch instead of confirming
an exact archive match. It does not start another restore or mutate data.

**Shared-etcd limit:** stock etcd lease keep-alive is not authorized by key
ranges. Tests confirm that a retired identity with no tenant key-write grants
can still renew a known lease ID. Key-write revocation therefore does not
provide an adversarial expiry-time fence. Fresh restore IDs avoid reusing old
IDs but do not make lease APIs tenant-isolated. Stronger isolation requires an
additional lease-aware authorization boundary or separate etcd clusters.

## Recovery prerequisites

Use one operator at a time. The CLI checks prerequisites again before every
write transaction, but etcd auth changes and KV transactions are separate APIs.
Trusted administrators must serialize permission and data changes throughout
recovery. Administrative access remains capable of bypassing tenant safeguards.

1. Retain the original deployment configuration and its public client
   certificate. The certificate identifies the writer that must be retired;
   its original key does not need to remain usable. The retained config must
   still reference that original certificate during the restore command.
2. Stop the host StatefulSet **or** Deployment by scaling it to zero. Wait for
   the controller to observe the change and for every control-plane Pod to
   disappear, including terminating and orphaned Pods. Prevent GitOps or another
   operator from scaling it back up during recovery.
3. Issue a separate, fresh recovery client certificate through the operator's
   PKI. Do not mount this credential into any running Pod before recovery
   completes. Provision its etcd user with read/write access to exactly the
   tenant root, with no foreign read or write permissions.
4. Revoke all grants that allow the original or any other nonadministrator
   identity to write any part of this tenant root. Keep disabled user records
   so their retirement can be audited. The recovery user must be the only
   nonadministrator writer intersecting the root. Another tenant's disjoint
   grants can remain active.
5. Keep the etcd administrator credential outside tenant Pods. This command
   uses it only for the read-only auth audit, not to mutate users or grants.

For example, after the operator has issued the recovery certificate, etcd's
normal administrative commands can provision a scoped identity and retire the
old writer. Replace the example user/role names with the actual deployed ones:

```sh
etcdctl user add recovery-a --no-password
etcdctl role add recovery-a
etcdctl role grant-permission recovery-a readwrite /tenants/tenant-a/ --prefix
etcdctl user grant-role recovery-a recovery-a
etcdctl user revoke-role tenant-a tenant-a
```

Supply explicit etcd administrator endpoint/TLS settings to those commands.
The restore command does not assume this example grant is sufficient: it lists
all users and roles and rejects any remaining writer, broad recovery grant,
disabled auth, or retired administrator. Revocation is enforced by etcd against
already-connected old clients, as exercised in the integration tests. It is
not host/process/node fencing; manage those systems separately.

## Execute and resume

```sh
vcluster debug etcd tenant-restore /secure/tenant-backup.gz \
  --config /secure/deployed-values.yaml --name tenant-a \
  --namespace tenant-host-namespace \
  --host-kubeconfig /secure/host-kubeconfig --host-context explicit-host-context \
  --admin-ca /secure/etcd-ca.crt \
  --admin-cert /secure/etcd-admin.crt --admin-key /secure/etcd-admin.key \
  --recovery-ca /secure/etcd-ca.crt \
  --recovery-cert /secure/recovery-a.crt --recovery-key /secure/recovery-a.key
```

For an archive containing leased entries, add `--restart-lease-ttl` to this
command and to any subsequent `--resume` invocation after reviewing the policy
above. `tenant-inspect` reports `leasedEntries` and `leaseGroups`.

The host identity and retained immutable layout marker must match the archive.
The CLI requires exactly one named StatefulSet or Deployment, observed zero
replicas, zero owned ReplicaSets and no control-plane Pods. Host access needs
`get` on the configuration Secret and layout ConfigMap and `list` on
StatefulSets, Deployments, ReplicaSets and Pods in the selected namespace.
This workflow does not scale resources or modify Secret data automatically.

Before any destination mutation, restore verifies the complete archive into a
private temporary spool. Replacing the input file later cannot change the
validated data used by that invocation. It then verifies the existing etcd
claim, same-cluster ID, root, endpoint and deployment UID.

A persistent `<root>/vcluster/restore-state` checkpoint blocks runtime startup.
The initial clear deletes only registry and mapping ranges. Copy batches contain
at most 32 records, bounded below the normal etcd request size. Every transaction
compares the ownership claim, checkpoint revision and absence of competing
recovery/migration barriers. Restored values receive new etcd revisions; the
shared global revision is never reset. The restore does not recreate original
per-key MVCC history. Leases follow the explicit restart policy above.

On interruption or an unknown transaction outcome, leave the tenant stopped
and rerun the **identical command with `--resume`**. A different archive,
ownership or recovery identity is rejected. Do not delete the checkpoint to
force startup. Resume continues after the committed batch; it does not clear
already-restored data again. A full data digest is checked before completion.
An atomic completion receipt allows `--resume` to confirm a completed restore
after its final response was lost, without starting another destructive copy.
Confirmation also checks that current data still matches the archive.

After successful recovery, distribute the recovery certificate/key to the
deployment's existing TLS mount, preserve the deployment configuration Secret
UID and root, and restart the control plane. The old identity stays revoked.
Do not replace the configuration Secret merely to update its contents.
Verify Kubernetes access, host synchronization and workloads before accepting
recovery. Those real-workload checks remain an operator acceptance step.

## Validation and limits

The tests exercise real authenticated etcd, paginated fixed-revision backup,
active writer refusal, permission revocation on an existing connection, partial
copy/resume, lost commit and completion responses, content comparison, startup
barriers, and continued activity by a different tenant. An actual CLI test uses
etcd and a disposable host Kubernetes API. Its zero-replica Deployment status is
seeded because this process fixture has no controller manager or worker.

Unit tests cover malformed/corrupt archives and fake-host checks for both chart
workload kinds, active ReplicaSets, terminating/orphaned Pods, ambiguous
controllers, changed identity and changed layout metadata. They do not prove a
real chart-managed workload recovery, network isolation, CSI data recovery, HA,
or migration cutover. Built-in global snapshot/restore remains blocked in tenant
mode; only these dedicated commands are scoped to the tenant.

Lease tests cover shared grouping, persistent values staying unleased, explicit
policy refusal, lost copy response, expiry during interruption, replay without
another range clear, actual expiry after successful recovery, and rejection of
a lease shared across tenant roots. The real CLI/API test also restores leased
data and checks the policy flag.
