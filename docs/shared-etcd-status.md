# Verified source checkpoint — 26 October 2026

This checkout resumes from published commit `498b3b08`, whose tree matches the
previous verified isolation/backup/recovery implementation. It adds journaled
same-cluster legacy migration and automated recovery preparation/cutover.
The previously reported missing migration source was not assumed to exist;
this workflow is a new implementation against the published baseline.

## Current external-etcd baseline

The isolated test server is now stock **etcd 3.6.16**, verified against upstream
release checksums. The prior 3.6.8 observations below are historical evidence,
not the current lease authorization contract. All six vendored etcd modules are
now aligned to **3.6.16**, replacing the mixed 3.6.8 server/storage and 3.6.14
client baseline. The required transitive updates are included in the vendor
tree; see [dependency provenance and deployment boundaries](shared-etcd-dependencies.md).
No existing cluster, production image or runtime server was upgraded by this
source/dependency change.

After the dependency update, the full native suite passed in **145.280 seconds**
(the preceding external-server qualification took 146.640 seconds), including
the lease checks, three-member leader transfer/loss/restart, all migration/recovery CLI
paths, both runtime restart cases and Helm upgrade checks. The authorization
tests distinguish protected nonempty leases from TTL-only metadata and empty
leases. See [the lease qualification](shared-etcd-lease-authorization.md).

## Implemented

- External-etcd parent prefixes for Kubernetes registry and mappings, scoped
  debug operations, immutable host markers and atomic ownership claims.
- Single-replica startup baseline handling and restart synchronization checks.
- Kubernetes permission-denied Status fallback as a separate source patch.
- Dedicated `tenant-backup`, `tenant-inspect` and `tenant-restore` CLI commands.
  Recovery requires a stopped host workload and independently audited writer
  revocation. It uses bounded archives, prevalidation, resumable transaction
  checkpoints, runtime barriers, content verification and completion receipts.
- Archive format 2 supports TTL-backed values and shared lease groups; format 1
  remains readable. Leased restore requires `--restart-lease-ttl`, grants fresh
  leases, renews them during copy and replays interrupted expired groups without
  repeating the destructive clear.
- `tenant-migrate` and `tenant-recover`: durable host operation Secret, automatic
  scale-down, local-CA client issuance, scoped etcd identity provisioning,
  recorded user-role revocation, archive digest pinning, identity-preserving
  configuration/TLS cutover and observed controller readiness. Migration retains
  original data, reserves source/target atomically and supports pre-cutover
  rollback. Completed journals can be archived for subsequent operations.
- Refusal of unrelated permission revocation, unknown writers, changed host
  identities/configuration/TLS/Pod templates, competing lifecycle barriers,
  mismatched archives and post-cutover rollback. Issuer/admin keys stay local.
- Rejection of Kubernetes underscore flag aliases that could bypass reserved
  etcd settings, with a reproduced failing regression before the fix.

## Verification completed

| Check | Result |
| --- | --- |
| CLI build, vendored dependencies, Go 1.27.3 | Passed |
| Dependency upgrade: static Linux amd64 CLI build; binary module inspection | Passed; all six etcd modules report v3.6.16 |
| Dependency provenance: public module checksum verification and canonical vendor comparison | Passed for all 23 upgraded module trees; other vendor dependencies preserved |
| Dependency upgrade: build/vet and nine relevant package unit suites, including snapshot conversion/restore | Passed |
| Dependency upgrade: full native suite with updated vendored CLI and stock etcd 3.6.16 | Passed in 145.280 seconds, including lease authorization, three-member failover/restart, migration/recovery, vCluster restarts and Helm upgrade |
| Go vet on `./cmd/... ./pkg/... ./e2e/...` | Passed |
| Tagged integration Go vet | Passed |
| Unit tests: config, tenantbackup, setup/config, mappings/store, synccontext, runtime config, CLI, snapshot and debug/etcd | Passed |
| Native integration regression suite, real etcd 3.6.8 and Kubernetes API 1.36.0 | All functional cases passed in the 204.338-second run; Helm launch initially failed because its downloaded binary lacked execute permission |
| Final actual CLI migration/recovery/rollback/subsequent-operation and Helm upgrade checks | Passed after correcting the test asset execute permission; tested final CLI build |
| New backend migration tests | Passed: lost copy response/resume, rollback, leases, source/foreign preservation, collateral revocation and credential-authentication ordering |
| Final changed-package unit tests | Passed: archive digest pin, invalid access plans, runtime host barrier, marker-deletion interruption and configuration preservation |
| Actual CLI backup, inspection and recovery with disposable host API | Passed; zero-replica controller status is seeded, no worker exists |
| Own-source whitespace and shell syntax | Passed; canonical vendored dependency documentation retains upstream whitespace |
| Repository custom lint via `just` | Unavailable: `just` and the custom lint binary are not installed |
| Scheduled-Pod/CNI VM suite | Skipped; no complete result claimed |

The current and preceding native runs used the stock verified envtest API server, with the optional
patched-Status assertion disabled. The separate Kubernetes patch was retained
but was **not rebuilt or revalidated in this session**. Native tests prove real
process/API behavior, not a Helm-managed workload rollout on a worker cluster.

## Required next work

1. Real-cluster acceptance for the new migration/recovery workflows, including
   a controller-managed scale-down/restart, actual Helm values reconciliation,
   normal operator/runtime RBAC, Pods, CNI/DNS, volumes and application recovery.
2. Build/distribute the matching patched Kubernetes/vCluster images and verify
   the denied-Status fallback in those images.
3. External issuer integrations and ongoing certificate renewal; the automated
   operation currently supports an authorized local signing CA and direct
   dedicated Secret mounts. HA and broad/multi-tenant writer grants are refused.
4. Volume/application/external-resource backup providers and protected recovery
   bundles for host configuration, PKI and encryption-provider dependencies.
5. HA, cross-cluster recovery, cloning, arbitrary root relocation, retained-root
   decommission, failure/load testing and broader version/storage/network
   acceptance.

The [journaled operations procedure](shared-etcd-operations.md) defines the
supported shutdown, authorization, lease, cutover, resume and rollback contract.
Trusted administrators/reconcilers must serialize changes; the workflow is not
an atomic transaction spanning Kubernetes, PKI and etcd auth. Once cutover starts,
completion is forward-only. Native API fixtures have no host controller or worker.

Leased recovery explicitly restarts captured remaining TTL, subject to the etcd
minimum duration, rather than preserving the old wall-clock deadline. It is
bounded to 1,024 groups and seven days of captured remaining TTL. The old 3.6.8 renewal bypass is rejected on the qualified 3.6.16 server.
TTL-only metadata and permanent empty-lease ownership remain separate boundaries;
there is no claim of complete tenant isolation for every Lease API object.
Archives also exclude volume contents, host
resources and encryption/PKI keys.
See the [recovery procedure](shared-etcd-recovery.md) for the exact supported
contract and required operator steps.

The first focused new-workflow integration run passed in 40.825 seconds. The
final CLI/Helm rerun verified the final build, including Pod-template/TLS drift
guards. All tests used disposable processes and generated identities. No existing
cluster was changed and no image was published. The source checkpoint is
published through the browser workflow described below.

## GitHub checkpoint target

The user authorized syncing the fork `imanlotfimahyari/vcluster` to the tested
upstream revision and publishing tested checkpoints on branch
`feat/shared-etcd-store-prefix`. The fork was 177 commits behind, without
divergent commits. An initial fast-forward request reported an integration
permission error, but a later authoritative read verified the fork's `main` at
`e702d7ba743bd03d9c572cc0d3807def27779317`, matching the tested upstream revision.
The connected GitHub API continued to reject writes with `Resource not accessible
by integration` (403). With the user's approval, the signed-in browser
successfully created `feat/shared-etcd-store-prefix` and published the source in
directory-sized commits. Its implementation corresponds to tested local commits
`6643b194` and `92465b0e`, followed by publication instructions and script-mode
packaging changes. No Go implementation changed during publication.

Web uploads do not retain executable bits. Both harness entry points are now
ordinary files, invoked explicitly with Bash/Python; the README reflects this.
Shell syntax and whitespace checks accompany the packaging adjustment. Browser
commit IDs differ from local IDs, so verification fetches the public branch and
compares the complete Git trees, including file contents, paths and modes.
The ongoing checkpoint workflow is recorded in `AGENTS.md`.
