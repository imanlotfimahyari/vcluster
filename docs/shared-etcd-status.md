# Verified source checkpoint — 25 October 2026

This file records what is present in this checkout and was checked in this
session. Earlier completion descriptions for migration, automatic fencing and
image packaging could not be recovered as source files and are not part of this
checkpoint.

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
- Rejection of Kubernetes underscore flag aliases that could bypass reserved
  etcd settings, with a reproduced failing regression before the fix.

## Verification completed

| Check | Result |
| --- | --- |
| CLI build, vendored dependencies, Go 1.27.3 | Passed |
| Go vet on `./cmd/... ./pkg/... ./e2e/...` | Passed |
| Tagged integration Go vet | Passed |
| Unit tests: config, tenantbackup, setup/config, mappings/store, synccontext, runtime config, CLI, snapshot and debug/etcd | Passed |
| Full native integration suite, real etcd 3.6.8, Kubernetes API 1.36.0 and Helm 3.22.0 | Passed in 74.620 seconds, including leased CLI recovery |
| Focused leased recovery tests | Passed in 9.597 seconds |
| Actual CLI backup, inspection and recovery with disposable host API | Passed; zero-replica controller status is seeded, no worker exists |
| Formatting, shell syntax and `git diff --check` | Passed |
| Repository custom lint via `just` | Unavailable: `just` and the custom lint binary are not installed |
| Scheduled-Pod/CNI VM suite | Skipped; no complete result claimed |

The full native run used the stock verified envtest API server, with the optional
patched-Status assertion disabled. The separate Kubernetes patch was retained
but was **not rebuilt or revalidated in this session**. Native tests prove real
process/API behavior, not a Helm-managed workload rollout on a worker cluster.

## Required next work

1. Legacy-to-tenant migration using the verified archive/recovery foundation,
   including a host cutover journal, identity preservation and rollback tests.
   No migration or clone command currently exists.
2. Automated writer provisioning/rotation if desired. Current recovery checks
   existing permissions but deliberately does not change etcd users or roles.
3. Build and distribute the matching patched Kubernetes/vCluster images; then
   validate two real Helm deployments, normal host RBAC, Pods, CNI/DNS, upgrades
   and workload recovery on the operator's test cluster.
4. HA, cross-cluster recovery, cloning, retained-root decommission, failure/load
   testing and broader version/storage/network acceptance.

Leased recovery explicitly restarts captured remaining TTL, subject to the etcd
minimum duration, rather than preserving the old wall-clock deadline. It is
bounded to 1,024 groups and seven days of captured remaining TTL. Stock etcd
keep-alive calls are not key-range authorized: revoking key-write grants does
not prevent renewal of a known lease ID. Fresh restore IDs do not solve that
shared-service isolation limit. Archives also exclude volume contents, host
resources and encryption/PKI keys.
See the [recovery procedure](shared-etcd-recovery.md) for the exact supported
contract and required operator steps.

All tests used disposable processes and generated identities. No existing
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
