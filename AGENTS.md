# Shared etcd implementation checkpoints

Follow `CLAUDE.md` and its referenced verification workflow.

The user has authorized publishing each tested implementation checkpoint to
`imanlotfimahyari/vcluster`, branch `feat/shared-etcd-store-prefix`. Keep the
fork's `main` aligned with the exact upstream revision used for implementation
and tests when it can be fast-forwarded safely. Do not force-push divergent work.

After a meaningful implementation step, complete the relevant checks, update
`docs/shared-etcd-status.md`, commit locally, then publish the checkpoint and
verify the remote head/tree. Preserve concurrent remote commits. Do not publish
credentials or test artifacts containing generated private keys.

The fork's `main` was verified at the tested upstream revision
`e702d7ba743bd03d9c572cc0d3807def27779317`. GitHub API writes still return HTTP
403 `Resource not accessible by integration`, and shell Git has no push
credentials. The user approved publishing through the signed-in GitHub browser;
that route successfully created the feature branch and uploaded source files.
Use that approved fallback when needed, preserving paths and verifying the
remote Git tree after upload. Browser commits have different IDs from the local
implementation commits; exact tree equality verifies the source checkpoint.

The two harness entry points are stored as ordinary files for browser-upload
compatibility. Invoke `bash hack/test-shared-etcd.sh` and
`python hack/run-shared-etcd-vm.py` explicitly. Preserve local commits and
downloadable source checkpoints as additional copies.
