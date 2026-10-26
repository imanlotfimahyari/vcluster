# etcd dependency baseline

The implementation pins all six etcd modules to **v3.6.16** in `go.mod`,
`go.sum` and `vendor/`: API, client/pkg, client, etcdutl, pkg and server. The
previous checkout combined v3.6.8 server/storage modules with v3.6.14 clients.
This update carries the upstream code into vendored CLI/runtime builds; the
external-process qualification alone did not update these dependencies.

The release also requires updates to compression, OpenTelemetry, generated
Google APIs and `golang.org/x/{crypto,net,sync,sys,term,text,tools}`. Existing
higher versions of other dependencies are retained. bbolt and Raft remain at
their existing versions, v1.4.3 and v3.6.0 respectively.

## Source and regeneration

The six public module downloads resolve to upstream commit
`0b065e61c679ea81f6862bb24c9815f7fc673966`. Go verified their module checksums;
the new checksums are recorded in `go.sum`. This is upstream release code, with
no private TTL-inspection candidate applied.

Full-repository `go get` cannot resolve the private
`github.com/loft-sh/e2e-framework` dependency in this environment. The update was
generated with Go's normal module/vendor tools in an isolated public module,
using the existing vendored package inventory and retaining existing dependency
version minimums. Only the 23 upgraded modules and their manifest/checksum
entries were replaced. The generated result introduced no packages or modules
in unchanged dependencies; their existing vendor files were preserved.

Maintainers with private-module access should use the regular full-repository
regeneration and review its diff:

```sh
go get go.etcd.io/etcd/api/v3@v3.6.16 \
  go.etcd.io/etcd/client/pkg/v3@v3.6.16 \
  go.etcd.io/etcd/client/v3@v3.6.16 \
  go.etcd.io/etcd/etcdutl/v3@v3.6.16 \
  go.etcd.io/etcd/pkg/v3@v3.6.16 \
  go.etcd.io/etcd/server/v3@v3.6.16
go mod vendor
```

Builds and checks in this environment use `-mod=vendor`. The verification
results are recorded in [the current status](shared-etcd-status.md).

## Deployment boundary

A library update does **not** upgrade an operator's external etcd cluster,
replace running images or roll out a new vCluster deployment. The shared
external service must independently use the qualified stock 3.6.16 release.
The chart's deployed-etcd image defaults and version-specific image mappings
are separate from this shared-external-etcd path and have not been changed to
an unverified container tag. Image building, distribution and real worker/CNI/CSI
acceptance remain release requirements.

The existing [lease authorization contract](shared-etcd-lease-authorization.md)
still applies: TTL-only metadata and the lack of permanent creator ownership
for empty leases are unchanged. No private patch is published by this update.
