# Kubernetes etcd Status permission fallback

The status-probe retry originates in the Kubernetes API server binary. Editing
vCluster's vendored Kubernetes libraries does not change that external binary.

`etcd-status-permission-fallback.patch` targets Kubernetes v1.36.0, commit
`ecf6decece6a6de25a57aad9ba90b6ce580f6f78`. It treats etcd Status PermissionDenied
as a terminal, conservative capability decision: RequestWatchProgress is marked
unsupported and the ordinary storage paths remain available. It does not
invent a server version, enable an unverified optimization, suppress transient
connection errors, or grant additional etcd permissions.

The patch includes regression tests for canonical, gRPC, and wrapped permission
denials, and verifies that transient failures still return an error for retry.
It has been built as an API server and tested with restricted tenant identities
in the shared-etcd process harness. The opt-in assertion
`VCLUSTER_SHARED_ETCD_REQUIRE_STATUS_FALLBACK=1` checks for the capability fallback
and rejects repeated Status requests in the tenant process logs.

## Rebuild and integrate

In a clean checkout of the exact Kubernetes target version:

```sh
git apply /path/to/etcd-status-permission-fallback.patch
go test -mod=vendor ./staging/src/k8s.io/apiserver/pkg/storage/feature
CGO_ENABLED=0 go build -mod=vendor -o kube-apiserver ./cmd/kube-apiserver
```

The supplied Dockerfile replaces `/kubernetes/kube-apiserver` in a matching
vCluster Kubernetes distro image. Provide the exact base image explicitly and
use a private test tag. Configure that image through
`controlPlane.distro.k8s.image`; the chart's init container copies its binaries
into the vCluster Pod.

No image has been published and no upstream PR has been opened. This is a local
compatibility patch, not a claim that stock Kubernetes releases contain the fix.
Production use requires review, a matching image build, and the normal upgrade
checks. Rebase and test the patch against each supported Kubernetes release.

Database size polling remains disabled separately in tenant mode. Administrators
should collect cluster-wide etcd metrics with appropriately privileged tooling.
