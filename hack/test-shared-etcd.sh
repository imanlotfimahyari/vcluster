#!/usr/bin/env bash
set -euo pipefail

# Explicitly opt into disposable local-process integration tests. The Go tests
# generate their own credentials and never read the caller's kubeconfig.
: "${VCLUSTER_SHARED_ETCD_ASSETS:?Set this to a directory containing verified etcd, kube-apiserver, and helm binaries}"
export AGENT_SESSION="${AGENT_SESSION:-shared-etcd-prefix}"
export GOMAXPROCS="${GOMAXPROCS:-4}"

for test_binary in etcd kube-apiserver helm; do
  if [[ ! -x "${VCLUSTER_SHARED_ETCD_ASSETS}/${test_binary}" ]]; then
    echo "Missing executable test asset: ${test_binary}" >&2
    exit 1
  fi
done

etcd_version=$("${VCLUSTER_SHARED_ETCD_ASSETS}/etcd" --version)
if [[ "$etcd_version" != *"etcd Version: 3.6.16"$'\n'* ]]; then
  echo "The isolated shared-etcd baseline requires stock etcd 3.6.16" >&2
  exit 1
fi

cd "$(dirname "${BASH_SOURCE[0]}")/.."
# Qualify the same libraries that vendored CLI/runtime builds will ship.
# A matching external executable alone does not detect stale server code.
for etcd_module in api/v3 client/pkg/v3 client/v3 etcdutl/v3 pkg/v3 server/v3; do
  module_version=$(go list -mod=vendor -m -f '{{.Version}}' "go.etcd.io/etcd/${etcd_module}")
  if [[ "$module_version" != "v3.6.16" ]]; then
    echo "The shared-etcd dependency baseline requires ${etcd_module} v3.6.16, got ${module_version}" >&2
    exit 1
  fi
done

build_directory=$(mktemp -d)
trap 'rm -rf "$build_directory"' EXIT
export VCLUSTER_SHARED_ETCD_CLI="$build_directory/vcluster"
go build -mod=vendor -p=4 -o "$VCLUSTER_SHARED_ETCD_CLI" ./cmd/vcluster/
go vet -mod=vendor -p=4 -tags=integration ./test/integration/sharedetcd ./config ./pkg/k8s ./pkg/setup/config ./pkg/syncer/synccontext
go test -mod=vendor -p=4 -tags=integration -timeout=6m -count=1 -v ./test/integration/sharedetcd
