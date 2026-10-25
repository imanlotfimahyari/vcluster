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

cd "$(dirname "${BASH_SOURCE[0]}")/.."
build_directory=$(mktemp -d)
trap 'rm -rf "$build_directory"' EXIT
export VCLUSTER_SHARED_ETCD_CLI="$build_directory/vcluster"
go build -mod=vendor -p=4 -o "$VCLUSTER_SHARED_ETCD_CLI" ./cmd/vcluster/
go vet -mod=vendor -p=4 -tags=integration ./test/integration/sharedetcd ./config ./pkg/k8s ./pkg/setup/config ./pkg/syncer/synccontext
go test -mod=vendor -p=4 -tags=integration -timeout=6m -count=1 -v ./test/integration/sharedetcd
