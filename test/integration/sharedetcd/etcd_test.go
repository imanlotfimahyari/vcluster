//go:build integration && linux

package sharedetcd_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/loft-sh/vcluster/pkg/config"
	"github.com/loft-sh/vcluster/pkg/etcd"
	"github.com/loft-sh/vcluster/pkg/mappings/store"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

func TestRealEtcdTenantIsolation(t *testing.T) {
	lab := newEtcdLab(t)
	aCfg, bCfg := lab.config(t, "tenant-a"), lab.config(t, "tenant-b")
	clients := make([]etcd.Client, 2)
	for i, cfg := range []*config.VirtualClusterConfig{aCfg, bCfg} {
		probeCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		certs := lab.certs[cfg.Name]
		err := etcd.WaitForEtcd(probeCtx, certs, lab.endpoint)
		cancel()
		if err != nil {
			t.Fatalf("tenant readiness with restricted credentials failed: %v", err)
		}
		client, err := etcd.NewFromConfig(t.Context(), cfg)
		if err != nil {
			t.Fatalf("tenant client startup with restricted credentials failed: %v", err)
		}
		t.Cleanup(func() { _ = client.Close() })
		clients[i] = client
	}
	aLayout, err := aCfg.ResolveEtcdStorageLayout()
	if err != nil {
		t.Fatal(err)
	}
	bLayout, err := bCfg.ResolveEtcdStorageLayout()
	if err != nil {
		t.Fatal(err)
	}
	a, b := store.NewEtcdBackend(clients[0], aLayout.MappingsPrefix), store.NewEtcdBackend(clients[1], bLayout.MappingsPrefix)
	aMapping := testMapping("host-a")
	bMapping := testMapping("host-b")
	if err := a.Save(t.Context(), aMapping); err != nil {
		t.Fatal(err)
	}
	if err := b.Save(t.Context(), bMapping); err != nil {
		t.Fatal(err)
	}
	assertMappings(t, a, "host-a")
	assertMappings(t, b, "host-b")

	// Save repeatedly until the asynchronous watch is established. There is no
	// ready signal in Backend.Watch; a fixed sleep would make this test flaky.
	aWatch := a.Watch(t.Context())
	awaitMappingUpdate(t, a, aWatch, aMapping)
	if err := a.Delete(t.Context(), aMapping); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-aWatch:
		if event.Err != nil || len(event.Events) != 1 || event.Events[0].Type != store.BackendWatchEventTypeDelete {
			t.Fatalf("unexpected deletion event: %+v", event)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no delete watch event")
	}
	assertMappings(t, b, "host-b")

	// A newly initialized backend reconnects using the same tenant credentials.
	reconnected, err := etcd.NewFromConfig(t.Context(), bCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reconnected.Close() })
	assertMappings(t, store.NewEtcdBackend(reconnected, bLayout.MappingsPrefix), "host-b")

	for _, direction := range []struct{ name, other string }{{"tenant-a", "tenant-b"}, {"tenant-b", "tenant-a"}} {
		t.Run(direction.name+"-cross-tenant-denial", func(t *testing.T) {
			raw := rawEtcdClient(t, lab.endpoint, lab.certs[direction.name])
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			foreign := "/tenants/" + direction.other + "/"
			_, err := raw.Get(ctx, foreign, clientv3.WithPrefix())
			assertPermissionDenied(t, err)
			_, err = raw.Put(ctx, foreign+"unauthorized", "test")
			assertPermissionDenied(t, err)
			_, err = raw.Delete(ctx, foreign, clientv3.WithPrefix())
			assertPermissionDenied(t, err)
			_, err = raw.Get(ctx, "/", clientv3.WithPrefix())
			assertPermissionDenied(t, err)
			response := <-raw.Watch(ctx, foreign, clientv3.WithPrefix())
			// Watch transports authorization errors in a cancellation reason.
			if !response.Canceled || response.Err() == nil || !strings.Contains(response.Err().Error(), "etcdserver: permission denied") {
				t.Fatalf("expected permission-denied watch cancellation, got %+v", response)
			}
		})
	}
	// Debug clear's list/delete sequence must preserve the other tenant's data.
	mappings, err := b.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, mapping := range mappings {
		if err := b.Delete(t.Context(), mapping); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Save(t.Context(), aMapping); err != nil {
		t.Fatal(err)
	}
	assertMappings(t, a, "host-a")
	remaining, err := b.List(t.Context())
	if err != nil || len(remaining) != 0 {
		t.Fatalf("clear failed: %v, %v", remaining, err)
	}
}

func testMapping(host string) *store.Mapping {
	return &store.Mapping{NameMapping: synccontext.NameMapping{
		GroupVersionKind: schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"},
		VirtualName:      types.NamespacedName{Namespace: "default", Name: "same-name"},
		HostName:         types.NamespacedName{Namespace: "host", Name: host},
	}}
}

func assertMappings(t *testing.T, backend store.Backend, host string) {
	t.Helper()
	mappings, err := backend.List(t.Context())
	if err != nil || len(mappings) != 1 || mappings[0].HostName.Name != host {
		t.Fatalf("unexpected tenant mappings: %+v, %v", mappings, err)
	}
}

func awaitMappingUpdate(t *testing.T, backend store.Backend, ch <-chan store.BackendWatchResponse, mapping *store.Mapping) {
	t.Helper()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := backend.Save(t.Context(), mapping); err != nil {
			t.Fatal(err)
		}
		select {
		case event := <-ch:
			if event.Err != nil || len(event.Events) != 1 || event.Events[0].Mapping.HostName != mapping.HostName {
				t.Fatalf("wrong watch event: %+v", event)
			}
			return
		case <-ticker.C:
		case <-timeout.C:
			t.Fatal("watch did not become ready")
		}
	}
}

func assertPermissionDenied(t *testing.T, err error) {
	t.Helper()
	if status.Code(err) != codes.PermissionDenied && !errors.Is(err, rpctypes.ErrPermissionDenied) {
		t.Fatalf("expected etcd permission denial, got %v", err)
	}
}
