package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/loft-sh/vcluster/pkg/etcd"
	"github.com/loft-sh/vcluster/pkg/syncer/synccontext"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// Only the methods used by the mappings backend are implemented; an unexpected
// operation on the embedded interface fails the test rather than reaching etcd.
type mappingsEtcdFake struct {
	etcd.Client
	values  map[string][]byte
	watches map[string][]chan clientv3.WatchResponse
}

func newMappingsEtcdFake(t *testing.T) *mappingsEtcdFake {
	t.Helper()
	f := &mappingsEtcdFake{values: map[string][]byte{}, watches: map[string][]chan clientv3.WatchResponse{}}
	t.Cleanup(func() {
		for _, channels := range f.watches {
			for _, ch := range channels {
				close(ch)
			}
		}
	})
	return f
}

func (f *mappingsEtcdFake) List(_ context.Context, prefix string) ([]etcd.Value, error) {
	var values []etcd.Value
	for key, value := range f.values {
		if strings.HasPrefix(key, prefix) {
			values = append(values, etcd.Value{Key: []byte(key), Data: value})
		}
	}
	return values, nil
}

func (f *mappingsEtcdFake) Watch(_ context.Context, prefix string) clientv3.WatchChan {
	ch := make(chan clientv3.WatchResponse, 16)
	f.watches[prefix] = append(f.watches[prefix], ch)
	return ch
}

func (f *mappingsEtcdFake) Put(_ context.Context, key string, value []byte) (int64, error) {
	f.values[key] = value
	f.emit(key, value, mvccpb.PUT)
	return 1, nil
}

func (f *mappingsEtcdFake) Delete(_ context.Context, key string) error {
	delete(f.values, key)
	// No value or PrevKv: exercise key-based reconstruction on delete.
	f.emit(key, nil, mvccpb.DELETE)
	return nil
}

func (f *mappingsEtcdFake) emit(key string, value []byte, eventType mvccpb.Event_EventType) {
	for prefix, channels := range f.watches {
		if strings.HasPrefix(key, prefix) {
			for _, ch := range channels {
				ch <- clientv3.WatchResponse{Events: []*clientv3.Event{{Type: eventType, Kv: &mvccpb.KeyValue{Key: []byte(key), Value: value}}}}
			}
		}
	}
}

func tenantTestMapping(group, version, kind, namespace, name, host string) *Mapping {
	return &Mapping{NameMapping: synccontext.NameMapping{
		GroupVersionKind: schema.GroupVersionKind{Group: group, Version: version, Kind: kind},
		VirtualName:      types.NamespacedName{Namespace: namespace, Name: name},
		HostName:         types.NamespacedName{Name: host},
	}}
}

func nextMappingEvent(t *testing.T, ch <-chan BackendWatchResponse, want BackendWatchEventType) *Mapping {
	t.Helper()
	select {
	case response := <-ch:
		if response.Err != nil || len(response.Events) != 1 || response.Events[0].Type != want {
			t.Fatalf("unexpected watch response: %+v", response)
		}
		return response.Events[0].Mapping
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for mapping event")
		return nil
	}
}

func TestEtcdBackendTenantIsolation(t *testing.T) {
	t.Parallel()
	f := newMappingsEtcdFake(t)
	ctx := t.Context()
	aPrefix, bPrefix := "/tenants/a/mappings/", "/tenants/b/mappings/"
	a, b := NewEtcdBackend(f, aPrefix), NewEtcdBackend(f, bPrefix)
	aWatch, bWatch := a.Watch(ctx), b.Watch(ctx)

	// Identical virtual names must coexist with different physical mappings.
	aMapping := tenantTestMapping("", "v1", "ConfigMap", "default", "same-name", "host-a")
	bMapping := tenantTestMapping("", "v1", "ConfigMap", "default", "same-name", "host-b")
	for _, item := range []struct {
		backend Backend
		mapping *Mapping
	}{{a, aMapping}, {b, bMapping}} {
		if err := item.backend.Save(ctx, item.mapping); err != nil {
			t.Fatal(err)
		}
	}
	if nextMappingEvent(t, aWatch, BackendWatchEventTypeUpdate).HostName.Name != "host-a" ||
		nextMappingEvent(t, bWatch, BackendWatchEventTypeUpdate).HostName.Name != "host-b" {
		t.Fatal("watch crossed tenant boundaries")
	}

	// An adjacent prefix must never become part of the mappings range.
	data, err := json.Marshal(bMapping)
	if err != nil {
		t.Fatal(err)
	}
	f.values["/tenants/a/mappings-other/v1/configmap/default/same-name"] = data
	for _, item := range []struct{ prefix, host string }{{aPrefix, "host-a"}, {bPrefix, "host-b"}} {
		// A fresh backend represents a controller restart against the same store.
		mappings, err := NewEtcdBackend(f, item.prefix).List(ctx)
		if err != nil || len(mappings) != 1 || mappings[0].HostName.Name != item.host {
			t.Fatalf("restart/list isolation failed: mappings=%+v, err=%v", mappings, err)
		}
	}
	aMapping.HostName.Name = "host-a-updated"
	if err := a.Save(ctx, aMapping); err != nil {
		t.Fatal(err)
	}
	if nextMappingEvent(t, aWatch, BackendWatchEventTypeUpdate).HostName.Name != "host-a-updated" {
		t.Fatal("mapping update lost")
	}

	// Debug clear lists and deletes mappings through the selected backend.
	toClear, err := a.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, mapping := range toClear {
		if err := a.Delete(ctx, mapping); err != nil {
			t.Fatal(err)
		}
	}
	deleted := nextMappingEvent(t, aWatch, BackendWatchEventTypeDeleteReconstructed)
	if deleted.VirtualName != aMapping.VirtualName || deleted.Kind != "configmap" || deleted.Version != "v1" {
		t.Fatalf("wrong reconstructed deletion: %+v", deleted)
	}
	remaining, err := b.List(ctx)
	if err != nil || len(remaining) != 1 || remaining[0].HostName.Name != "host-b" {
		t.Fatalf("clear affected the other tenant: %+v, %v", remaining, err)
	}
	if len(f.watches[bPrefix][0]) != 0 {
		t.Fatal("other tenant received a watch event")
	}
	select {
	case event := <-bWatch:
		t.Fatalf("other tenant received an unexpected event: %+v", event)
	default:
	}
	if len(f.values) != 2 {
		t.Fatalf("clear deleted adjacent or foreign keys: %v", f.values)
	}
}

func TestEtcdBackendMappingKeyRoundTrip(t *testing.T) {
	t.Parallel()
	for _, prefix := range []string{MappingsPrefix, "/tenants/test/mappings/"} {
		for _, mapping := range []*Mapping{
			tenantTestMapping("", "v1", "ConfigMap", "default", "test", "host"),
			tenantTestMapping("", "v1", "Node", "", "test", "host"),
			tenantTestMapping("apps", "v1", "Deployment", "default", "test", "host"),
			tenantTestMapping("example.test", "v1beta1", "Widget", "", "test", "host"),
		} {
			backend := &etcdBackend{mappingsPrefix: prefix}
			key, err := backend.mappingToKey(mapping)
			if err != nil {
				t.Fatal(err)
			}
			want := prefix + mapping.GroupVersion().String() + "/" + strings.ToLower(mapping.Kind) + "/"
			if mapping.VirtualName.Namespace != "" {
				want += mapping.VirtualName.Namespace + "/"
			}
			want += mapping.VirtualName.Name
			if key != want {
				t.Fatalf("key = %q, want %q", key, want)
			}
			reconstructed := backend.reconstructNameMappingFromKey(key)
			if reconstructed.VirtualName != mapping.VirtualName || reconstructed.GroupVersion() != mapping.GroupVersion() || reconstructed.Kind != strings.ToLower(mapping.Kind) {
				t.Fatalf("round trip failed: %+v", reconstructed)
			}
		}
	}
}

func TestEtcdBackendRejectsMappingKeyTraversal(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*Mapping){
		"namespace":  func(m *Mapping) { m.VirtualName.Namespace = "../../../b/mappings" },
		"name":       func(m *Mapping) { m.VirtualName.Name = "../../other" },
		"kind":       func(m *Mapping) { m.Kind = "../../other" },
		"group":      func(m *Mapping) { m.Group = "../other" },
		"version":    func(m *Mapping) { m.Version = ".." },
		"empty name": func(m *Mapping) { m.VirtualName.Name = "" },
	} {
		t.Run(name, func(t *testing.T) {
			f := newMappingsEtcdFake(t)
			backend := NewEtcdBackend(f, "/tenants/a/mappings/")
			mapping := tenantTestMapping("", "v1", "Pod", "default", "test", "host")
			change(mapping)
			if backend.Save(t.Context(), mapping) == nil || backend.Delete(t.Context(), mapping) == nil {
				t.Fatal("mapping traversal accepted")
			}
			if len(f.values) != 0 {
				t.Fatal("invalid mapping reached etcd")
			}
		})
	}
}
