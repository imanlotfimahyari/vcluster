package store

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"

	vclusterconfig "github.com/loft-sh/vcluster/config"
	"github.com/loft-sh/vcluster/pkg/etcd"
	"go.etcd.io/etcd/api/v3/mvccpb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

// MappingsPrefix is the legacy mappings path used by existing archives.
const MappingsPrefix = vclusterconfig.DefaultMappingsPrefix

// NewEtcdBackend uses the mappings prefix from a validated storage layout.
func NewEtcdBackend(etcdClient etcd.Client, mappingsPrefix string) Backend {
	return &etcdBackend{
		etcdClient:     etcdClient,
		mappingsPrefix: mappingsPrefix,
	}
}

type etcdBackend struct {
	etcdClient     etcd.Client
	mappingsPrefix string
}

func (m *etcdBackend) List(ctx context.Context) ([]*Mapping, error) {
	mappings, err := m.etcdClient.List(ctx, m.mappingsPrefix)
	if err != nil {
		return nil, fmt.Errorf("etcd backend: list mappings: %w", err)
	}

	retMappings := make([]*Mapping, 0, len(mappings))
	for _, kv := range mappings {
		retMapping := &Mapping{}
		err = json.Unmarshal(kv.Data, retMapping)
		if err != nil {
			return nil, fmt.Errorf("etcd backend: parse mapping %s: %w", string(kv.Key), err)
		}

		retMappings = append(retMappings, retMapping)
	}

	return retMappings, nil
}

func (m *etcdBackend) Watch(ctx context.Context) <-chan BackendWatchResponse {
	responseChan := make(chan BackendWatchResponse)
	watchChan := m.etcdClient.Watch(ctx, m.mappingsPrefix)
	go func() {
		defer close(responseChan)

		for event := range watchChan {
			switch {
			case event.Canceled:
				responseChan <- BackendWatchResponse{
					Err: event.Err(),
				}
			case len(event.Events) > 0:
				retEvents := make([]*BackendWatchEvent, 0, len(event.Events))
				for _, singleEvent := range event.Events {
					var eventType BackendWatchEventType
					switch singleEvent.Type {
					case mvccpb.PUT:
						eventType = BackendWatchEventTypeUpdate
					case mvccpb.DELETE:
						eventType = BackendWatchEventTypeDelete
					default:
						continue
					}

					// parse mapping
					retMapping := &Mapping{}

					value := singleEvent.Kv.Value
					if len(value) == 0 && singleEvent.Type == mvccpb.DELETE && singleEvent.PrevKv != nil {
						value = singleEvent.PrevKv.Value
					}

					err := json.Unmarshal(value, retMapping)
					if err != nil {
						klog.FromContext(ctx).Info(
							"etcd backend: Error decoding event",
							"key", string(singleEvent.Kv.Key),
							"singleEventValue", string(singleEvent.Kv.Value),
							"eventType", eventType,
							"error", err.Error(),
						)

						// we only send the reconstructed mapping to the consumer
						if eventType == BackendWatchEventTypeDelete {
							retEvents = append(retEvents, &BackendWatchEvent{
								Type:    BackendWatchEventTypeDeleteReconstructed,
								Mapping: m.reconstructNameMappingFromKey(string(singleEvent.Kv.Key)),
							})
						}

						continue
					}

					retEvents = append(retEvents, &BackendWatchEvent{
						Type:    eventType,
						Mapping: retMapping,
					})
				}

				responseChan <- BackendWatchResponse{
					Events: retEvents,
				}
			}
		}
	}()

	return responseChan
}

func (m *etcdBackend) Save(ctx context.Context, mapping *Mapping) error {
	mappingBytes, err := json.Marshal(mapping)
	if err != nil {
		return err
	}

	key, err := m.mappingToKey(mapping)
	if err != nil {
		return err
	}
	_, err = m.etcdClient.Put(ctx, key, mappingBytes)
	return err
}

func (m *etcdBackend) Delete(ctx context.Context, mapping *Mapping) error {
	key, err := m.mappingToKey(mapping)
	if err != nil {
		return err
	}
	return m.etcdClient.Delete(ctx, key)
}

func (m *etcdBackend) reconstructNameMappingFromKey(key string) *Mapping {
	retMapping := &Mapping{}
	trimmedKey := strings.TrimPrefix(key, m.mappingsPrefix)
	splittedKey := strings.Split(trimmedKey, "/")
	if splittedKey[0] == "v1" {
		retMapping.GroupVersionKind = corev1.SchemeGroupVersion.WithKind(splittedKey[1])
		if len(splittedKey) == 4 {
			retMapping.VirtualName = types.NamespacedName{
				Namespace: splittedKey[2],
				Name:      splittedKey[3],
			}
		} else {
			retMapping.VirtualName = types.NamespacedName{
				Name: splittedKey[2],
			}
		}
	} else {
		retMapping.GroupVersionKind = schema.GroupVersionKind{
			Group:   splittedKey[0],
			Version: splittedKey[1],
			Kind:    splittedKey[2],
		}
		if len(splittedKey) == 5 {
			retMapping.VirtualName = types.NamespacedName{
				Namespace: splittedKey[3],
				Name:      splittedKey[4],
			}
		} else {
			retMapping.VirtualName = types.NamespacedName{
				Name: splittedKey[3],
			}
		}
	}

	return retMapping
}

func (m *etcdBackend) mappingToKey(mapping *Mapping) (string, error) {
	if mapping == nil {
		return "", fmt.Errorf("mapping must not be nil")
	}
	// Each logical key component must stay in its own path segment. In
	// particular debug commands must never escape the selected tenant range.
	parts := []string{mapping.Version, mapping.Kind, mapping.VirtualName.Name}
	if mapping.Group != "" {
		parts = append(parts, mapping.Group)
	}
	if mapping.VirtualName.Namespace != "" {
		parts = append(parts, mapping.VirtualName.Namespace)
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." || strings.ContainsAny(part, "/\\\x00") {
			return "", fmt.Errorf("mapping contains an invalid storage key component")
		}
	}

	nameNamespace := mapping.VirtualName.Name
	if mapping.VirtualName.Namespace != "" {
		nameNamespace = mapping.VirtualName.Namespace + "/" + nameNamespace
	}

	return path.Join(m.mappingsPrefix, mapping.GroupVersion().String(), strings.ToLower(mapping.Kind), nameNamespace), nil
}
