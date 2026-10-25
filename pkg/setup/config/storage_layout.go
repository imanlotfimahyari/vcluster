package config

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/loft-sh/vcluster/pkg/config"
	"github.com/loft-sh/vcluster/pkg/etcd"
	"github.com/loft-sh/vcluster/pkg/tenantbackup"
	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const storageLayoutVersion = "1"

// StorageLayoutMarkerName is deliberately separate from Helm-owned resources.
// It must survive an upgrade or uninstall until an operator decommissions the
// backing data explicitly. Never attach a garbage-collecting owner reference.
func StorageLayoutMarkerName(name string) string { return "vc-storage-layout-" + name }

func storageLayoutData(vConfig *config.VirtualClusterConfig, deploymentUID string) (map[string]string, error) {
	layout, err := vConfig.ResolveEtcdStorageLayout()
	if err != nil {
		return nil, err
	}
	data := map[string]string{"version": storageLayoutVersion, "rootPrefix": layout.RootPrefix, "deploymentUID": deploymentUID}
	if layout.RootPrefix != "" {
		data["etcdEndpoint"] = strings.TrimSuffix(strings.TrimPrefix(vConfig.ControlPlane.BackingStore.Etcd.External.Endpoint, "https://"), "/")
	}
	return data, nil
}

func validateStorageLayoutMarker(existing, wanted map[string]string) error {
	for _, key := range []string{"version", "rootPrefix", "deploymentUID", "etcdEndpoint"} {
		if existing[key] != wanted[key] {
			return fmt.Errorf("persisted storage layout does not match %s; an explicit storage migration or recovery is required", key)
		}
	}
	return nil
}

// EnsureStorageLayout rejects a changed prefix before any API server or syncer
// writes. The host marker catches edits through Helm or configuration Secrets;
// the etcd claim rejects another deployment using the exact same tenant root.
func EnsureStorageLayout(ctx context.Context, vConfig *config.VirtualClusterConfig) error {
	secret, err := vConfig.HostClient.CoreV1().Secrets(vConfig.HostNamespace).Get(ctx, "vc-config-"+vConfig.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read deployment identity: %w", err)
	}
	if secret.UID == "" {
		return fmt.Errorf("deployment configuration Secret has no UID")
	}
	wanted, err := storageLayoutData(vConfig, string(secret.UID))
	if err != nil {
		return err
	}
	markers := vConfig.HostClient.CoreV1().ConfigMaps(vConfig.HostNamespace)
	marker, err := markers.Get(ctx, StorageLayoutMarkerName(vConfig.Name), metav1.GetOptions{})
	if kerrors.IsNotFound(err) {
		// Generated Kubernetes clients may return a non-nil zero-value object
		// together with NotFound. It is not an existing durable marker.
		marker = nil
	}
	if err == nil {
		if err := validateStorageLayoutMarker(marker.Data, wanted); err != nil {
			return err
		}
	} else if !kerrors.IsNotFound(err) {
		return fmt.Errorf("read persisted storage layout: %w", err)
	} else if wanted["rootPrefix"] != "" {
		// Older deployments have no durable marker. Do not interpret the absence of
		// a marker as permission to point an existing deployment at a new keyspace.
		_, err := vConfig.HostClient.CoreV1().Secrets(vConfig.HostNamespace).Get(ctx, "vc-"+vConfig.Name, metav1.GetOptions{})
		if err == nil {
			return fmt.Errorf("existing vCluster has no storage layout marker; explicit offline adoption or migration is required")
		}
		if !kerrors.IsNotFound(err) {
			return fmt.Errorf("check existing deployment: %w", err)
		}
	}
	if wanted["rootPrefix"] != "" {
		claimCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		endpoint, certs := etcd.GetEtcdEndpoint(vConfig)
		raw, err := etcd.GetEtcdClient(claimCtx, certs, endpoint)
		if err != nil {
			return err
		}
		defer raw.Close()
		if err := claimTenantStorage(claimCtx, raw, wanted); err != nil {
			return err
		}
	}
	if marker != nil {
		return nil
	}
	immutable := true
	_, err = markers.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: StorageLayoutMarkerName(vConfig.Name)}, Immutable: &immutable, Data: wanted}, metav1.CreateOptions{})
	if kerrors.IsAlreadyExists(err) {
		marker, err = markers.Get(ctx, StorageLayoutMarkerName(vConfig.Name), metav1.GetOptions{})
		if err != nil {
			return err
		}
		return validateStorageLayoutMarker(marker.Data, wanted)
	}
	if err != nil {
		return fmt.Errorf("persist storage layout: %w", err)
	}
	return nil
}

func claimTenantStorage(ctx context.Context, raw *clientv3.Client, wanted map[string]string) error {
	root := wanted["rootPrefix"]
	claimKey := root + "vcluster/storage-layout"
	payload, err := json.Marshal(wanted)
	if err != nil {
		return err
	}
	// The range comparison and claim are atomic: never adopt an existing,
	// unclaimed tenant keyspace, even if another client writes during startup.
	response, err := raw.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(root+tenantbackup.RestoreSuffix), "=", 0),
		clientv3.Compare(clientv3.CreateRevision(root+tenantbackup.FenceSuffix), "=", 0),
		clientv3.Compare(clientv3.CreateRevision(root+tenantbackup.MigrationSuffix), "=", 0),
	).Then(clientv3.OpTxn(
		[]clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision(root), "=", 0).WithPrefix()},
		[]clientv3.Op{clientv3.OpPut(claimKey, string(payload))},
		[]clientv3.Op{clientv3.OpGet(claimKey)},
	)).Commit()
	if err != nil {
		return fmt.Errorf("claim tenant storage: %w", err)
	}
	if !response.Succeeded {
		return fmt.Errorf("tenant storage has an active recovery or migration barrier")
	}
	claimResponse := response.Responses[0].GetResponseTxn()
	if claimResponse.Succeeded {
		return nil
	}
	kvs := claimResponse.Responses[0].GetResponseRange().Kvs
	if len(kvs) != 1 {
		return fmt.Errorf("tenant storage contains data without an ownership claim; explicit offline adoption or recovery is required")
	}
	existing := map[string]string{}
	if err := json.Unmarshal(kvs[0].Value, &existing); err != nil {
		return fmt.Errorf("invalid tenant storage ownership claim")
	}
	return validateStorageLayoutMarker(existing, wanted)
}
