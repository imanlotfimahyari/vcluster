package config

import (
	"strings"
	"testing"

	"github.com/loft-sh/vcluster/pkg/config"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func legacyLayoutConfig() *config.VirtualClusterConfig {
	c := &config.VirtualClusterConfig{Name: "test", HostNamespace: "test"}
	c.HostClient = fake.NewSimpleClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "vc-config-test", Namespace: "test", UID: types.UID("deployment-a")}})
	return c
}

func TestStorageLayoutMarkerPreservesUpgradeIdentity(t *testing.T) {
	c := legacyLayoutConfig()
	if err := EnsureStorageLayout(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	marker, err := c.HostClient.CoreV1().ConfigMaps("test").Get(t.Context(), StorageLayoutMarkerName("test"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if marker.Immutable == nil || !*marker.Immutable || len(marker.OwnerReferences) != 0 || marker.Data["deploymentUID"] != "deployment-a" {
		t.Fatalf("invalid durable marker: %+v", marker)
	}
	if err := EnsureStorageLayout(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	c.ControlPlane.BackingStore.Etcd.External.Enabled = true
	c.ControlPlane.BackingStore.Etcd.External.StorePrefix = "/tenants/test"
	c.ControlPlane.Distro.K8S.Enabled = true
	c.ControlPlane.Distro.K8S.APIServer.Enabled = true
	if err := EnsureStorageLayout(t.Context(), c); err == nil || !strings.Contains(err.Error(), "rootPrefix") {
		t.Fatalf("prefix change was not rejected before storage access: %v", err)
	}
}

func TestStorageLayoutMarkerRejectsReplacementDeployment(t *testing.T) {
	c := legacyLayoutConfig()
	if err := EnsureStorageLayout(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	secret, err := c.HostClient.CoreV1().Secrets("test").Get(t.Context(), "vc-config-test", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	secret.UID = types.UID("deployment-b")
	if _, err := c.HostClient.CoreV1().Secrets("test").Update(t.Context(), secret, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := EnsureStorageLayout(t.Context(), c); err == nil || !strings.Contains(err.Error(), "deploymentUID") {
		t.Fatalf("replacement deployment accepted: %v", err)
	}
}

func TestStorageLayoutRejectsUnmarkedExistingDeployment(t *testing.T) {
	c := legacyLayoutConfig()
	c.ControlPlane.BackingStore.Etcd.External.Enabled = true
	c.ControlPlane.BackingStore.Etcd.External.StorePrefix = "/tenants/test"
	c.ControlPlane.Distro.K8S.Enabled = true
	c.ControlPlane.Distro.K8S.APIServer.Enabled = true
	if _, err := c.HostClient.CoreV1().Secrets("test").Create(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "vc-test"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := EnsureStorageLayout(t.Context(), c); err == nil || !strings.Contains(err.Error(), "offline adoption") {
		t.Fatalf("existing deployment was silently adopted: %v", err)
	}
}
