//go:build integration && linux

package sharedetcd_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	setupconfig "github.com/loft-sh/vcluster/pkg/setup/config"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestTenantRootCannotBeClaimedByAnotherDeployment(t *testing.T) {
	lab := newEtcdLab(t)
	host, _, _ := startHostAPI(t, lab)
	for _, namespace := range []string{"owner-a", "owner-b"} {
		if _, err := host.CoreV1().Namespaces().Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, err := host.CoreV1().Secrets(namespace).Create(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "vc-config-test"}}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	cfg := lab.config(t, "tenant-a")
	cfg.Name = "test"
	cfg.HostNamespace = "owner-a"
	cfg.HostClient = host
	if err := setupconfig.EnsureStorageLayout(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	if err := setupconfig.EnsureStorageLayout(t.Context(), cfg); err != nil {
		t.Fatalf("idempotent claim failed: %v", err)
	}
	cfg.HostNamespace = "owner-b"
	if err := setupconfig.EnsureStorageLayout(t.Context(), cfg); err == nil || !strings.Contains(err.Error(), "deploymentUID") {
		t.Fatalf("another deployment claimed the same root: %v", err)
	}
	cfg.ControlPlane.BackingStore.Etcd.External.StorePrefix = "/tenants/tenant-a/unclaimed"
	if _, err := lab.admin.Put(t.Context(), "/tenants/tenant-a/unclaimed/registry/existing", "test"); err != nil {
		t.Fatal(err)
	}
	if err := setupconfig.EnsureStorageLayout(t.Context(), cfg); err == nil || !strings.Contains(err.Error(), "without an ownership claim") {
		t.Fatalf("unclaimed existing data was adopted: %v", err)
	}
}

func TestHelmUpgradePreservesStorageLayout(t *testing.T) {
	helm := filepath.Join(os.Getenv("VCLUSTER_SHARED_ETCD_ASSETS"), "helm")
	if _, err := os.Stat(helm); err != nil {
		t.Skip("Helm executable is required for the real upgrade test")
	}
	lab := newEtcdLab(t)
	host, kubeconfig, _ := startHostAPI(t, lab)
	namespace := "helm-layout"
	if _, err := host.CoreV1().Namespaces().Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.CoreV1().Secrets(namespace).Create(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "vc-config-test"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	cfg := lab.config(t, "tenant-a")
	cfg.Name = "test"
	cfg.HostNamespace = namespace
	cfg.HostClient = host
	if err := setupconfig.EnsureStorageLayout(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	chart, err := filepath.Abs("../../../chart")
	if err != nil {
		t.Fatal(err)
	}
	run := func(prefix string, dryRun bool) ([]byte, error) {
		args := []string{"upgrade", "--install", "test", chart, "--kubeconfig=" + kubeconfig, "--namespace=" + namespace, "--take-ownership",
			"--set=controlPlane.backingStore.etcd.external.enabled=true", "--set=controlPlane.backingStore.etcd.external.endpoint=" + lab.endpoint,
			"--set=controlPlane.backingStore.etcd.external.storePrefix=" + prefix, "--set=telemetry.enabled=false"}
		if dryRun {
			args = append(args, "--dry-run=server")
		}
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		return exec.CommandContext(ctx, helm, args...).CombinedOutput()
	}
	if out, err := run("/tenants/tenant-a", false); err != nil {
		t.Fatalf("initial Helm install failed: %v\n%s", err, out)
	}
	if out, err := run("/tenants/tenant-a/", false); err != nil {
		t.Fatalf("same-layout Helm upgrade failed: %v\n%s", err, out)
	}
	for _, prefix := range []string{"/tenants/tenant-b", ""} {
		out, err := run(prefix, true)
		if err == nil || !strings.Contains(string(out), "storePrefix cannot change") {
			t.Fatalf("Helm did not reject changed layout: %v\n%s", err, out)
		}
	}
	marker, err := host.CoreV1().ConfigMaps(namespace).Get(t.Context(), setupconfig.StorageLayoutMarkerName("test"), metav1.GetOptions{})
	if err != nil || marker.Data["rootPrefix"] != "/tenants/tenant-a/" {
		t.Fatalf("Helm modified the durable guard: %v", err)
	}
}
