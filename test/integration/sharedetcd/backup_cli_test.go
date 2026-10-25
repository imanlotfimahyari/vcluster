//go:build integration && linux

package sharedetcd_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	setupconfig "github.com/loft-sh/vcluster/pkg/setup/config"
	"github.com/loft-sh/vcluster/pkg/tenantbackup"
	clientv3 "go.etcd.io/etcd/client/v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestTenantArchiveCLIWithRealHostAPI(t *testing.T) {
	binary := os.Getenv("VCLUSTER_SHARED_ETCD_CLI")
	if binary == "" {
		t.Skip("set VCLUSTER_SHARED_ETCD_CLI to the built vcluster CLI")
	}
	lab := newEtcdLab(t)
	host, kubeconfig, _ := startHostAPI(t, lab)
	ctx := t.Context()
	namespace := "tenant-a"
	if _, err := host.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.CoreV1().Secrets(namespace).Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "vc-config-tenant-a"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	cfg := lab.config(t, "tenant-a")
	cfg.HostClient = host
	if err := setupconfig.EnsureStorageLayout(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{"app": "vcluster", "release": "tenant-a"}
	zero := int32(0)
	deployment, err := host.AppsV1().Deployments(namespace).Create(ctx, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "tenant-a"},
		Spec:       appsv1.DeploymentSpec{Replicas: &zero, Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "syncer", Image: "test.invalid/unused:fixture"}}}}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// There is no host controller manager in this process fixture. Mark the
	// synthetic zero-replica Deployment observed; no workload Pods are started.
	deployment.Status.ObservedGeneration = deployment.Generation
	if _, err := host.AppsV1().Deployments(namespace).UpdateStatus(ctx, deployment, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	configFile := filepath.Join(t.TempDir(), "values.json")
	configData, err := json.Marshal(cfg.Config)
	if err != nil {
		t.Fatal(err)
	}
	writeFixture(t, configFile, configData)
	old := rawEtcdClient(t, lab.endpoint, lab.certs["tenant-a"])
	key := "/tenants/tenant-a/registry/synthetic/raw"
	if _, err := old.Put(ctx, key, "original-value"); err != nil {
		t.Fatal(err)
	}
	lease, err := old.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.Put(ctx, "/tenants/tenant-a/registry/events/default/cli", "temporary", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "backup.gz")
	run := func(args ...string) ([]byte, error) {
		call, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(call, binary, append([]string{"debug", "etcd"}, args...)...)
		return cmd.CombinedOutput()
	}
	out, err := run("tenant-backup", archive, "--config", configFile, "--name", "tenant-a")
	if err != nil {
		t.Fatalf("CLI backup failed: %v: %s", err, out)
	}
	out, err = run("tenant-inspect", archive)
	if err != nil {
		t.Fatalf("CLI inspection failed: %v: %s", err, out)
	}
	if strings.Contains(string(out), "original-value") {
		t.Fatal("inspection disclosed object data")
	}
	var summary tenantbackup.Summary
	if err := json.Unmarshal(out, &summary); err != nil || summary.Entries != 2 || summary.LeasedEntries != 1 {
		t.Fatalf("CLI summary was not valid: %v: %s", err, out)
	}
	recovery := recoveryIdentity(t, lab, summary.Root, "recovery-a")
	certs := lab.ca.identity(t, "recovery-a", false)
	admin := lab.certs["root"]
	args := []string{"tenant-restore", archive, "--config", configFile, "--name", "tenant-a", "--namespace", namespace, "--host-kubeconfig", kubeconfig, "--host-context", "test",
		"--admin-ca", admin.CaCert, "--admin-cert", admin.ServerCert, "--admin-key", admin.ServerKey,
		"--recovery-ca", certs.CaCert, "--recovery-cert", certs.ServerCert, "--recovery-key", certs.ServerKey}
	if out, err := run(args...); err == nil || !strings.Contains(string(out), "restart-lease-ttl") {
		t.Fatalf("CLI did not require explicit lease expiry policy: %v: %s", err, out)
	}
	args = append(args, "--restart-lease-ttl")
	if out, err := run(args...); err == nil || !strings.Contains(string(out), "still write") {
		t.Fatalf("CLI did not reject an unfenced writer: %v: %s", err, out)
	}
	if _, err := old.Put(ctx, key, "after-backup"); err != nil {
		t.Fatal(err)
	}
	if _, err := lab.admin.UserRevokeRole(ctx, "tenant-a", "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if out, err := run(args...); err != nil {
		t.Fatalf("CLI restore failed: %v: %s", err, out)
	}
	value, err := recovery.Get(ctx, key)
	if err != nil || len(value.Kvs) != 1 || string(value.Kvs[0].Value) != "original-value" {
		t.Fatal("CLI did not restore the original value")
	}
	if out, err := run(append(args, "--resume")...); err != nil {
		t.Fatalf("CLI completion confirmation failed: %v: %s", err, out)
	}
}
