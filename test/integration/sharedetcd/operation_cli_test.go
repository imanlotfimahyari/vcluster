//go:build integration && linux

package sharedetcd_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/loft-sh/vcluster/pkg/config"
	setupconfig "github.com/loft-sh/vcluster/pkg/setup/config"
	"github.com/loft-sh/vcluster/pkg/tenantbackup"
	clientv3 "go.etcd.io/etcd/client/v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestJournaledMigrationAndRecoveryCLI(t *testing.T) {
	binary := os.Getenv("VCLUSTER_SHARED_ETCD_CLI")
	if binary == "" {
		t.Skip("set VCLUSTER_SHARED_ETCD_CLI")
	}
	for _, mode := range []string{"migrate", "recover", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			lab := newEtcdLab(t)
			ctx := t.Context()
			host, kubeconfig, _ := startHostAPI(t, lab)
			name := "tenant-a"
			namespace := name
			_, err := host.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			cfg := lab.config(t, name)
			writer := rawEtcdClient(t, lab.endpoint, lab.certs[name])
			key := "/tenants/tenant-a/registry/test/value"
			if mode != "recover" {
				writer = legacyWriter(t, lab)
				cfg.ControlPlane.BackingStore.Etcd.External.StorePrefix = ""
				cfg.ControlPlane.Distro.K8S.APIServer.ExtraArgs = []string{"--etcd-compaction-interval=0", "--etcd_db_metric_poll_interval", "0s"}
				key = "/registry/test/value"
			}
			deployed := lab.certs[name]
			if mode != "recover" {
				deployed = lab.certs["legacy"]
			}
			ca, _ := os.ReadFile(deployed.CaCert)
			cert, _ := os.ReadFile(deployed.ServerCert)
			keyBytes, _ := os.ReadFile(deployed.ServerKey)
			tlsSecret, err := host.CoreV1().Secrets(namespace).Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "dedicated-etcd"}, Data: map[string][]byte{"ca.crt": ca, "tls.crt": cert, "tls.key": keyBytes}}, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			cfg.ControlPlane.BackingStore.Etcd.External.TLS.CaFile = "/external-etcd/ca.crt"
			cfg.ControlPlane.BackingStore.Etcd.External.TLS.CertFile = "/external-etcd/tls.crt"
			cfg.ControlPlane.BackingStore.Etcd.External.TLS.KeyFile = "/external-etcd/tls.key"
			data, err := json.Marshal(cfg.Config)
			if err != nil {
				t.Fatal(err)
			}
			configSecret, err := host.CoreV1().Secrets(namespace).Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "vc-config-" + name}, Data: map[string][]byte{"config.yaml": data}}, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "recover" {
				cfg.HostClient = host
				// Claim using real etcd credentials; the deployed values intentionally use
				// Pod mount paths that are unavailable in this external test process.
				saved := cfg.ControlPlane.BackingStore.Etcd.External.TLS
				cfg.ControlPlane.BackingStore.Etcd.External.TLS = lab.config(t, name).ControlPlane.BackingStore.Etcd.External.TLS
				if err := setupconfig.EnsureStorageLayout(ctx, cfg); err != nil {
					t.Fatal(err)
				}
				cfg.ControlPlane.BackingStore.Etcd.External.TLS = saved
			}
			zero := int32(0)
			labels := map[string]string{"app": "vcluster", "release": name}
			deployment, err := host.AppsV1().Deployments(namespace).Create(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: appsv1.DeploymentSpec{Replicas: &zero, Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "external-etcd", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: tlsSecret.Name}}}}, Containers: []corev1.Container{{Name: "syncer", Image: "test.invalid/fixture", VolumeMounts: []corev1.VolumeMount{{Name: "external-etcd", MountPath: "/external-etcd"}}}}}}}}, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			deployment.Status.ObservedGeneration = deployment.Generation
			if _, err := host.AppsV1().Deployments(namespace).UpdateStatus(ctx, deployment, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := writer.Put(ctx, key, "before"); err != nil {
				t.Fatal(err)
			}
			if mode != "recover" {
				if _, err := writer.Put(ctx, "/vcluster/mappings/v1/pods/p", "mapping"); err != nil {
					t.Fatal(err)
				}
			}
			archive := filepath.Join(t.TempDir(), "archive.gz")
			if mode == "recover" {
				if _, err := tenantbackup.Export(ctx, writer, "/tenants/tenant-a/", archive); err != nil {
					t.Fatal(err)
				}
				if _, err := writer.Put(ctx, key, "after"); err != nil {
					t.Fatal(err)
				}
			}
			admin := lab.certs["root"]
			action := mode
			if mode == "rollback" {
				action = "migrate"
			}
			args := []string{"debug", "etcd", "tenant-" + action, archive, "--name", name, "--namespace", namespace, "--host-kubeconfig", kubeconfig, "--host-context", "test", "--tls-secret", tlsSecret.Name, "--admin-ca", admin.CaCert, "--admin-cert", admin.ServerCert, "--admin-key", admin.ServerKey, "--issuer-cert", lab.ca.path, "--issuer-key", filepath.Join(lab.ca.dir, "ca.key"), "--timeout", "45s"}
			if mode != "recover" {
				args = append(args, "--store-prefix", "/tenants/migrated")
			}
			run := func(extra ...string) ([]byte, error) {
				ctx, cancel := context.WithTimeout(ctx, time.Minute)
				defer cancel()
				return exec.CommandContext(ctx, binary, append(slices.Clone(args), extra...)...).CombinedOutput()
			}
			out, err := run()
			if err != nil {
				t.Fatalf("prepare/copy CLI failed: %v: %s", err, out)
			}
			if !strings.Contains(string(out), "verified") {
				t.Fatalf("copy did not reach verified: %s", out)
			}
			assertPermissionDenied(t, func() error { _, err := writer.Put(ctx, key, "stale"); return err }())
			// Both tenant and legacy startup must respect the host journal.
			cfg.HostClient = host
			if err := setupconfig.EnsureStorageLayout(ctx, cfg); err == nil || !strings.Contains(err.Error(), "operation blocks") {
				t.Fatalf("runtime did not honor journal: %v", err)
			}
			if mode == "rollback" {
				if out, err := run("--rollback"); err != nil {
					t.Fatalf("rollback CLI failed: %v: %s", err, out)
				}
				if _, err := writer.Put(ctx, key, "restored-access"); err != nil {
					t.Fatal(err)
				}
				result, err := lab.admin.Get(ctx, "/tenants/migrated/", clientv3.WithPrefix())
				if err != nil || len(result.Kvs) != 0 {
					t.Fatal("rollback left target data")
				}
				return
			}
			if out, err := run("--resume", "--commit"); err != nil {
				t.Fatalf("cutover CLI failed: %v: %s", err, out)
			}
			if out, err := run("--resume", "--commit"); err != nil {
				t.Fatalf("completion confirmation failed: %v: %s", err, out)
			}
			current, err := host.CoreV1().Secrets(namespace).Get(ctx, configSecret.Name, metav1.GetOptions{})
			if err != nil || current.UID != configSecret.UID {
				t.Fatal("configuration Secret identity changed")
			}
			parsed, err := config.ParseConfigBytes(current.Data["config.yaml"], name, nil)
			if err != nil {
				t.Fatal(err)
			}
			layout, err := parsed.ResolveEtcdStorageLayout()
			if err != nil || layout.RootPrefix == "" {
				t.Fatal("target layout not installed")
			}
			value, err := lab.admin.Get(ctx, layout.RootPrefix+"registry/test/value")
			if err != nil || len(value.Kvs) != 1 || string(value.Kvs[0].Value) != "before" {
				t.Fatal("copied/recovered value wrong")
			}
			rotated, err := host.CoreV1().Secrets(namespace).Get(ctx, tlsSecret.Name, metav1.GetOptions{})
			if err != nil || rotated.UID != tlsSecret.UID || string(rotated.Data["tls.crt"]) == string(cert) {
				t.Fatal("TLS rotation failed or replaced Secret UID")
			}
			parsed.HostClient = host
			parsed.HostNamespace = namespace
			// Use rotated files for an actual runtime ownership/startup check.
			dir := t.TempDir()
			for item, b := range rotated.Data {
				writeFixture(t, filepath.Join(dir, item), b)
			}
			parsed.ControlPlane.BackingStore.Etcd.External.TLS.CaFile = filepath.Join(dir, "ca.crt")
			parsed.ControlPlane.BackingStore.Etcd.External.TLS.CertFile = filepath.Join(dir, "tls.crt")
			parsed.ControlPlane.BackingStore.Etcd.External.TLS.KeyFile = filepath.Join(dir, "tls.key")
			if err := setupconfig.EnsureStorageLayout(ctx, parsed); err != nil {
				t.Fatal("post-cutover startup refused", err)
			}
			if mode == "recover" {
				// Reusing the same backup in a later operation must not adopt the previous
				// completion receipt, which belongs to a different recovery identity.
				if _, err := lab.admin.Put(ctx, layout.RootPrefix+"registry/test/value", "changed-again"); err != nil {
					t.Fatal(err)
				}
				if out, err := run("--new-operation", "--commit"); err != nil {
					t.Fatalf("same-archive subsequent recovery failed: %v: %s", err, out)
				}
			}
			if mode == "migrate" {
				nextArchive := filepath.Join(t.TempDir(), "next.gz")
				if _, err := tenantbackup.Export(ctx, lab.admin, layout.RootPrefix, nextArchive); err != nil {
					t.Fatal(err)
				}
				if _, err := lab.admin.Put(ctx, layout.RootPrefix+"registry/test/value", "changed"); err != nil {
					t.Fatal(err)
				}
				args[2] = "tenant-recover"
				args[3] = nextArchive
				if out, err := run("--new-operation", "--commit"); err != nil {
					t.Fatalf("recovery after migration failed: %v: %s", err, out)
				}
				restored, err := lab.admin.Get(ctx, layout.RootPrefix+"registry/test/value")
				if err != nil || len(restored.Kvs) != 1 || string(restored.Kvs[0].Value) != "before" {
					t.Fatal("subsequent operation did not restore data")
				}
			}

		})
	}
}
