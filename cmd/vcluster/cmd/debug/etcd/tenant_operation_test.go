package etcd

import (
	"errors"
	"slices"
	"strings"
	"testing"

	setupconfig "github.com/loft-sh/vcluster/pkg/setup/config"
	"github.com/loft-sh/vcluster/pkg/tenantbackup"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
)

func TestCutoverResumesAfterMarkerDeletion(t *testing.T) {
	host, owner := stoppedFixture()
	ctx := t.Context()
	cfg, err := host.CoreV1().Secrets("host").Get(ctx, "vc-config-test", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	original := []byte("controlPlane:\n  backingStore:\n    etcd:\n      external:\n        endpoint: example.invalid:2379\n        storePrefix: \"\"\n")
	cfg.Data = map[string][]byte{"config.yaml": original}
	if _, err := host.CoreV1().Secrets("host").Update(ctx, cfg, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	old := owner
	old.Root = ""
	old.Endpoint = ""
	marker, err := host.CoreV1().ConfigMaps("host").Get(ctx, setupconfig.StorageLayoutMarkerName("test"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	marker.Data = map[string]string{"version": "1", "rootPrefix": "", "deploymentUID": old.DeploymentUID}
	if _, err := host.CoreV1().ConfigMaps("host").Update(ctx, marker, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	tlsData := map[string][]byte{"ca.crt": []byte("ca"), "tls.crt": []byte("original"), "tls.key": []byte("original-key")}
	if _, err := host.CoreV1().Secrets("host").Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "dedicated", UID: "tls-uid"}, Data: tlsData}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	p := &operation{host: host, secret: &corev1.Secret{Data: map[string][]byte{"ca.crt": []byte("ca"), "tls.crt": []byte("new"), "tls.key": []byte("new-key")}}, s: operationState{Mode: "migrate", Phase: "cutover", Namespace: "host", Name: "test", Source: old, Target: owner, Config: original, TLSName: "dedicated", TLSUID: "tls-uid", OriginalTLS: tlsData, CAItem: "ca.crt", CertItem: "tls.crt", KeyItem: "tls.key", Kind: "Deployment", WorkloadUID: "controller"}}
	failed := false
	host.PrependReactor("create", "configmaps", func(a ktesting.Action) (bool, runtime.Object, error) {
		if !failed {
			failed = true
			return true, nil, errors.New("injected marker creation outage")
		}
		return false, nil, nil
	})
	if err := p.cutover(ctx); err == nil {
		t.Fatal("expected cutover failure")
	}
	if err := p.stopped(ctx, true); err != nil {
		t.Fatal("partial cutover cannot resume", err)
	}
	if err := p.cutover(ctx); err != nil {
		t.Fatal("cutover resume", err)
	}
	marker, err = host.CoreV1().ConfigMaps("host").Get(ctx, setupconfig.StorageLayoutMarkerName("test"), metav1.GetOptions{})
	if err != nil || !markerMatches(marker, owner) {
		t.Fatal("target marker missing")
	}
	if err := p.rollback(ctx); err == nil || !strings.Contains(err.Error(), "before cutover") {
		t.Fatal("post-cutover rollback accepted", err)
	}
}
func TestTargetConfigPreservesOtherValues(t *testing.T) {
	p := &operation{s: operationState{Mode: "migrate", Target: tenantbackup.Ownership{Root: "/tenants/test/"}, Config: []byte("custom: retained\ncontrolPlane:\n  backingStore:\n    etcd:\n      external:\n        endpoint: example.invalid:2379\n")}}
	target, err := p.targetConfig()
	if err != nil || !strings.Contains(string(target), "custom: retained") || !strings.Contains(string(target), "storePrefix: /tenants/test") {
		t.Fatal("configuration lost values", err)
	}
}

// Existing scoped legacy deployments disable global maintenance explicitly.
// Tenant roots manage the same settings, so cutover must consume those zero
// settings instead of carrying reserved overrides into the target config.
func TestTargetConfigConsumesDisabledMaintenance(t *testing.T) {
	original := []byte("custom: retained\ncontrolPlane:\n  distro:\n    k8s:\n      apiServer:\n        extraArgs:\n        - --secure-port=7443\n        - --etcd-compaction-interval=0\n        - --etcd_db_metric_poll_interval\n        - 0s\n  backingStore:\n    etcd:\n      external:\n        endpoint: example.invalid:2379\n")
	p := &operation{s: operationState{Mode: "migrate", Target: tenantbackup.Ownership{Root: "/tenants/test/"}, Config: original}}
	out, err := p.targetConfig()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "etcd-compaction") || strings.Contains(string(out), "etcd_db_metric") || !strings.Contains(string(out), "--secure-port=7443") || !strings.Contains(string(out), "custom: retained") {
		t.Fatal("migration did not consume disabled maintenance while preserving unrelated options")
	}
	p.s.Mode = "recover"
	out, err = p.targetConfig()
	if err != nil || string(out) != string(original) {
		t.Fatal("recovery must preserve its configuration exactly", err)
	}
}

func TestMigrationMaintenanceArguments(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want []string
		bad  bool
	}{
		{name: "absent", args: []string{"--secure-port=7443"}, want: []string{"--secure-port=7443"}},
		{name: "disabled aliases", args: []string{"--secure-port", "7443", "-etcd_compaction_interval=0s", "--etcd-db-metric-poll-interval", "0"}, want: []string{"--secure-port", "7443"}},
		{name: "nonzero", args: []string{"--etcd-compaction-interval=1s"}, bad: true},
		{name: "negative", args: []string{"--etcd-compaction-interval=-1s"}, bad: true},
		{name: "missing", args: []string{"--etcd-compaction-interval"}, bad: true},
		{name: "next flag", args: []string{"--etcd-compaction-interval", "--etcd-prefix=/other"}, bad: true},
		{name: "prefix", args: []string{"--etcd_prefix=/other"}, bad: true},
		{name: "server", args: []string{"--etcd-servers=other:2379"}, bad: true},
		{name: "unknown", args: []string{"--etcd-unknown=0"}, bad: true},
		{name: "later override", args: []string{"--etcd-compaction-interval=0", "--etcd-compaction-interval=1m"}, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := migrationAPIServerArgs(tc.args)
			if (err != nil) != tc.bad {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tc.bad && !slices.Equal(got, tc.want) {
				t.Fatalf("filtered options: %v", got)
			}
		})
	}
}

func TestTargetConfigPreservesNullExtraArgs(t *testing.T) {
	p := &operation{s: operationState{Mode: "migrate", Target: tenantbackup.Ownership{Root: "/tenants/test/"}, Config: []byte("controlPlane:\n  distro:\n    k8s:\n      apiServer:\n        extraArgs: null\n")}}
	if _, err := p.targetConfig(); err != nil {
		t.Fatal(err)
	}
}
