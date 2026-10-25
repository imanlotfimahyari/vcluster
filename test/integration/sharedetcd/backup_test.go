//go:build integration && linux

package sharedetcd_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	setupconfig "github.com/loft-sh/vcluster/pkg/setup/config"
	"github.com/loft-sh/vcluster/pkg/tenantbackup"
	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func claimBackupTenant(t *testing.T, lab *etcdLab, name string) tenantbackup.Ownership {
	t.Helper()
	owner := tenantbackup.Ownership{Version: "1", Root: "/tenants/" + name + "/", DeploymentUID: "disposable-" + name, Endpoint: strings.TrimPrefix(lab.endpoint, "https://")}
	value, _ := json.Marshal(owner)
	if _, err := lab.admin.Put(t.Context(), owner.Root+tenantbackup.ClaimSuffix, string(value)); err != nil {
		t.Fatal(err)
	}
	return owner
}

func recoveryIdentity(t *testing.T, lab *etcdLab, root, name string) *clientv3.Client {
	t.Helper()
	ctx := t.Context()
	if _, err := lab.admin.UserAddWithOptions(ctx, name, "", &clientv3.UserAddOptions{NoPassword: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := lab.admin.RoleAdd(ctx, name); err != nil {
		t.Fatal(err)
	}
	if _, err := lab.admin.RoleGrantPermission(ctx, name, root, clientv3.GetPrefixRangeEnd(root), clientv3.PermissionType(clientv3.PermReadWrite)); err != nil {
		t.Fatal(err)
	}
	if _, err := lab.admin.UserGrantRole(ctx, name, name); err != nil {
		t.Fatal(err)
	}
	return rawEtcdClient(t, lab.endpoint, lab.ca.identity(t, name, false))
}

type snapshotMutation struct {
	clientv3.KV
	mutate func()
	pages  int
}

func (s *snapshotMutation) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	r, err := s.KV.Get(ctx, key, opts...)
	if strings.HasSuffix(key, tenantbackup.ClaimSuffix) {
		return r, err
	}
	s.pages++
	if s.pages == 1 && s.mutate != nil {
		s.mutate()
	}
	return r, err
}

func TestTenantBackupFixedRevisionAndValidation(t *testing.T) {
	lab := newEtcdLab(t)
	owner := claimBackupTenant(t, lab, "tenant-a")
	raw := rawEtcdClient(t, lab.endpoint, lab.certs["tenant-a"])
	for i := 0; i < 90; i++ {
		if _, err := raw.Put(t.Context(), fmt.Sprintf("%sregistry/test/%03d", owner.Root, i), "before"); err != nil {
			t.Fatal(err)
		}
	}
	wrapped := &snapshotMutation{KV: raw, mutate: func() {
		if _, err := raw.Put(t.Context(), owner.Root+"registry/test/089", "after"); err != nil {
			t.Fatal(err)
		}
		if _, err := raw.Put(t.Context(), owner.Root+"mappings/late", "late"); err != nil {
			t.Fatal(err)
		}
	}}
	file := filepath.Join(t.TempDir(), "backup.gz")
	summary, err := tenantbackup.Export(t.Context(), wrapped, owner.Root, file)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Entries != 90 || wrapped.pages < 3 {
		t.Fatalf("pagination not exercised: %+v, pages=%d", summary, wrapped.pages)
	}
	inspected, err := tenantbackup.Inspect(file)
	if err != nil || *inspected != *summary {
		t.Fatalf("archive validation: %v", err)
	}
	info, err := os.Stat(file)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("archive is not private")
	}
	before, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tenantbackup.Export(t.Context(), raw, owner.Root, file); err == nil {
		t.Fatal("existing archive overwritten")
	}
	after, _ := os.ReadFile(file)
	if !bytes.Equal(before, after) {
		t.Fatal("existing archive changed")
	}
	if _, err := raw.Put(t.Context(), owner.Root+tenantbackup.RestoreSuffix, "pending"); err != nil {
		t.Fatal(err)
	}
	failed := filepath.Join(t.TempDir(), "blocked.gz")
	if _, err := tenantbackup.Export(t.Context(), raw, owner.Root, failed); err == nil {
		t.Fatal("backup accepted active recovery")
	}
	if _, err := os.Stat(failed); !os.IsNotExist(err) {
		t.Fatal("failed backup published partial archive")
	}
}

func TestTenantRecoveryFencingResumeAndIsolation(t *testing.T) {
	lab := newEtcdLab(t)
	owner := claimBackupTenant(t, lab, "tenant-a")
	old := rawEtcdClient(t, lab.endpoint, lab.certs["tenant-a"])
	other := rawEtcdClient(t, lab.endpoint, lab.certs["tenant-b"])
	expected := map[string]string{}
	for _, prefix := range []string{"registry/test/", "mappings/test/"} {
		for i := 0; i < 70; i++ {
			key := fmt.Sprintf("%s%s%03d", owner.Root, prefix, i)
			value := string([]byte{byte(i), 0, 0xff})
			expected[key] = value
			if _, err := old.Put(t.Context(), key, value); err != nil {
				t.Fatal(err)
			}
		}
	}
	file := filepath.Join(t.TempDir(), "backup.gz")
	summary, err := tenantbackup.Export(t.Context(), old, owner.Root, file)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.Put(t.Context(), owner.Root+"registry/extra", "delete-on-restore"); err != nil {
		t.Fatal(err)
	}
	otherArchive := filepath.Join(t.TempDir(), "different.gz")
	if _, err := tenantbackup.Export(t.Context(), old, owner.Root, otherArchive); err != nil {
		t.Fatal(err)
	}
	recovery := recoveryIdentity(t, lab, owner.Root, "recovery-a")
	calls := 0
	opts := tenantbackup.RestoreOptions{Admin: lab.admin, RecoveryUser: "recovery-a", RetiredUsers: []string{"tenant-a"}, CheckStopped: func(ctx context.Context) error {
		calls++
		_, err := other.Put(ctx, "/tenants/tenant-b/registry/progress", fmt.Sprint(calls))
		return err
	}}
	// Merely stopping Pods is insufficient while a stale client remains valid.
	if _, err := tenantbackup.Restore(t.Context(), recovery, file, opts); err == nil || !strings.Contains(err.Error(), "still write") {
		t.Fatalf("unfenced restore accepted: %v", err)
	}
	if _, err := lab.admin.UserRevokeRole(t.Context(), "tenant-a", "tenant-a"); err != nil {
		t.Fatal(err)
	}
	_, err = old.Put(t.Context(), owner.Root+"registry/stale", "must-fail")
	assertPermissionDenied(t, err)
	// Wrong destination ownership must fail before a recovery barrier or delete.
	changed := owner
	changed.DeploymentUID = "replacement-deployment"
	payload, _ := json.Marshal(changed)
	if _, err := lab.admin.Put(t.Context(), owner.Root+tenantbackup.ClaimSuffix, string(payload)); err != nil {
		t.Fatal(err)
	}
	if _, err := tenantbackup.Restore(t.Context(), recovery, file, opts); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("wrong destination accepted: %v", err)
	}
	payload, _ = json.Marshal(owner)
	if _, err := lab.admin.Put(t.Context(), owner.Root+tenantbackup.ClaimSuffix, string(payload)); err != nil {
		t.Fatal(err)
	}
	// Simulate a lost successful first-copy transaction response. Resume must
	// observe the persisted checkpoint rather than clear the tenant again.
	flaky := &lostCommitKV{KV: recovery, failAt: 3}
	if _, err := tenantbackup.Restore(t.Context(), flaky, file, opts); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("expected interrupted restore: %v", err)
	}
	pending, err := recovery.Get(t.Context(), owner.Root+tenantbackup.RestoreSuffix)
	if err != nil || len(pending.Kvs) != 1 {
		t.Fatal("interruption did not retain startup barrier")
	}
	cfg := lab.config(t, "tenant-a")
	cfg.ControlPlane.BackingStore.Etcd.External.TLS.CaFile = lab.ca.path
	certs := lab.ca.identity(t, "recovery-a", false)
	cfg.ControlPlane.BackingStore.Etcd.External.TLS.CertFile = certs.ServerCert
	cfg.ControlPlane.BackingStore.Etcd.External.TLS.KeyFile = certs.ServerKey
	immutable := true
	cfg.HostClient = fake.NewSimpleClientset(
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "vc-config-tenant-a", Namespace: "tenant-a", UID: types.UID(owner.DeploymentUID)}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: setupconfig.StorageLayoutMarkerName("tenant-a"), Namespace: "tenant-a"}, Immutable: &immutable, Data: map[string]string{"version": "1", "rootPrefix": owner.Root, "deploymentUID": owner.DeploymentUID, "etcdEndpoint": owner.Endpoint}},
	)
	if err := setupconfig.EnsureStorageLayout(t.Context(), cfg); err == nil || !strings.Contains(err.Error(), "barrier") {
		t.Fatalf("runtime bypassed recovery barrier: %v", err)
	}
	if _, err := tenantbackup.Restore(t.Context(), recovery, file, opts); err == nil {
		t.Fatal("resume flag was not required")
	}
	opts.Resume = true
	if _, err := tenantbackup.Restore(t.Context(), recovery, otherArchive, opts); err == nil || !strings.Contains(err.Error(), "checkpoint") {
		t.Fatalf("wrong resume archive accepted: %v", err)
	}
	// Regranted or unknown writers prevent continuation before any next batch.
	if _, err := lab.admin.UserGrantRole(t.Context(), "tenant-a", "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := tenantbackup.Restore(t.Context(), recovery, file, opts); err == nil {
		t.Fatal("resumed restore ignored revived writer")
	}
	if _, err := lab.admin.UserRevokeRole(t.Context(), "tenant-a", "tenant-a"); err != nil {
		t.Fatal(err)
	}
	result, err := tenantbackup.Restore(t.Context(), recovery, file, opts)
	if err != nil {
		t.Fatal(err)
	}
	if result.SHA256 != summary.SHA256 {
		t.Fatal("resume returned different archive")
	}
	if err := setupconfig.EnsureStorageLayout(t.Context(), cfg); err != nil {
		t.Fatalf("completed restore blocked startup: %v", err)
	}
	all, err := recovery.Get(t.Context(), owner.Root, clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(all.Kvs) != len(expected)+2 {
		t.Fatalf("restored key count %d", len(all.Kvs))
	}
	for _, item := range all.Kvs {
		if string(item.Key) == owner.Root+tenantbackup.ClaimSuffix || string(item.Key) == owner.Root+tenantbackup.RestoreReceiptSuffix {
			continue
		}
		if expected[string(item.Key)] != string(item.Value) {
			t.Fatal("restored raw value differs")
		}
		if item.ModRevision <= summary.Revision {
			t.Fatal("restore did not move revisions forward")
		}
	}
	progress, err := other.Get(t.Context(), "/tenants/tenant-b/registry/progress")
	if err != nil || len(progress.Kvs) != 1 {
		t.Fatal("other tenant stopped progressing")
	}
	_, err = recovery.Put(t.Context(), "/tenants/tenant-b/registry/foreign", "no")
	assertPermissionDenied(t, err)
	_, err = old.Put(t.Context(), owner.Root+"registry/stale", "no")
	assertPermissionDenied(t, err)
	// A second export has new MVCC metadata, but exactly the same object data.
	newFile := filepath.Join(t.TempDir(), "after.gz")
	newSummary, err := tenantbackup.Export(t.Context(), recovery, owner.Root, newFile)
	if err != nil {
		t.Fatal(err)
	}
	if newSummary.DataSHA256 != summary.DataSHA256 {
		t.Fatal("restored content digest differs")
	}
}

func TestTenantRecoveryAuthAuditRefusesUnsafeGrants(t *testing.T) {
	lab := newEtcdLab(t)
	root := "/tenants/tenant-a/"
	_ = recoveryIdentity(t, lab, root, "recovery-a")
	ctx := t.Context()
	if _, err := lab.admin.UserRevokeRole(ctx, "tenant-a", "tenant-a"); err != nil {
		t.Fatal(err)
	}
	audit := func() error {
		return tenantbackup.AuditWriters(ctx, lab.admin, root, "recovery-a", []string{"tenant-a"})
	}
	if err := audit(); err != nil {
		t.Fatal(err)
	}
	if _, err := lab.admin.RoleGrantPermission(ctx, "recovery-a", "/", "\x00", clientv3.PermissionType(clientv3.PermRead)); err != nil {
		t.Fatal(err)
	}
	if err := audit(); err == nil {
		t.Fatal("recovery identity with foreign read access accepted")
	}
	if _, err := lab.admin.RoleRevokePermission(ctx, "recovery-a", "/", "\x00"); err != nil {
		t.Fatal(err)
	}
	if _, err := lab.admin.UserGrantRole(ctx, "tenant-a", "root"); err != nil {
		t.Fatal(err)
	}
	if err := audit(); err == nil {
		t.Fatal("retired administrator accepted")
	}
	if _, err := lab.admin.UserRevokeRole(ctx, "tenant-a", "root"); err != nil {
		t.Fatal(err)
	}
	if _, err := lab.admin.UserGrantRole(ctx, "tenant-b", "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if err := audit(); err == nil {
		t.Fatal("unknown intersecting writer accepted")
	}
	if _, err := lab.admin.UserRevokeRole(ctx, "tenant-b", "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := lab.admin.AuthDisable(ctx); err != nil {
		t.Fatal(err)
	}
	if err := audit(); err == nil {
		t.Fatal("disabled etcd auth accepted")
	}
	if _, err := lab.admin.AuthEnable(ctx); err != nil {
		t.Fatal(err)
	}
	if err := audit(); err != nil {
		t.Fatal(err)
	}
}

func TestTenantRecoveryLostCompletionResponse(t *testing.T) {
	lab := newEtcdLab(t)
	owner := claimBackupTenant(t, lab, "tenant-a")
	old := rawEtcdClient(t, lab.endpoint, lab.certs["tenant-a"])
	if _, err := old.Put(t.Context(), owner.Root+"registry/test", "original"); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "backup.gz")
	if _, err := tenantbackup.Export(t.Context(), old, owner.Root, file); err != nil {
		t.Fatal(err)
	}
	recovery := recoveryIdentity(t, lab, owner.Root, "recovery-a")
	if _, err := lab.admin.UserRevokeRole(t.Context(), "tenant-a", "tenant-a"); err != nil {
		t.Fatal(err)
	}
	flaky := &lostCommitKV{KV: recovery, failAt: 4}
	opts := tenantbackup.RestoreOptions{Admin: lab.admin, RecoveryUser: "recovery-a", RetiredUsers: []string{"tenant-a"}, CheckStopped: func(context.Context) error { return nil }}
	if _, err := tenantbackup.Restore(t.Context(), flaky, file, opts); err == nil {
		t.Fatal("injected lost completion response was not exercised")
	}
	opts.Resume = true
	if _, err := tenantbackup.Restore(t.Context(), flaky, file, opts); err != nil {
		t.Fatalf("completed restore could not be confirmed after lost response: %v", err)
	}
	if flaky.calls != 4 {
		t.Fatal("completion confirmation started another destructive restore")
	}
	if _, err := recovery.Put(t.Context(), owner.Root+"registry/test", "later-change"); err != nil {
		t.Fatal(err)
	}
	if _, err := tenantbackup.Restore(t.Context(), flaky, file, opts); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("stale completion receipt accepted changed data: %v", err)
	}
	if flaky.calls != 4 {
		t.Fatal("stale completion confirmation changed data")
	}
}

type lostCommitKV struct {
	clientv3.KV
	calls, failAt int
	rangeDeletes  int
}

func (k *lostCommitKV) Txn(ctx context.Context) clientv3.Txn {
	return &lostCommitTxn{inner: k.KV.Txn(ctx), owner: k}
}

type lostCommitTxn struct {
	inner clientv3.Txn
	owner *lostCommitKV
}

func (t *lostCommitTxn) If(c ...clientv3.Cmp) clientv3.Txn { t.inner = t.inner.If(c...); return t }
func (t *lostCommitTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	for _, op := range ops {
		if op.IsDelete() && len(op.RangeBytes()) > 0 {
			t.owner.rangeDeletes++
		}
	}
	t.inner = t.inner.Then(ops...)
	return t
}
func (t *lostCommitTxn) Else(ops ...clientv3.Op) clientv3.Txn {
	t.inner = t.inner.Else(ops...)
	return t
}
func (t *lostCommitTxn) Commit() (*clientv3.TxnResponse, error) {
	r, err := t.inner.Commit()
	t.owner.calls++
	if err == nil && t.owner.calls == t.owner.failAt {
		return nil, errors.New("injected lost committed response")
	}
	return r, err
}
