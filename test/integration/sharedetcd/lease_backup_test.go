//go:build integration && linux

package sharedetcd_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/loft-sh/vcluster/pkg/tenantbackup"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/apimachinery/pkg/util/wait"
)

func TestTenantLeasedBackup(t *testing.T) {
	lab := newEtcdLab(t)
	owner := claimBackupTenant(t, lab, "tenant-a")
	old := rawEtcdClient(t, lab.endpoint, lab.certs["tenant-a"])
	lease, err := old.Grant(t.Context(), 60)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 85; i++ {
		key := fmt.Sprintf("registry/events/default/%03d", i)
		if _, err := old.Put(t.Context(), owner.Root+key, "leased-value", clientv3.WithLease(lease.ID)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := old.Put(t.Context(), owner.Root+"mappings/persistent", "persistent-value"); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "leased.gz")
	summary, err := tenantbackup.Export(t.Context(), old, owner.Root, file)
	if err != nil {
		t.Fatalf("ordinary TTL-backed Kubernetes data cannot be backed up: %v", err)
	}
	if summary.Entries != 86 || summary.Version != 2 || summary.LeasedEntries != 85 || summary.LeaseGroups != 1 {
		t.Fatal("leased records were silently dropped")
	}
	inspected, err := tenantbackup.Inspect(file)
	if err != nil || *inspected != *summary {
		t.Fatalf("leased archive inspection failed: %v", err)
	}
	recovery := recoveryIdentity(t, lab, owner.Root, "recovery-a")
	if _, err := lab.admin.UserRevokeRole(t.Context(), "tenant-a", "tenant-a"); err != nil {
		t.Fatal(err)
	}
	checks := 0
	opts := tenantbackup.RestoreOptions{Admin: lab.admin, RecoveryUser: "recovery-a", RetiredUsers: []string{"tenant-a"}, Leases: recovery.Lease, CheckStopped: func(context.Context) error { checks++; return nil }}
	if _, err := tenantbackup.Restore(t.Context(), recovery, file, opts); err == nil || !strings.Contains(err.Error(), "restart-lease-ttl") {
		t.Fatalf("leased restore did not require explicit expiry policy: %v", err)
	}
	if checks != 0 {
		t.Fatal("lease policy refusal contacted host or mutated destination")
	}
	opts.RestartLeaseTTL = true
	flaky := &lostCommitKV{KV: recovery, failAt: 4}
	if _, err := tenantbackup.Restore(t.Context(), flaky, file, opts); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("lost leased copy response was not exercised: %v", err)
	}
	pending, err := recovery.Get(t.Context(), owner.Root+tenantbackup.RestoreSuffix)
	if err != nil || len(pending.Kvs) != 1 {
		t.Fatal("missing leased restore checkpoint")
	}
	var checkpoint struct {
		Next   int `json:"next"`
		Leases map[string]struct {
			Destination int64 `json:"destination"`
		} `json:"leases"`
	}
	if err := json.Unmarshal(pending.Kvs[0].Value, &checkpoint); err != nil || checkpoint.Next != 32 || len(checkpoint.Leases) != 1 {
		t.Fatalf("invalid lease checkpoint: %v", err)
	}
	var interruptedLease clientv3.LeaseID
	for _, binding := range checkpoint.Leases {
		interruptedLease = clientv3.LeaseID(binding.Destination)
	}
	if interruptedLease == lease.ID || interruptedLease == 0 {
		t.Fatal("restore reused the source lease")
	}
	// Stock etcd lease keep-alive is not range-authorized. This deliberately
	// records the shared-service limit; revoked KV writes are checked separately.
	if _, err = old.KeepAliveOnce(t.Context(), interruptedLease); err != nil {
		t.Fatalf("stock lease behavior changed; revisit the documented limit: %v", err)
	}
	_, err = old.Put(t.Context(), owner.Root+"registry/stale", "denied")
	assertPermissionDenied(t, err)
	// Expiring an interrupted attempt's lease removes some committed values.
	// Resume must recreate the group and replay values, without clearing again.
	if _, err := recovery.Revoke(t.Context(), interruptedLease); err != nil {
		t.Fatal(err)
	}
	opts.Resume = true
	if _, err := tenantbackup.Restore(t.Context(), flaky, file, opts); err != nil {
		t.Fatalf("expired leased restore could not resume: %v", err)
	}
	if flaky.rangeDeletes != 2 {
		t.Fatalf("resume repeated destructive range deletes: %d", flaky.rangeDeletes)
	}
	data, err := recovery.Get(t.Context(), owner.Root+"registry/events/", clientv3.WithPrefix())
	if err != nil || len(data.Kvs) != 85 {
		t.Fatal("resume did not restore expired leased values")
	}
	restoredLease := data.Kvs[0].Lease
	if restoredLease == 0 || restoredLease == int64(lease.ID) || restoredLease == int64(interruptedLease) {
		t.Fatal("expired/source lease was reused")
	}
	for _, item := range data.Kvs {
		if item.Lease != restoredLease || string(item.Value) != "leased-value" {
			t.Fatal("shared lease grouping or value changed")
		}
	}
	persistent, err := recovery.Get(t.Context(), owner.Root+"mappings/persistent")
	if err != nil || len(persistent.Kvs) != 1 || persistent.Kvs[0].Lease != 0 {
		t.Fatal("persistent value acquired a lease")
	}
	if _, err := tenantbackup.Restore(t.Context(), recovery, file, opts); err != nil {
		t.Fatalf("leased completion could not be confirmed: %v", err)
	}
}

func TestRestoredTenantLeaseExpiresAfterSuccess(t *testing.T) {
	lab := newEtcdLab(t)
	owner := claimBackupTenant(t, lab, "tenant-a")
	old := rawEtcdClient(t, lab.endpoint, lab.certs["tenant-a"])
	lease, err := old.Grant(t.Context(), 6)
	if err != nil {
		t.Fatal(err)
	}
	key := owner.Root + "registry/events/default/expiry"
	if _, err := old.Put(t.Context(), key, "temporary", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatal(err)
	}
	if _, err := old.Put(t.Context(), owner.Root+"registry/persistent", "retain"); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "expiry.gz")
	if _, err := tenantbackup.Export(t.Context(), old, owner.Root, file); err != nil {
		t.Fatal(err)
	}
	recovery := recoveryIdentity(t, lab, owner.Root, "recovery-a")
	if _, err := lab.admin.UserRevokeRole(t.Context(), "tenant-a", "tenant-a"); err != nil {
		t.Fatal(err)
	}
	opts := tenantbackup.RestoreOptions{Admin: lab.admin, RecoveryUser: "recovery-a", RetiredUsers: []string{"tenant-a"}, CheckStopped: func(context.Context) error { return nil }, Leases: recovery.Lease, RestartLeaseTTL: true}
	if _, err := tenantbackup.Restore(t.Context(), recovery, file, opts); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
		response, err := recovery.Get(ctx, key)
		return err == nil && len(response.Kvs) == 0, err
	}); err != nil {
		t.Fatalf("recovery kept leased values alive after success: %v", err)
	}
	retained, err := recovery.Get(t.Context(), owner.Root+"registry/persistent")
	if err != nil || len(retained.Kvs) != 1 {
		t.Fatal("lease expiry removed persistent data")
	}
}

func TestTenantBackupRefusesForeignSharedLease(t *testing.T) {
	lab := newEtcdLab(t)
	owner := claimBackupTenant(t, lab, "tenant-a")
	raw := rawEtcdClient(t, lab.endpoint, lab.certs["tenant-a"])
	lease, err := lab.admin.Grant(t.Context(), 60)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{owner.Root + "registry/events/a", "/tenants/tenant-b/registry/events/b"} {
		if _, err := lab.admin.Put(t.Context(), key, "fixture", clientv3.WithLease(lease.ID)); err != nil {
			t.Fatal(err)
		}
	}
	file := filepath.Join(t.TempDir(), "foreign.gz")
	if _, err := tenantbackup.Export(t.Context(), raw, owner.Root, file); err == nil {
		t.Fatal("foreign shared lease was exported with scoped credentials")
	}
}
