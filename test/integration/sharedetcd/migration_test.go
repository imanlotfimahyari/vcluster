//go:build integration && linux

package sharedetcd_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/loft-sh/vcluster/pkg/tenantbackup"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func legacyWriter(t *testing.T, lab *etcdLab) *clientv3.Client {
	t.Helper()
	ctx := t.Context()
	if _, err := lab.admin.RoleAdd(ctx, "legacy"); err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{"/registry", "/vcluster/mappings"} {
		if _, err := lab.admin.RoleGrantPermission(ctx, "legacy", root, clientv3.GetPrefixRangeEnd(root), clientv3.PermissionType(clientv3.PermReadWrite)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := lab.admin.UserAddWithOptions(ctx, "legacy", "", &clientv3.UserAddOptions{NoPassword: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := lab.admin.UserGrantRole(ctx, "legacy", "legacy"); err != nil {
		t.Fatal(err)
	}
	lab.certs["legacy"] = lab.ca.identity(t, "legacy", false)
	return rawEtcdClient(t, lab.endpoint, lab.certs["legacy"])
}
func TestOfflineLegacyMigrationResumeAndRollback(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(fmt.Sprint("rollback=", rollback), func(t *testing.T) {
			lab := newEtcdLab(t)
			ctx := t.Context()
			old := legacyWriter(t, lab)
			for i := 0; i < 75; i++ {
				if _, err := old.Put(ctx, fmt.Sprintf("/registry/resources/%03d", i), fmt.Sprint(i)); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := old.Put(ctx, "/vcluster/mappings/v1/pods/example", "mapping"); err != nil {
				t.Fatal(err)
			}
			lease, err := old.Grant(ctx, 120)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := old.Put(ctx, "/registry/events/ttl", "ephemeral", clientv3.WithLease(lease.ID)); err != nil {
				t.Fatal(err)
			}
			if _, err := lab.admin.Put(ctx, "/tenants/tenant-b/registry/sentinel", "untouched"); err != nil {
				t.Fatal(err)
			}
			owner := tenantbackup.Ownership{Version: "1", Root: "/tenants/migrated/", Endpoint: strings.TrimPrefix(lab.endpoint, "https://"), DeploymentUID: "legacy-config-uid"}
			plan, err := tenantbackup.PlanAccess(ctx, lab.admin, owner.Root, "migration-recovery", []string{"legacy"}, true)
			if err != nil {
				t.Fatal(err)
			}
			if err := tenantbackup.PrepareAccess(ctx, lab.admin, plan); err != nil {
				t.Fatal(err)
			}
			assertPermissionDenied(t, func() error { _, err := old.Put(ctx, "/registry/denied", "no"); return err }())
			if err := tenantbackup.ReserveMigration(ctx, lab.admin, owner, "operation-1", false); err != nil {
				t.Fatal(err)
			}
			other := owner
			other.DeploymentUID = "different"
			if err := tenantbackup.ReserveMigration(ctx, lab.admin, other, "operation-2", false); err == nil {
				t.Fatal("duplicate reservation accepted")
			}
			archive := filepath.Join(t.TempDir(), "migration.gz")
			sum, err := tenantbackup.ExportLegacy(ctx, lab.admin, owner, "operation-1", archive)
			if err != nil {
				t.Fatal(err)
			}
			if sum.Entries != 77 || sum.LeasedEntries != 1 {
				t.Fatalf("wrong translated archive counts: %+v", sum)
			}
			opts := tenantbackup.RestoreOptions{Admin: lab.admin, RecoveryUser: plan.Recovery, RetiredUsers: plan.Retired, CheckStopped: func(_ context.Context) error { return nil }, Leases: lab.admin.Lease, RestartLeaseTTL: true}
			// Commit a copy batch but lose its response. Resume must retain source,
			// reject a different archive/token and finish the same target safely.
			faulty := &lostCommitKV{KV: lab.admin, failAt: 4}
			guard := tenantbackup.MigrationGuard{Token: "operation-1", Access: plan}
			if _, err := tenantbackup.CopyMigration(ctx, faulty, archive, opts, guard); err == nil {
				t.Fatal("expected lost copy response")
			}
			opts.Resume = true
			wrong := guard
			wrong.Token = "other"
			if _, err := tenantbackup.CopyMigration(ctx, lab.admin, archive, opts, wrong); err == nil {
				t.Fatal("wrong migration token accepted")
			}
			if rollback {
				if err := tenantbackup.AbortMigration(ctx, lab.admin, owner, "operation-1"); err != nil {
					t.Fatal(err)
				}
				if err := tenantbackup.RollbackAccess(ctx, lab.admin, plan); err != nil {
					t.Fatal(err)
				}
				if _, err := old.Put(ctx, "/registry/after-rollback", "works"); err != nil {
					t.Fatal(err)
				}
				dst, err := lab.admin.Get(ctx, owner.Root, clientv3.WithPrefix())
				if err != nil || len(dst.Kvs) != 0 {
					t.Fatal("rollback left target data")
				}
			} else {
				if _, err := tenantbackup.CopyMigration(ctx, lab.admin, archive, opts, guard); err != nil {
					t.Fatal(err)
				}
				// Unknown completion confirmation must keep the migration barrier intact.
				if _, err := tenantbackup.CopyMigration(ctx, lab.admin, archive, opts, guard); err != nil {
					t.Fatal(err)
				}
				if err := tenantbackup.FinishMigration(ctx, lab.admin, owner, "operation-1"); err != nil {
					t.Fatal(err)
				}
				if err := tenantbackup.FinishMigration(ctx, lab.admin, owner, "operation-1"); err != nil {
					t.Fatal("idempotent finish", err)
				}
				if err := tenantbackup.AbortMigration(ctx, lab.admin, owner, "operation-1"); err == nil {
					t.Fatal("rollback after cutover accepted")
				}
				dst, err := lab.admin.Get(ctx, owner.Root+"registry/resources/020")
				if err != nil || len(dst.Kvs) != 1 || string(dst.Kvs[0].Value) != "20" {
					t.Fatal("destination data mismatch")
				}
				restored, err := lab.admin.Get(ctx, owner.Root+"registry/events/ttl")
				if err != nil || len(restored.Kvs) != 1 || restored.Kvs[0].Lease == int64(lease.ID) || restored.Kvs[0].Lease == 0 {
					t.Fatal("source lease reused or lost")
				}
			}
			src, err := lab.admin.Get(ctx, "/registry/resources/020")
			if err != nil || len(src.Kvs) != 1 || string(src.Kvs[0].Value) != "20" {
				t.Fatal("legacy source changed")
			}
			foreign, err := lab.admin.Get(ctx, "/tenants/tenant-b/registry/sentinel")
			if err != nil || len(foreign.Kvs) != 1 || string(foreign.Kvs[0].Value) != "untouched" {
				t.Fatal("foreign tenant changed")
			}
		})
	}
}
func TestAutomatedAccessRefusesCollateralRevocation(t *testing.T) {
	lab := newEtcdLab(t)
	ctx := t.Context()
	legacyWriter(t, lab)
	if _, err := lab.admin.RoleGrantPermission(ctx, "legacy", "/unrelated/", clientv3.GetPrefixRangeEnd("/unrelated/"), clientv3.PermissionType(clientv3.PermReadWrite)); err != nil {
		t.Fatal(err)
	}
	if _, err := tenantbackup.PlanAccess(ctx, lab.admin, "/tenants/migrated/", "fresh", []string{"legacy"}, true); err == nil {
		t.Fatal("broad legacy role accepted")
	}
	user, err := lab.admin.UserGet(ctx, "legacy")
	if err != nil || len(user.Roles) != 1 {
		t.Fatal("read-only planning changed permissions")
	}
	if _, err := tenantbackup.PlanAccess(ctx, lab.admin, "/tenants/tenant-b/", "fresh", []string{"legacy"}, false); err == nil {
		t.Fatal("unidentified tenant writer accepted")
	}
}
func TestLegacyMigrationRejectsForeignSharedLease(t *testing.T) {
	lab := newEtcdLab(t)
	old := legacyWriter(t, lab)
	ctx := t.Context()
	l, err := old.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"/registry/events/x", "/tenants/tenant-b/registry/x"} {
		if _, err := lab.admin.Put(ctx, key, "value", clientv3.WithLease(l.ID)); err != nil {
			t.Fatal(err)
		}
	}
	owner := tenantbackup.Ownership{Version: "1", Root: "/tenants/migrated/", Endpoint: strings.TrimPrefix(lab.endpoint, "https://"), DeploymentUID: "legacy-uid"}
	if err := tenantbackup.ReserveMigration(ctx, lab.admin, owner, "op", false); err != nil {
		t.Fatal(err)
	}
	if _, err := tenantbackup.ExportLegacy(ctx, lab.admin, owner, "op", filepath.Join(t.TempDir(), "bad.gz")); err == nil {
		t.Fatal("foreign shared lease accepted")
	}
}

func TestAccessPreparationChecksCredentialBeforeRevocation(t *testing.T) {
	lab := newEtcdLab(t)
	old := legacyWriter(t, lab)
	ctx := t.Context()
	p, err := tenantbackup.PlanAccess(ctx, lab.admin, "/tenants/migrated/", "fresh", []string{"legacy"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tenantbackup.PrepareAccessWithCheck(ctx, lab.admin, p, func() error { return errors.New("untrusted signing CA") }); err == nil {
		t.Fatal("unusable replacement identity accepted")
	}
	if _, err := old.Put(ctx, "/registry/still-authorized", "yes"); err != nil {
		t.Fatal("old writer retired before replacement authenticated", err)
	}
	// An interrupted provisioning attempt can resume without deleting users or
	// changing shared role definitions.
	if err := tenantbackup.PrepareAccess(ctx, lab.admin, p); err != nil {
		t.Fatal(err)
	}
	assertPermissionDenied(t, func() error { _, err := old.Put(ctx, "/registry/still-authorized", "no"); return err }())
	if err := tenantbackup.RollbackAccess(ctx, lab.admin, p); err != nil {
		t.Fatal(err)
	}
	if _, err := old.Put(ctx, "/registry/restored", "yes"); err != nil {
		t.Fatal(err)
	}
}
