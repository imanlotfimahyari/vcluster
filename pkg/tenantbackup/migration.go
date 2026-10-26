package tenantbackup

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const LegacyMigrationKey = "/vcluster/migration-state"

type MigrationGuard struct {
	Token  string
	Access *AccessPlan
}

func migrationClaim(owner Ownership) string { b, _ := json.Marshal(owner); return string(b) }

// ReserveMigration atomically reserves an empty target and the single legacy
// source. Both barriers survive process failure. The source is never deleted.
func ReserveMigration(ctx context.Context, kv clientv3.KV, owner Ownership, token string, resume bool) error {
	root, err := NormalizeRoot(owner.Root)
	if err != nil || root != owner.Root || token == "" {
		return errors.New("invalid migration reservation")
	}
	h := Header{Type: "header", Version: 2, Root: root, Revision: 1, ClusterID: 1, Ownership: owner}
	if err := validateHeader(h); err != nil {
		return err
	}
	claim := migrationClaim(owner)
	if resume {
		r, err := kv.Txn(ctx).If(clientv3.Compare(clientv3.Value(LegacyMigrationKey), "=", token), clientv3.Compare(clientv3.Value(root+MigrationSuffix), "=", token), clientv3.Compare(clientv3.Value(root+ClaimSuffix), "=", claim)).Then(clientv3.OpGet(root + ClaimSuffix)).Commit()
		if err != nil {
			return err
		}
		if !r.Succeeded {
			return errors.New("migration reservation does not match operation")
		}
		return nil
	}
	r, err := kv.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision(LegacyMigrationKey), "=", 0), clientv3.Compare(clientv3.CreateRevision(root), "=", 0).WithPrefix()).Then(clientv3.OpPut(LegacyMigrationKey, token), clientv3.OpPut(root+MigrationSuffix, token), clientv3.OpPut(root+ClaimSuffix, claim)).Commit()
	if err != nil {
		return err
	}
	if !r.Succeeded {
		return errors.New("legacy source or target is already reserved/nonempty")
	}
	return nil
}

// legacyExportView translates exactly the two supported source ranges into the
// archive's target-relative ordering, preserving fixed-revision pagination.
// No arbitrary prefix relocation or Kubernetes identity transformation occurs.
type legacyExportView struct {
	clientv3.KV
	lease       leaseReader
	root, token string
	owner       Ownership
}

var legacyRanges = [][2]string{{"mappings/", "/vcluster/mappings/"}, {"registry/", "/registry/"}}

func (v *legacyExportView) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if key == v.root+ClaimSuffix {
		r, err := v.KV.Get(ctx, LegacyMigrationKey)
		if err != nil {
			return nil, err
		}
		if len(r.Kvs) != 1 || string(r.Kvs[0].Value) != v.token {
			return nil, errors.New("legacy migration barrier changed")
		}
		return &clientv3.GetResponse{Header: r.Header, Kvs: []*mvccpb.KeyValue{{Key: []byte(key), Value: []byte(migrationClaim(v.owner))}}, Count: 1}, nil
	}
	op := clientv3.OpGet(key, opts...)
	if !strings.HasPrefix(key, v.root) || string(op.RangeBytes()) != clientv3.GetPrefixRangeEnd(v.root) || op.Rev() <= 0 || op.Limit() != 32 {
		return nil, errors.New("unsupported legacy export request")
	}
	out := &clientv3.GetResponse{}
	for _, p := range legacyRanges {
		target := v.root + p[0]
		if key >= clientv3.GetPrefixRangeEnd(target) {
			continue
		}
		start := p[1]
		if key > target {
			start = p[1] + strings.TrimPrefix(key, target)
		}
		r, err := v.KV.Get(ctx, start, clientv3.WithRange(clientv3.GetPrefixRangeEnd(p[1])), clientv3.WithRev(op.Rev()), clientv3.WithLimit(33-int64(len(out.Kvs))), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		if err != nil {
			return nil, err
		}
		out.Header = r.Header
		for _, item := range r.Kvs {
			copy := *item
			copy.Key = []byte(target + strings.TrimPrefix(string(item.Key), p[1]))
			out.Kvs = append(out.Kvs, &copy)
		}
		if len(out.Kvs) > 32 || r.More {
			out.More = true
			break
		}
	}
	if len(out.Kvs) > 32 {
		out.Kvs = out.Kvs[:32]
	}
	out.Count = int64(len(out.Kvs))
	return out, nil
}
func (v *legacyExportView) TimeToLive(ctx context.Context, id clientv3.LeaseID, opts ...clientv3.LeaseOption) (*clientv3.LeaseTimeToLiveResponse, error) {
	if v.lease == nil {
		return nil, errors.New("legacy leased data requires a lease-aware source")
	}
	r, err := v.lease.TimeToLive(ctx, id, opts...)
	if err != nil {
		return nil, err
	}
	keys := make([][]byte, 0, len(r.Keys))
	for _, key := range r.Keys {
		found := false
		for _, p := range legacyRanges {
			if strings.HasPrefix(string(key), p[1]) {
				keys = append(keys, []byte(v.root+p[0]+strings.TrimPrefix(string(key), p[1])))
				found = true
				break
			}
		}
		if !found {
			return nil, errors.New("legacy lease includes unrelated data")
		}
	}
	copy := *r
	copy.Keys = keys
	return &copy, nil
}
func ExportLegacy(ctx context.Context, source clientv3.KV, owner Ownership, token, destination string) (*Summary, error) {
	lease, _ := source.(leaseReader)
	return Export(ctx, &legacyExportView{KV: source, lease: lease, root: owner.Root, token: token, owner: owner}, owner.Root, destination)
}

// CopyMigration retains the migration barrier after the restore receipt is
// committed. Host configuration cutover must finish before it is released.
func CopyMigration(ctx context.Context, kv clientv3.KV, archive string, opts RestoreOptions, guard MigrationGuard) (*Summary, error) {
	if guard.Token == "" || guard.Access == nil || !guard.Access.Legacy {
		return nil, errors.New("migration guard is required")
	}
	sum, err := Inspect(archive)
	if err != nil {
		return nil, err
	}
	if sum.Root != guard.Access.Root || opts.RecoveryUser != guard.Access.Recovery {
		return nil, errors.New("migration archive and access plan differ")
	}
	opts.migration = &guard
	return Restore(ctx, kv, archive, opts)
}
func FinishMigration(ctx context.Context, kv clientv3.KV, owner Ownership, token string) error {
	r, err := kv.Txn(ctx).If(clientv3.Compare(clientv3.Value(LegacyMigrationKey), "=", token), clientv3.Compare(clientv3.Value(owner.Root+ClaimSuffix), "=", migrationClaim(owner)), clientv3.Compare(clientv3.Value(owner.Root+MigrationSuffix), "=", token), clientv3.Compare(clientv3.CreateRevision(owner.Root+RestoreSuffix), "=", 0), clientv3.Compare(clientv3.CreateRevision(owner.Root+RestoreReceiptSuffix), ">", 0)).Then(clientv3.OpDelete(owner.Root + MigrationSuffix)).Commit()
	if err != nil {
		return err
	}
	if !r.Succeeded {
		// An unknown completion response is safe to confirm without mutation.
		r, err = kv.Txn(ctx).If(clientv3.Compare(clientv3.Value(LegacyMigrationKey), "=", token), clientv3.Compare(clientv3.Value(owner.Root+ClaimSuffix), "=", migrationClaim(owner)), clientv3.Compare(clientv3.CreateRevision(owner.Root+MigrationSuffix), "=", 0), clientv3.Compare(clientv3.CreateRevision(owner.Root+RestoreSuffix), "=", 0), clientv3.Compare(clientv3.CreateRevision(owner.Root+RestoreReceiptSuffix), ">", 0)).Then(clientv3.OpGet(owner.Root + RestoreReceiptSuffix)).Commit()
		if err != nil {
			return err
		}
		if !r.Succeeded {
			return errors.New("migration completion guards changed")
		}
	}
	return nil
}

// AbortMigration removes only the reserved target and its matching source
// barrier. Call only while the host journal still excludes cutover/startup.
func AbortMigration(ctx context.Context, kv clientv3.KV, owner Ownership, token string) error {
	r, err := kv.Txn(ctx).If(clientv3.Compare(clientv3.Value(LegacyMigrationKey), "=", token), clientv3.Compare(clientv3.Value(owner.Root+MigrationSuffix), "=", token), clientv3.Compare(clientv3.Value(owner.Root+ClaimSuffix), "=", migrationClaim(owner))).Then(clientv3.OpDelete(owner.Root, clientv3.WithPrefix()), clientv3.OpDelete(LegacyMigrationKey)).Commit()
	if err != nil {
		return err
	}
	if !r.Succeeded {
		return errors.New("migration rollback guards changed")
	}
	return nil
}
