package tenantbackup

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// RestoreOptions intentionally requires external evidence of quiescence and
// an independent administrator audit. A stopped Pod alone cannot fence a stale
// process retaining valid etcd credentials. Administrators must serialize auth
// changes throughout recovery; etcd auth and KV transactions are not atomic.
type RestoreOptions struct {
	Admin           clientv3.Auth
	RecoveryUser    string
	RetiredUsers    []string
	CheckStopped    func(context.Context) error
	Resume          bool
	TempDir         string
	Leases          clientv3.Lease
	RestartLeaseTTL bool
}

type restoreState struct {
	Version      int                     `json:"version"`
	Archive      string                  `json:"archive"`
	Owner        Ownership               `json:"owner"`
	RecoveryUser string                  `json:"recoveryUser"`
	Phase        string                  `json:"phase"`
	Next         int                     `json:"next"`
	Leases       map[string]leaseBinding `json:"leases,omitempty"`
}

// AuditWriters refuses restore unless auth is enabled, all retired identities
// are nonadministrators, and recoveryUser is the only nonadministrator with any
// write permission intersecting root. The recovery identity must have full
// read/write access to root and no permissions outside it.
func AuditWriters(ctx context.Context, admin clientv3.Auth, root, recoveryUser string, retired []string) error {
	if admin == nil || recoveryUser == "" || recoveryUser == "root" || len(retired) == 0 {
		return errors.New("independent auth audit and retired writer identities are required")
	}
	var err error
	root, err = NormalizeRoot(root)
	if err != nil {
		return err
	}
	retiredSet := map[string]bool{}
	for _, name := range retired {
		if name == "" || name == recoveryUser || name == "root" {
			return errors.New("recovery identity must differ from every retired nonadministrator writer")
		}
		retiredSet[name] = true
	}
	status, err := admin.AuthStatus(ctx)
	if err != nil {
		return fmt.Errorf("audit etcd auth status: %w", err)
	}
	if !status.Enabled {
		return errors.New("etcd authentication must be enabled")
	}
	users, err := admin.UserList(ctx)
	if err != nil {
		return fmt.Errorf("audit etcd users: %w", err)
	}
	fullAccess, seenRecovery := false, false
	for _, name := range users.Users {
		user, err := admin.UserGet(ctx, name)
		if err != nil {
			return fmt.Errorf("audit etcd user roles: %w", err)
		}
		if name == recoveryUser {
			seenRecovery = true
		}
		if _, ok := retiredSet[name]; ok {
			retiredSet[name] = false
		}
		isAdmin := false
		for _, role := range user.Roles {
			if role == "root" {
				isAdmin = true
			}
		}
		if isAdmin {
			if name == recoveryUser {
				return errors.New("recovery identity cannot be an administrator")
			}
			if _, retired := retiredSet[name]; retired {
				return errors.New("retired writer cannot be an administrator")
			}
			continue
		}
		for _, role := range user.Roles {
			permissions, err := admin.RoleGet(ctx, role)
			if err != nil {
				return fmt.Errorf("audit etcd permissions: %w", err)
			}
			for _, permission := range permissions.Perm {
				if permission == nil {
					return errors.New("invalid etcd permission")
				}
				start, end := string(permission.Key), string(permission.RangeEnd)
				if name == recoveryUser {
					if !rangeInside(start, end, root) {
						return errors.New("recovery identity has permissions outside the tenant root")
					}
					if start == root && end == clientv3.GetPrefixRangeEnd(root) && permission.PermType == clientv3.PermReadWrite {
						fullAccess = true
					}
				} else if permission.PermType != clientv3.PermRead && rangesIntersect(start, end, root, clientv3.GetPrefixRangeEnd(root)) {
					return errors.New("another nonadministrator identity can still write the tenant root; revoke its grants first")
				}
			}
		}
	}
	for _, missing := range retiredSet {
		if missing {
			return errors.New("retired writer identity is missing; retain disabled users for the audit")
		}
	}
	if !seenRecovery || !fullAccess {
		return errors.New("recovery identity lacks exclusive tenant-root read/write permission")
	}
	return nil
}

func rangeInside(start, end, root string) bool {
	if end == "" {
		return strings.HasPrefix(start, root)
	}
	return start >= root && end != "\x00" && end <= clientv3.GetPrefixRangeEnd(root) && start < end
}

func rangesIntersect(start, end, otherStart, otherEnd string) bool {
	if end == "" {
		return start >= otherStart && start < otherEnd
	}
	return start < otherEnd && (end == "\x00" || end > otherStart)
}

// Restore restores registry and mappings only, keeping the existing deployment
// identity, endpoint, root and etcd cluster. It never resets global revisions.
// A persistent checkpoint blocks runtime startup until all batches complete.
// Unknown transaction outcomes require --resume with the identical archive.
func Restore(ctx context.Context, kv clientv3.KV, archive string, opts RestoreOptions) (*Summary, error) {
	if opts.CheckStopped == nil {
		return nil, errors.New("host workload quiescence check is required")
	}
	spool, err := os.CreateTemp(opts.TempDir, "tenant-restore-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(spool.Name())
	defer spool.Close()
	summary, err := inspect(archive, spool)
	if err != nil {
		return nil, err
	}
	plan, err := archiveLeasePlan(spool)
	if err != nil {
		return nil, err
	}
	if len(plan) > 0 && (!opts.RestartLeaseTTL || opts.Leases == nil) {
		return nil, errors.New("leased data requires explicit --restart-lease-ttl and a lease-aware recovery client")
	}
	root := summary.Root
	check := func() error {
		if err := opts.CheckStopped(ctx); err != nil {
			return fmt.Errorf("workload is not stopped: %w", err)
		}
		return AuditWriters(ctx, opts.Admin, root, opts.RecoveryUser, opts.RetiredUsers)
	}
	if err := check(); err != nil {
		return nil, err
	}
	claim, err := kv.Get(ctx, root+ClaimSuffix)
	if err != nil {
		return nil, err
	}
	if len(claim.Kvs) != 1 || claim.Header.ClusterId != summary.ClusterID {
		return nil, errors.New("destination ownership or etcd cluster does not match archive")
	}
	var owner Ownership
	if err := json.Unmarshal(claim.Kvs[0].Value, &owner); err != nil || owner != summary.Ownership {
		return nil, errors.New("destination deployment identity or storage layout does not match archive")
	}
	claimValue := string(claim.Kvs[0].Value)
	stateKey := root + RestoreSuffix
	current, err := kv.Get(ctx, stateKey)
	if err != nil {
		return nil, err
	}
	state := restoreState{Version: 1, Archive: summary.SHA256, Owner: owner, RecoveryUser: opts.RecoveryUser, Phase: "clear"}
	token := int64(0)
	if len(current.Kvs) > 0 {
		if !opts.Resume {
			return nil, errors.New("restore is incomplete; use --resume with the identical archive and identity")
		}
		if err := json.Unmarshal(current.Kvs[0].Value, &state); err != nil || state.Version != 1 || state.Archive != summary.SHA256 || state.Owner != owner || state.RecoveryUser != opts.RecoveryUser || state.Next < 0 || state.Next > summary.Entries ||
			(state.Phase != "clear" && state.Phase != "copy") || (state.Phase == "clear" && state.Next != 0) {
			return nil, errors.New("restore checkpoint does not match this archive or recovery identity")
		}
		token = current.Kvs[0].ModRevision
	} else if opts.Resume {
		// Completion and its receipt are one transaction. Confirm a committed
		// result after a lost response without starting another destructive copy.
		receipt, err := kv.Get(ctx, root+RestoreReceiptSuffix)
		if err != nil {
			return nil, err
		}
		var completed restoreState
		if len(receipt.Kvs) != 1 || json.Unmarshal(receipt.Kvs[0].Value, &completed) != nil || completed.Version != 1 || completed.Phase != "complete" || completed.Archive != summary.SHA256 || completed.Owner != owner || completed.RecoveryUser != opts.RecoveryUser || completed.Next != summary.Entries {
			return nil, errors.New("no matching pending restore or completion receipt exists")
		}
		if err := validateLeaseBindings(plan, completed.Leases, true); err != nil {
			return nil, err
		}
		if err := verifyRestoredData(ctx, kv, summary, false, completed.Leases); err != nil {
			return nil, err
		}
		return summary, nil
	}
	if err := validateLeaseBindings(plan, state.Leases, state.Next > 0); err != nil {
		return nil, err
	}
	var renewals *leaseSession
	// Every write binds ownership, the checkpoint revision, and competing
	// lifecycle barriers in a single etcd transaction.
	commit := func(next *restoreState, ops ...clientv3.Op) error {
		if renewals != nil {
			if err := renewals.check(); err != nil {
				return err
			}
		}
		if err := check(); err != nil {
			return err
		}
		conditions := []clientv3.Cmp{
			clientv3.Compare(clientv3.Value(root+ClaimSuffix), "=", claimValue),
			clientv3.Compare(clientv3.ModRevision(stateKey), "=", token),
			clientv3.Compare(clientv3.CreateRevision(root+MigrationSuffix), "=", 0),
			clientv3.Compare(clientv3.CreateRevision(root+FenceSuffix), "=", 0),
		}
		if next == nil {
			ops = append(ops, clientv3.OpDelete(stateKey))
		} else {
			payload, _ := json.Marshal(next)
			ops = append(ops, clientv3.OpPut(stateKey, string(payload)))
		}
		result, err := kv.Txn(ctx).If(conditions...).Then(ops...).Commit()
		if err != nil {
			return fmt.Errorf("restore transaction outcome may be unknown; retry with --resume: %w", err)
		}
		if !result.Succeeded {
			return errors.New("ownership, lifecycle barrier or restore checkpoint changed; restore refused")
		}
		token = result.Header.Revision
		return nil
	}
	if token == 0 {
		if err := commit(&state); err != nil {
			return nil, err
		}
	}
	if state.Phase == "clear" {
		next := state
		next.Phase = "copy"
		if err := commit(&next, clientv3.OpDelete(root+"registry/", clientv3.WithPrefix()), clientv3.OpDelete(root+"mappings/", clientv3.WithPrefix())); err != nil {
			return nil, err
		}
		state = next
	}
	if len(plan) > 0 {
		bindings, session, changed, err := prepareRestoreLeases(ctx, opts.Leases, plan, state.Leases)
		if err != nil {
			return nil, err
		}
		renewals = session
		defer renewals.close()
		if changed {
			next := state
			next.Leases = bindings
			next.Next = 0
			if err := commit(&next); err != nil {
				return nil, err
			}
			state = next
		}
	}
	if _, err := spool.Seek(0, 0); err != nil {
		return nil, err
	}
	scan := bufio.NewScanner(spool)
	scan.Buffer(make([]byte, 64*1024), maxLineBytes)
	index, batchBytes := 0, 0
	ops := []clientv3.Op{}
	flush := func() error {
		if len(ops) == 0 {
			return nil
		}
		next := state
		next.Next = index
		if err := commit(&next, ops...); err != nil {
			return err
		}
		state = next
		ops = nil
		batchBytes = 0
		return nil
	}
	for scan.Scan() {
		if index < state.Next {
			index++
			continue
		}
		var rec record
		if err := json.Unmarshal(scan.Bytes(), &rec); err != nil {
			return nil, errors.New("private restore spool is invalid")
		}
		size := len(root) + len(rec.Key) + len(rec.Value) + 128
		if len(ops) > 0 && (len(ops) >= 32 || batchBytes+size > 1200*1024) {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		var putOptions []clientv3.OpOption
		if rec.Lease != 0 {
			binding, exists := state.Leases[strconv.FormatInt(rec.Lease, 10)]
			if !exists {
				return nil, errors.New("missing restore lease mapping")
			}
			putOptions = append(putOptions, clientv3.WithLease(binding.Destination))
		}
		ops = append(ops, clientv3.OpPut(root+rec.Key, string(rec.Value), putOptions...))
		batchBytes += size
		index++
	}
	if err := scan.Err(); err != nil {
		return nil, err
	}
	if err := flush(); err != nil {
		return nil, err
	}
	if state.Next != summary.Entries {
		return nil, errors.New("restore spool entry count changed")
	}
	if renewals != nil {
		if err := renewals.refresh(); err != nil {
			return nil, err
		}
	}
	if err := verifyRestoredData(ctx, kv, summary, true, state.Leases); err != nil {
		return nil, err
	}
	completed := state
	completed.Phase = "complete"
	payload, _ := json.Marshal(completed)
	if err := commit(nil, clientv3.OpPut(root+RestoreReceiptSuffix, string(payload))); err != nil {
		return nil, err
	}
	return summary, nil
}

func verifyRestoredData(ctx context.Context, kv clientv3.KV, summary *Summary, pending bool, bindings map[string]leaseBinding) error {
	type originalLease struct{ source, ttl int64 }
	byDestination := map[int64]originalLease{}
	for source, binding := range bindings {
		id, err := strconv.ParseInt(source, 10, 64)
		if err != nil {
			return errors.New("invalid restore lease source")
		}
		byDestination[int64(binding.Destination)] = originalLease{source: id, ttl: binding.TTL}
	}
	root := summary.Root
	anchor, err := kv.Get(ctx, root+ClaimSuffix)
	if err != nil {
		return err
	}
	start, end := root, clientv3.GetPrefixRangeEnd(root)
	hash := sha256.New()
	count := 0
	for {
		page, err := kv.Get(ctx, start, clientv3.WithRange(end), clientv3.WithRev(anchor.Header.Revision), clientv3.WithLimit(32), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		if err != nil {
			return fmt.Errorf("verify restored data: %w", err)
		}
		for _, item := range page.Kvs {
			key := strings.TrimPrefix(string(item.Key), root)
			if key == ClaimSuffix || key == RestoreReceiptSuffix || (key == RestoreSuffix && pending) {
				continue
			}
			if !validDataKey(key) || len(item.Key) > MaxKeyBytes || len(item.Value) > MaxValueBytes {
				return errors.New("unexpected data or lifecycle barrier while verifying restore")
			}
			count++
			if count > summary.Entries {
				return errors.New("restored data has more entries than the archive")
			}
			rec := record{Type: "kv", Key: key, Value: item.Value}
			if item.Lease != 0 {
				original, exists := byDestination[item.Lease]
				if !exists {
					return errors.New("unexpected lease on restored data")
				}
				rec.Lease = original.source
				rec.TTL = original.ttl
			}
			_, _ = hash.Write(line(rec))
		}
		if !page.More {
			break
		}
		if len(page.Kvs) == 0 {
			return errors.New("empty verification page")
		}
		start = string(page.Kvs[len(page.Kvs)-1].Key) + "\x00"
	}
	if count != summary.Entries || hex.EncodeToString(hash.Sum(nil)) != summary.DataSHA256 {
		return errors.New("restored data does not match archive; startup barrier retained if recovery is pending")
	}
	return nil
}
