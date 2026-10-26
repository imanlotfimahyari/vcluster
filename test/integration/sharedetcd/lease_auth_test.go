//go:build integration && linux

package sharedetcd_test

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const leaseTestEtcdVersion = "3.6.16"

// Fail closed when an old binary is supplied: passing the lease suite must not
// silently mean we tested a different etcd server. Libraries remain separately
// versioned in go.mod; these tests exercise the external executable.
func requireLeaseTestVersion(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, asset(t, "etcd"), "--version").Output()
	if err != nil || !strings.Contains(string(out), "etcd Version: "+leaseTestEtcdVersion+"\n") {
		t.Fatalf("lease authorization suite requires stock etcd %s", leaseTestEtcdVersion)
	}
}
func leaseIdentity(t *testing.T, lab *etcdLab, name, root string, perm clientv3.PermissionType) *clientv3.Client {
	t.Helper()
	ctx := t.Context()
	if _, err := lab.admin.RoleAdd(ctx, name); err != nil {
		t.Fatal(err)
	}
	if _, err := lab.admin.RoleGrantPermission(ctx, name, root, clientv3.GetPrefixRangeEnd(root), perm); err != nil {
		t.Fatal(err)
	}
	if _, err := lab.admin.UserAddWithOptions(ctx, name, "", &clientv3.UserAddOptions{NoPassword: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := lab.admin.UserGrantRole(ctx, name, name); err != nil {
		t.Fatal(err)
	}
	lab.certs[name] = lab.ca.identity(t, name, false)
	return rawEtcdClient(t, lab.endpoint, lab.certs[name])
}

// Use the raw bidi RPC so the test controls each message on the same stream.
// High-level KeepAlive maintains its own reconnect/refresh loop.
func leaseStream(t *testing.T, client *clientv3.Client) pb.Lease_LeaseKeepAliveClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	stream, err := pb.NewLeaseClient(client.ActiveConnection()).LeaseKeepAlive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return stream
}
func renewOnStream(stream pb.Lease_LeaseKeepAliveClient, id clientv3.LeaseID) error {
	if err := stream.Send(&pb.LeaseKeepAliveRequest{ID: int64(id)}); err != nil {
		return err
	}
	response, err := stream.Recv()
	if err != nil {
		return err
	}
	if response.ID != int64(id) || response.TTL <= 0 {
		return errors.New("lease keep-alive did not renew the requested lease")
	}
	return nil
}
func assertLeaseRenewDenied(t *testing.T, client *clientv3.Client, id clientv3.LeaseID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	stream, err := pb.NewLeaseClient(client.ActiveConnection()).LeaseKeepAlive(ctx)
	if err == nil {
		err = renewOnStream(stream, id)
	}
	assertPermissionDenied(t, err)
}
func assertAuthRejected(t *testing.T, err error) {
	t.Helper()
	code := status.Code(err)
	if code != codes.PermissionDenied && code != codes.Unauthenticated && !errors.Is(err, rpctypes.ErrPermissionDenied) && !errors.Is(err, rpctypes.ErrAuthOldRevision) {
		t.Fatalf("expected authorization rejection, got %v", err)
	}
}
func TestLeaseAuthorizationBoundaries(t *testing.T) {
	requireLeaseTestVersion(t)
	lab := newEtcdLab(t)
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	a := rawEtcdClient(t, lab.endpoint, lab.certs["tenant-a"])
	b := rawEtcdClient(t, lab.endpoint, lab.certs["tenant-b"])
	reader := leaseIdentity(t, lab, "reader-a", "/tenants/tenant-a/", clientv3.PermissionType(clientv3.PermRead))
	lease, err := a.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	key := "/tenants/tenant-a/registry/lease-test"
	if _, err := a.Put(ctx, key, "a", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatal(err)
	}
	foreign, err := b.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Put(ctx, "/tenants/tenant-b/registry/lease-test", "b", clientv3.WithLease(foreign.ID)); err != nil {
		t.Fatal(err)
	}
	if err := renewOnStream(leaseStream(t, a), lease.ID); err != nil {
		t.Fatal("own lease renewal", err)
	}
	assertLeaseRenewDenied(t, b, lease.ID)
	assertLeaseRenewDenied(t, reader, lease.ID)
	for _, keys := range []bool{false, true} {
		var options []clientv3.LeaseOption
		if keys {
			options = append(options, clientv3.WithAttachedKeys())
		}
		if _, err := reader.TimeToLive(ctx, lease.ID, options...); err != nil {
			t.Fatal("reader cannot inspect its readable lease", err)
		}
		response, err := b.TimeToLive(ctx, lease.ID, options...)
		if keys {
			assertPermissionDenied(t, err)
		} else {
			// Upstream's TTL-only query returns metadata without attached-key
			// read authorization. Range grants do not promise TTL privacy.
			if err != nil || response.ID != lease.ID || response.TTL <= 0 || len(response.Keys) != 0 {
				t.Fatal("TTL-only metadata contract changed", err)
			}
		}
	}
	for _, client := range []*clientv3.Client{a, b, reader} {
		_, err := client.Leases(ctx)
		assertPermissionDenied(t, err)
	}
	listed, err := lab.admin.Leases(ctx)
	if err != nil || len(listed.Leases) < 2 {
		t.Fatal("admin cannot list leases", err)
	}
	_, err = b.Revoke(ctx, lease.ID)
	assertPermissionDenied(t, err)
	_, err = reader.Revoke(ctx, lease.ID)
	assertPermissionDenied(t, err)
	foreignKey := "/tenants/tenant-b/registry/illegal-attachment"
	_, err = b.Put(ctx, foreignKey, "no", clientv3.WithLease(lease.ID))
	assertPermissionDenied(t, err)
	_, err = b.Txn(ctx).Then(clientv3.OpPut(foreignKey, "no", clientv3.WithLease(lease.ID))).Commit()
	assertPermissionDenied(t, err)
	_, err = b.Txn(ctx).Then(clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpPut(foreignKey, "no", clientv3.WithLease(lease.ID))}, nil)).Commit()
	assertPermissionDenied(t, err)
	unchanged, err := lab.admin.Get(ctx, foreignKey)
	if err != nil || len(unchanged.Kvs) != 0 {
		t.Fatal("denied lease attachment wrote a key")
	}
	if _, err := a.Revoke(ctx, lease.ID); err != nil {
		t.Fatal("own revoke", err)
	}
	value, err := a.Get(ctx, key)
	if err != nil || len(value.Kvs) != 0 {
		t.Fatal("own revoke did not remove leased key")
	}
}
func TestLeaseRevocationOnExistingStream(t *testing.T) {
	requireLeaseTestVersion(t)
	lab := newEtcdLab(t)
	ctx := t.Context()
	a := rawEtcdClient(t, lab.endpoint, lab.certs["tenant-a"])
	l, err := a.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Put(ctx, "/tenants/tenant-a/registry/stream", "a", clientv3.WithLease(l.ID)); err != nil {
		t.Fatal(err)
	}
	stream := leaseStream(t, a)
	if err := renewOnStream(stream, l.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := lab.admin.UserRevokeRole(ctx, "tenant-a", "tenant-a"); err != nil {
		t.Fatal(err)
	}
	assertAuthRejected(t, renewOnStream(stream, l.ID))
	assertLeaseRenewDenied(t, a, l.ID)
	if _, err := lab.admin.UserGrantRole(ctx, "tenant-a", "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if err := renewOnStream(leaseStream(t, a), l.ID); err != nil {
		t.Fatal("fresh stream after regrant", err)
	}
}
func TestLeaseAuthorizationDuringPermissionChanges(t *testing.T) {
	requireLeaseTestVersion(t)
	lab := newEtcdLab(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	a := rawEtcdClient(t, lab.endpoint, lab.certs["tenant-a"])
	l, err := a.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	key := "/tenants/tenant-a/registry/concurrent"
	if _, err := a.Put(ctx, key, "a", clientv3.WithLease(l.ID)); err != nil {
		t.Fatal(err)
	}
	// Requests overlap each permission change. Assertions after the completed
	// auth change use fresh RPCs; we do not claim that an in-flight authorized
	// request is retroactively cancelled by revocation.
	for cycle := 0; cycle < 3; cycle++ {
		active, stop := context.WithCancel(ctx)
		var workers sync.WaitGroup
		started := make(chan struct{}, 2)
		failures := make(chan error, 2)
		for range 2 {
			workers.Go(func() {
				ticker := time.NewTicker(10 * time.Millisecond)
				defer ticker.Stop()
				notified := false
				for {
					select {
					case <-active.Done():
						return
					case <-ticker.C:
						stream, err := pb.NewLeaseClient(a.ActiveConnection()).LeaseKeepAlive(active)
						if err == nil {
							err = renewOnStream(stream, l.ID)
							_ = stream.CloseSend()
						}
						if !notified {
							started <- struct{}{}
							notified = true
						}
						if err != nil && active.Err() == nil && status.Code(err) != codes.PermissionDenied && status.Code(err) != codes.Unauthenticated {
							select {
							case failures <- err:
							default:
							}
							return
						}
					}
				}
			})
		}
		for range 2 {
			select {
			case <-started:
			case <-ctx.Done():
				stop()
				workers.Wait()
				t.Fatal("concurrent renewal workers did not start", ctx.Err())
			}
		}
		if _, err := lab.admin.RoleRevokePermission(ctx, "tenant-a", "/tenants/tenant-a/", clientv3.GetPrefixRangeEnd("/tenants/tenant-a/")); err != nil {
			stop()
			workers.Wait()
			t.Fatal(err)
		}
		assertLeaseRenewDenied(t, a, l.ID)
		stop()
		workers.Wait()
		select {
		case err := <-failures:
			t.Fatal("unexpected concurrent renewal failure", err)
		default:
		}
		if _, err := lab.admin.RoleGrantPermission(ctx, "tenant-a", "/tenants/tenant-a/", clientv3.GetPrefixRangeEnd("/tenants/tenant-a/"), clientv3.PermissionType(clientv3.PermReadWrite)); err != nil {
			t.Fatal(err)
		}
		if err := renewOnStream(leaseStream(t, a), l.ID); err != nil {
			t.Fatal("renewal after permission regrant", err)
		}
	}
}

// Upstream authorization follows attached keys, not the identity that called
// Grant. Empty leases have no persistent owner. This is an explicit semantic
// boundary, not evidence that stock etcd implements tenant-owned lease IDs.
func TestEmptyLeaseHasNoCreatorOwnership(t *testing.T) {
	requireLeaseTestVersion(t)
	lab := newEtcdLab(t)
	ctx := t.Context()
	a := rawEtcdClient(t, lab.endpoint, lab.certs["tenant-a"])
	b := rawEtcdClient(t, lab.endpoint, lab.certs["tenant-b"])
	empty, err := a.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	if err := renewOnStream(leaseStream(t, b), empty.ID); err != nil {
		t.Fatal("documented empty-lease behavior changed", err)
	}
	if _, err := b.Put(ctx, "/tenants/tenant-b/registry/claimed-empty", "b", clientv3.WithLease(empty.ID)); err != nil {
		t.Fatal("empty lease attachment behavior changed", err)
	}
	assertLeaseRenewDenied(t, a, empty.ID)
	if err := renewOnStream(leaseStream(t, b), empty.ID); err != nil {
		t.Fatal(err)
	}
	_, err = a.Revoke(ctx, empty.ID)
	assertPermissionDenied(t, err)
	another, err := a.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Revoke(ctx, another.ID); err != nil {
		t.Fatal("empty lease revoke behavior changed", err)
	}
}
