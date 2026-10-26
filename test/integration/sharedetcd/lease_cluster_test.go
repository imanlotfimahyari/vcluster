//go:build integration && linux

package sharedetcd_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/apimachinery/pkg/util/wait"
)

type leaseMember struct {
	endpoint string
	args     []string
	process  *testProcess
	admin    *clientv3.Client
	id       uint64
	active   bool
}
type leaseCluster struct {
	lab     *etcdLab
	members []*leaseMember
	binary  string
}

func newLeaseCluster(t *testing.T) *leaseCluster {
	t.Helper()
	requireLeaseTestVersion(t)
	c := &leaseCluster{binary: asset(t, "etcd")}
	ca, peerCA := newTestCA(t), newTestCA(t)
	server, root := ca.identity(t, "server", true), ca.identity(t, "root", false)
	var peers, initial []string
	for i := 0; i < 3; i++ {
		c.members = append(c.members, &leaseMember{endpoint: fmt.Sprintf("https://127.0.0.1:%d", unusedPort(t)), active: true})
		peers = append(peers, fmt.Sprintf("https://127.0.0.1:%d", unusedPort(t)))
		initial = append(initial, fmt.Sprintf("member-%d=%s", i, peers[i]))
	}
	for i, m := range c.members {
		peer := peerCA.identity(t, fmt.Sprintf("peer-%d", i), true)
		m.args = []string{"--name=" + fmt.Sprintf("member-%d", i), "--data-dir=" + t.TempDir(), "--listen-client-urls=" + m.endpoint, "--advertise-client-urls=" + m.endpoint, "--listen-peer-urls=" + peers[i], "--initial-advertise-peer-urls=" + peers[i], "--initial-cluster=" + strings.Join(initial, ","), "--initial-cluster-token=disposable-lease-authorization", "--cert-file=" + server.ServerCert, "--key-file=" + server.ServerKey, "--trusted-ca-file=" + ca.path, "--client-cert-auth=true", "--peer-cert-file=" + peer.ServerCert, "--peer-key-file=" + peer.ServerKey, "--peer-trusted-ca-file=" + peerCA.path, "--peer-client-cert-auth=true", "--log-level=error"}
		m.process = startProcess(t, fmt.Sprintf("lease-member-%d", i), os.Environ(), c.binary, m.args...)
		m.admin = rawEtcdClient(t, m.endpoint, root)
	}
	c.waitLeader(t, 0)
	c.lab = initializeEtcdLab(t, ca, c.members[0].endpoint, c.members[0].admin, root)
	c.waitAuth(t)
	return c
}
func (c *leaseCluster) waitLeader(t *testing.T, exclude uint64) *leaseMember {
	t.Helper()
	var selected *leaseMember
	err := wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
		leader := uint64(0)
		for _, m := range c.members {
			if !m.active {
				continue
			}
			if err := m.process.check(); err != nil {
				return false, err
			}
			call, cancel := context.WithTimeout(ctx, time.Second)
			response, err := m.admin.Status(call, m.endpoint)
			cancel()
			if err != nil || response.Leader == 0 || response.Leader == exclude {
				return false, nil
			}
			if leader != 0 && leader != response.Leader {
				return false, nil
			}
			leader = response.Leader
			m.id = response.Header.MemberId
		}
		for _, m := range c.members {
			if m.active && m.id == leader {
				selected = m
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatal("three-member lease fixture did not agree on a leader", err)
	}
	return selected
}
func (c *leaseCluster) waitAuth(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	reference, err := c.lab.admin.AuthStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		for _, m := range c.members {
			if !m.active {
				continue
			}
			call, cancel := context.WithTimeout(ctx, time.Second)
			status, err := m.admin.AuthStatus(call)
			cancel()
			if err != nil || !status.Enabled || status.AuthRevision < reference.AuthRevision {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		t.Fatal("members did not apply the authorization revision", err)
	}
}
func (c *leaseCluster) exercise(t *testing.T, id clientv3.LeaseID) {
	t.Helper()
	for i, m := range c.members {
		if !m.active {
			continue
		}
		t.Run(fmt.Sprintf("member-%d", i), func(t *testing.T) {
			a := rawEtcdClient(t, m.endpoint, c.lab.certs["tenant-a"])
			b := rawEtcdClient(t, m.endpoint, c.lab.certs["tenant-b"])
			if err := renewOnStream(leaseStream(t, a), id); err != nil {
				t.Fatal("owner renewal failed on member", err)
			}
			assertLeaseRenewDenied(t, b, id)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			_, err := b.TimeToLive(ctx, id, clientv3.WithAttachedKeys())
			assertPermissionDenied(t, err)
			_, err = b.Revoke(ctx, id)
			assertPermissionDenied(t, err)
		})
	}
}
func TestLeaseAuthorizationAcrossLeaderChangesAndRestarts(t *testing.T) {
	c := newLeaseCluster(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	a := rawEtcdClient(t, c.members[0].endpoint, c.lab.certs["tenant-a"])
	l, err := a.Grant(ctx, 180)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Put(ctx, "/tenants/tenant-a/registry/cluster-lease", "retained", clientv3.WithLease(l.ID)); err != nil {
		t.Fatal(err)
	}
	c.exercise(t, l.ID)
	oldLeader := c.waitLeader(t, 0)
	var target *leaseMember
	for _, m := range c.members {
		if m != oldLeader {
			target = m
			break
		}
	}
	if _, err := oldLeader.admin.MoveLeader(ctx, target.id); err != nil {
		t.Fatal("leader transfer failed", err)
	}
	leader := c.waitLeader(t, oldLeader.id)
	if leader.id != target.id {
		t.Fatal("unexpected transfer target")
	}
	c.exercise(t, l.ID)
	// Force election by stopping the new leader while two members retain quorum.
	leader.active = false
	leader.process.stop()
	c.waitLeader(t, leader.id)
	c.exercise(t, l.ID)
	leader.process = startProcess(t, "restarted-lease-leader", os.Environ(), c.binary, leader.args...)
	leader.active = true
	c.waitLeader(t, 0)
	c.waitAuth(t)
	c.exercise(t, l.ID)
	// Check revocation on a stream established against every member, including
	// follower forwarding. Wait for the recorded auth revision before asserting.
	var streams []pb.Lease_LeaseKeepAliveClient
	for _, m := range c.members {
		client := rawEtcdClient(t, m.endpoint, c.lab.certs["tenant-a"])
		stream := leaseStream(t, client)
		if err := renewOnStream(stream, l.ID); err != nil {
			t.Fatal(err)
		}
		streams = append(streams, stream)
	}
	if _, err := c.lab.admin.UserRevokeRole(ctx, "tenant-a", "tenant-a"); err != nil {
		t.Fatal(err)
	}
	c.waitAuth(t)
	for _, stream := range streams {
		assertAuthRejected(t, renewOnStream(stream, l.ID))
	}
	// A complete stop/start must retain auth records, data and lease attachment.
	for _, m := range c.members {
		m.active = false
		m.process.stop()
	}
	for i, m := range c.members {
		m.process = startProcess(t, fmt.Sprintf("restarted-all-%d", i), os.Environ(), c.binary, m.args...)
		m.active = true
	}
	c.waitLeader(t, 0)
	c.waitAuth(t)
	for _, m := range c.members {
		client := rawEtcdClient(t, m.endpoint, c.lab.certs["tenant-a"])
		assertLeaseRenewDenied(t, client, l.ID)
	}
	if _, err := c.lab.admin.UserGrantRole(ctx, "tenant-a", "tenant-a"); err != nil {
		t.Fatal(err)
	}
	c.waitAuth(t)
	c.exercise(t, l.ID)
	stored, err := c.lab.admin.Get(ctx, "/tenants/tenant-a/registry/cluster-lease")
	if err != nil || len(stored.Kvs) != 1 || stored.Kvs[0].Lease != int64(l.ID) || string(stored.Kvs[0].Value) != "retained" {
		t.Fatal("lease or value did not survive restart", err)
	}
}
