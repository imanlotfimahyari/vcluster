package tenantbackup

import (
	"testing"

	"go.etcd.io/etcd/api/v3/authpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestCorruptAccessPlanRefusedBeforeAuthMutations(t *testing.T) {
	for _, name := range []string{"nil", "root recovery", "root retired", "foreign grant", "unlisted user", "admin role"} {
		t.Run(name, func(t *testing.T) {
			p := &AccessPlan{Root: "/tenants/a/", Recovery: "fresh", Retired: []string{"old"}}
			grant := RetiredGrant{User: "old", Role: "old-role", Permissions: []*authpb.Permission{{PermType: clientv3.PermReadWrite, Key: []byte(p.Root), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(p.Root))}}}
			switch name {
			case "nil":
				p = nil
			case "root recovery":
				p.Recovery = "root"
			case "root retired":
				p.Retired = []string{"root"}
			case "foreign grant":
				grant.Permissions[0].Key = []byte("/foreign/")
				grant.Permissions[0].RangeEnd = []byte(clientv3.GetPrefixRangeEnd("/foreign/"))
				p.Grants = []RetiredGrant{grant}
			case "unlisted user":
				grant.User = "other"
				p.Grants = []RetiredGrant{grant}
			case "admin role":
				grant.Role = "root"
				p.Grants = []RetiredGrant{grant}
			}
			// A nil Auth would panic if an unsafe plan reached an RPC.
			if err := PrepareAccess(t.Context(), nil, p); err == nil {
				t.Fatal("invalid plan accepted")
			}
			if err := RollbackAccess(t.Context(), nil, p); err == nil {
				t.Fatal("invalid rollback accepted")
			}
		})
	}
}
