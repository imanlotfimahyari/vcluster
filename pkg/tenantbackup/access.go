package tenantbackup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// AccessPlan is persisted before any auth mutation. Only explicitly retired
// users lose roles; shared role definitions and other users are never changed.
type AccessPlan struct {
	Root     string         `json:"root"`
	Legacy   bool           `json:"legacy"`
	Recovery string         `json:"recovery"`
	Retired  []string       `json:"retired"`
	Grants   []RetiredGrant `json:"grants"`
}
type RetiredGrant struct {
	User        string               `json:"user"`
	Role        string               `json:"role"`
	Permissions []*authpb.Permission `json:"permissions"`
}

func accessScopes(root string, legacy bool) []string {
	scopes := []string{root}
	if legacy {
		scopes = append(scopes, "/registry", "/vcluster/mappings")
	}
	return scopes
}
func permissionTouches(p *authpb.Permission, scopes []string) bool {
	for _, s := range scopes {
		if rangesIntersect(string(p.Key), string(p.RangeEnd), s, clientv3.GetPrefixRangeEnd(s)) {
			return true
		}
	}
	return false
}
func permissionInside(p *authpb.Permission, scopes []string) bool {
	for _, s := range scopes {
		if rangeInside(string(p.Key), string(p.RangeEnd), s) {
			return true
		}
	}
	return false
}

// PlanAccess is read-only. Broad/multi-tenant roles and unexpected writers are
// refused before shutdown, identity creation or revocation.
func PlanAccess(ctx context.Context, auth clientv3.Auth, root, recovery string, retired []string, legacy bool) (*AccessPlan, error) {
	normalized, err := NormalizeRoot(root)
	if err != nil {
		return nil, err
	}
	if normalized != root || recovery == "" || recovery == "root" || len(retired) == 0 {
		return nil, errors.New("invalid recovery access request")
	}
	status, err := auth.AuthStatus(ctx)
	if err != nil {
		return nil, err
	}
	if !status.Enabled {
		return nil, errors.New("etcd authentication must be enabled")
	}
	users, err := auth.UserList(ctx)
	if err != nil {
		return nil, err
	}
	plan := &AccessPlan{Root: root, Recovery: recovery, Retired: slices.Clone(retired), Legacy: legacy}
	seen := map[string]bool{}
	for _, u := range retired {
		if u == "" || u == "root" || u == recovery || seen[u] {
			return nil, errors.New("retired identities must be distinct nonadministrators")
		}
		seen[u] = true
	}
	for _, u := range users.Users {
		if u == recovery {
			return nil, errors.New("replacement identity already exists; choose a fresh identity")
		}
		user, err := auth.UserGet(ctx, u)
		if err != nil {
			return nil, err
		}
		if slices.Contains(user.Roles, "root") {
			if seen[u] {
				return nil, errors.New("cannot retire an administrator")
			}
			continue
		}
		for _, role := range user.Roles {
			r, err := auth.RoleGet(ctx, role)
			if err != nil {
				return nil, err
			}
			touches := false
			for _, p := range r.Perm {
				if p == nil {
					return nil, errors.New("invalid permission")
				}
				if p.PermType != clientv3.PermRead && permissionTouches(p, accessScopes(root, legacy)) {
					touches = true
				}
			}
			if !touches {
				continue
			}
			if !seen[u] {
				return nil, errors.New("unexpected writer intersects operation ranges; explicitly identify all retired writers")
			}
			for _, p := range r.Perm {
				if !permissionInside(p, accessScopes(root, legacy)) {
					return nil, errors.New("retired writer role also covers unrelated ranges; split its permissions first")
				}
			}
			plan.Grants = append(plan.Grants, RetiredGrant{User: u, Role: role, Permissions: r.Perm})
		}
	}
	for _, u := range retired {
		if !slices.Contains(users.Users, u) {
			return nil, errors.New("retired writer is missing")
		}
	}
	if _, err := auth.RoleGet(ctx, recovery); err == nil {
		return nil, errors.New("replacement role already exists")
	} else if !errors.Is(err, rpctypes.ErrRoleNotFound) {
		return nil, err
	}
	return plan, nil
}
func samePermissions(a, b []*authpb.Permission) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}
func checkRetiredRoles(ctx context.Context, auth clientv3.Auth, p *AccessPlan) error {
	for _, g := range p.Grants {
		r, err := auth.RoleGet(ctx, g.Role)
		if err != nil {
			return err
		}
		if !samePermissions(r.Perm, g.Permissions) {
			return errors.New("retired role permissions changed since planning")
		}
	}
	return nil
}

func validateAccessPlan(p *AccessPlan) error {
	if p == nil {
		return errors.New("access plan is missing")
	}
	root, err := NormalizeRoot(p.Root)
	if err != nil || root != p.Root || p.Recovery == "" || p.Recovery == "root" || len(p.Retired) == 0 {
		return errors.New("invalid access plan")
	}
	seen := map[string]bool{}
	for _, u := range p.Retired {
		if u == "" || u == "root" || u == p.Recovery || seen[u] {
			return errors.New("invalid retired identities")
		}
		seen[u] = true
	}
	grants := map[string]bool{}
	for _, g := range p.Grants {
		if !seen[g.User] || g.Role == "" || g.Role == "root" || grants[g.User+"\x00"+g.Role] || len(g.Permissions) == 0 {
			return errors.New("invalid retired role binding")
		}
		grants[g.User+"\x00"+g.Role] = true
		for _, perm := range g.Permissions {
			if perm == nil || !permissionInside(perm, accessScopes(p.Root, p.Legacy)) {
				return errors.New("recorded role exceeds operation ranges")
			}
		}
	}
	return nil
}

// PrepareAccess is idempotent under the serialized administrator contract.
func PrepareAccess(ctx context.Context, auth clientv3.Auth, p *AccessPlan) error {
	return PrepareAccessWithCheck(ctx, auth, p, nil)
}

// PrepareAccessWithCheck authenticates the provisioned credential before retiring writers.
func PrepareAccessWithCheck(ctx context.Context, auth clientv3.Auth, p *AccessPlan, check func() error) error {
	if err := validateAccessPlan(p); err != nil {
		return err
	}
	if err := checkRetiredRoles(ctx, auth, p); err != nil {
		return err
	}
	if _, err := auth.RoleAdd(ctx, p.Recovery); err != nil && !errors.Is(err, rpctypes.ErrRoleAlreadyExist) {
		return err
	}
	r, err := auth.RoleGet(ctx, p.Recovery)
	if err != nil {
		return err
	}
	want := &authpb.Permission{PermType: clientv3.PermReadWrite, Key: []byte(p.Root), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(p.Root))}
	if len(r.Perm) > 0 && !samePermissions(r.Perm, []*authpb.Permission{want}) {
		return errors.New("replacement role permissions differ from operation plan")
	}
	if len(r.Perm) == 0 {
		if _, err := auth.RoleGrantPermission(ctx, p.Recovery, p.Root, clientv3.GetPrefixRangeEnd(p.Root), clientv3.PermissionType(clientv3.PermReadWrite)); err != nil {
			return err
		}
	}
	if _, err := auth.UserAddWithOptions(ctx, p.Recovery, "", &clientv3.UserAddOptions{NoPassword: true}); err != nil && !errors.Is(err, rpctypes.ErrUserAlreadyExist) {
		return err
	}
	u, err := auth.UserGet(ctx, p.Recovery)
	if err != nil {
		return err
	}
	for _, role := range u.Roles {
		if role != p.Recovery {
			return errors.New("replacement user has unexpected roles")
		}
	}
	if _, err := auth.UserGrantRole(ctx, p.Recovery, p.Recovery); err != nil {
		return err
	}
	if check != nil {
		if err := check(); err != nil {
			return err
		}
	}
	for _, g := range p.Grants {
		user, err := auth.UserGet(ctx, g.User)
		if err != nil {
			return err
		}
		if slices.Contains(user.Roles, "root") {
			return errors.New("retired writer became an administrator")
		}
		if slices.Contains(user.Roles, g.Role) {
			if _, err := auth.UserRevokeRole(ctx, g.User, g.Role); err != nil {
				return err
			}
		}
	}
	return AuditAccess(ctx, auth, p)
}
func AuditAccess(ctx context.Context, auth clientv3.Auth, p *AccessPlan) error {
	if err := validateAccessPlan(p); err != nil {
		return err
	}
	if err := AuditWriters(ctx, auth, p.Root, p.Recovery, p.Retired); err != nil {
		return err
	}
	if !p.Legacy {
		return nil
	}
	users, err := auth.UserList(ctx)
	if err != nil {
		return err
	}
	for _, name := range users.Users {
		u, err := auth.UserGet(ctx, name)
		if err != nil {
			return err
		}
		if slices.Contains(u.Roles, "root") {
			continue
		}
		for _, role := range u.Roles {
			r, err := auth.RoleGet(ctx, role)
			if err != nil {
				return err
			}
			for _, perm := range r.Perm {
				if perm == nil {
					return errors.New("invalid permission")
				}
				if perm.PermType != clientv3.PermRead && permissionTouches(perm, []string{"/registry", "/vcluster/mappings"}) {
					return errors.New("legacy source still has a nonadministrator writer")
				}
			}
		}
	}
	return nil
}

// RollbackAccess only reinstates the exact recorded role definitions. It never
// edits a role, deletes an identity, or grants an administrator role.
func RollbackAccess(ctx context.Context, auth clientv3.Auth, p *AccessPlan) error {
	if err := validateAccessPlan(p); err != nil {
		return err
	}
	if err := checkRetiredRoles(ctx, auth, p); err != nil {
		return err
	}
	u, err := auth.UserGet(ctx, p.Recovery)
	if err != nil && !errors.Is(err, rpctypes.ErrUserNotFound) {
		return err
	}
	if err == nil {
		for _, role := range u.Roles {
			if role != p.Recovery {
				return errors.New("replacement user changed; rollback refused")
			}
		}
		if slices.Contains(u.Roles, p.Recovery) {
			if _, err := auth.UserRevokeRole(ctx, p.Recovery, p.Recovery); err != nil {
				return err
			}
		}
	}
	for _, g := range p.Grants {
		if _, err := auth.UserGrantRole(ctx, g.User, g.Role); err != nil {
			return fmt.Errorf("restore retired role membership: %w", err)
		}
	}
	return nil
}
