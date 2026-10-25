package etcd

import "testing"

func TestTenantKeysPrefix(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		requested, root, want string
		wantErr               bool
	}{
		{requested: "/", want: "/"},
		{requested: "/custom", want: "/custom"},
		{requested: "/", root: "/tenants/a/", want: "/tenants/a/"},
		{root: "/tenants/a/", want: "/tenants/a/"},
		{requested: "/tenants/a", root: "/tenants/a/", want: "/tenants/a/"},
		{requested: "/tenants/a/mappings/", root: "/tenants/a/", want: "/tenants/a/mappings/"},
		{requested: "/tenants/b/", root: "/tenants/a/", wantErr: true},
		{requested: "/tenants/ab/", root: "/tenants/a/", wantErr: true},
		{requested: "/tenants/", root: "/tenants/a/", wantErr: true},
	} {
		got, err := tenantKeysPrefix(test.requested, test.root)
		if (err != nil) != test.wantErr || got != test.want {
			t.Fatalf("prefix(%q, %q) = %q, %v; want %q, error=%v", test.requested, test.root, got, err, test.want, test.wantErr)
		}
	}
}
