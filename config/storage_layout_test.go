package config

import (
	"reflect"
	"strings"
	"testing"
)

func sharedEtcdConfig(root string) *Config {
	c := &Config{}
	c.ControlPlane.BackingStore.Etcd.External.Enabled = true
	c.ControlPlane.BackingStore.Etcd.External.StorePrefix = root
	c.ControlPlane.Distro.K8S.Enabled = true
	c.ControlPlane.Distro.K8S.APIServer.Enabled = true
	return c
}

func TestResolveEtcdStorageLayout(t *testing.T) {
	t.Parallel()
	for _, root := range []string{"/tenants/test-a", "/tenants/test-a/"} {
		t.Run(root, func(t *testing.T) {
			c := sharedEtcdConfig(root)
			layout, err := c.ResolveEtcdStorageLayout()
			if err != nil {
				t.Fatal(err)
			}
			want := EtcdStorageLayout{
				RootPrefix: "/tenants/test-a/", KubernetesPrefix: "/tenants/test-a/registry", MappingsPrefix: "/tenants/test-a/mappings/",
			}
			if layout != want {
				t.Fatalf("layout = %+v, want %+v", layout, want)
			}
			if c.ControlPlane.BackingStore.Etcd.External.StorePrefix != root {
				t.Fatal("resolver mutated the config")
			}
			wantArgs := []string{"--etcd-prefix=/tenants/test-a/registry", "--etcd-compaction-interval=0", "--etcd-db-metric-poll-interval=0"}
			if !reflect.DeepEqual(layout.APIServerArgs(), wantArgs) {
				t.Fatalf("API-server args = %v, want %v", layout.APIServerArgs(), wantArgs)
			}
		})
	}
}

func TestResolveEtcdStorageLayoutLegacy(t *testing.T) {
	t.Parallel()
	c := &Config{}
	// Existing operator-supplied flags must retain their original precedence.
	c.ControlPlane.Distro.K8S.APIServer.ExtraArgs = []string{"--etcd-prefix=/existing", "--etcd-compaction-interval=10m"}
	layout, err := c.ResolveEtcdStorageLayout()
	if err != nil {
		t.Fatal(err)
	}
	if layout.RootPrefix != "" || layout.KubernetesPrefix != "/registry" || layout.MappingsPrefix != "/vcluster/mappings/" || len(layout.APIServerArgs()) != 0 {
		t.Fatalf("legacy layout changed: %+v", layout)
	}
	if err := c.ValidateSnapshotStorageLayout(); err != nil {
		t.Fatalf("legacy snapshot handling changed: %v", err)
	}
}

func TestResolveEtcdStorageLayoutRejectsUnsafeRoots(t *testing.T) {
	t.Parallel()
	for _, root := range []string{
		"/", "//", "///", "relative", "/tenants//test", "/tenants/./test", "/tenants/../test", "/tenants/test//",
		"/tenants/test\\other", "/tenants/test\x00", "/tenants/test\n", "/tenants/test name", "/tenants/test\u00a0", "/tenants/test\xff",
		"/registry", "/registry/", "/registry/tenant", "/vcluster", "/vcluster/mappings", "/vcluster/snapshot/test",
	} {
		t.Run(root, func(t *testing.T) {
			if _, err := sharedEtcdConfig(root).ResolveEtcdStorageLayout(); err == nil {
				t.Fatal("expected unsafe root to be rejected")
			}
		})
	}
	// Matching is by path boundary, not a common string prefix.
	for _, root := range []string{"/registry-other/test", "/vcluster-other/test"} {
		if _, err := sharedEtcdConfig(root).ResolveEtcdStorageLayout(); err != nil {
			t.Fatalf("rejected non-overlapping root: %v", err)
		}
	}
}

func TestResolveEtcdStorageLayoutRejectsOverrides(t *testing.T) {
	t.Parallel()
	for _, flag := range []string{"etcd-prefix", "etcd-compaction-interval", "etcd-db-metric-poll-interval", "etcd-servers", "etcd-servers-overrides"} {
		for _, args := range [][]string{{"--" + flag + "=value"}, {"--" + flag, "value"}, {"-" + flag + "=value"}, {"--" + strings.ReplaceAll(flag, "-", "_") + "=value"}, {"-" + strings.ReplaceAll(flag, "-", "_"), "value"}} {
			t.Run(strings.Join(args, " "), func(t *testing.T) {
				c := sharedEtcdConfig("/tenants/test")
				c.ControlPlane.Distro.K8S.APIServer.ExtraArgs = args
				if _, err := c.ResolveEtcdStorageLayout(); err == nil {
					t.Fatal("expected conflicting override to be rejected")
				}
			})
		}
	}
	c := sharedEtcdConfig("/tenants/test")
	c.ControlPlane.Distro.K8S.APIServer.ExtraArgs = []string{"--audit-log-path=/tmp/audit.log"}
	if _, err := c.ResolveEtcdStorageLayout(); err != nil {
		t.Fatalf("unrelated flag rejected: %v", err)
	}
}

func TestResolveEtcdStorageLayoutRejectsUnsupportedConfigurations(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*Config){
		"deployed etcd also enabled":     func(c *Config) { c.ControlPlane.BackingStore.Etcd.Deploy.Enabled = true },
		"external database also enabled": func(c *Config) { c.ControlPlane.BackingStore.Database.External.Enabled = true },
		"embedded database also enabled": func(c *Config) { c.ControlPlane.BackingStore.Database.Embedded.Enabled = true },
		"external etcd disabled":         func(c *Config) { c.ControlPlane.BackingStore.Etcd.External.Enabled = false },
		"embedded etcd takes precedence": func(c *Config) { c.ControlPlane.BackingStore.Etcd.Embedded.Enabled = true },
		"K8s disabled":                   func(c *Config) { c.ControlPlane.Distro.K8S.Enabled = false },
		"API server disabled":            func(c *Config) { c.ControlPlane.Distro.K8S.APIServer.Enabled = false },
		"custom command":                 func(c *Config) { c.ControlPlane.Distro.K8S.APIServer.Command = []string{"custom-apiserver"} },
	} {
		t.Run(name, func(t *testing.T) {
			c := sharedEtcdConfig("/tenants/test")
			change(c)
			if _, err := c.ResolveEtcdStorageLayout(); err == nil {
				t.Fatal("unsupported configuration accepted")
			}
		})
	}
}

func TestValidateChangesRejectsStorePrefixMigration(t *testing.T) {
	t.Parallel()
	for _, roots := range [][2]string{{"", "/tenants/a"}, {"/tenants/a", ""}, {"/tenants/a", "/tenants/b"}} {
		if err := ValidateChanges(sharedEtcdConfig(roots[0]), sharedEtcdConfig(roots[1])); err == nil || !strings.Contains(err.Error(), "migration") {
			t.Fatalf("expected migration rejection for %q -> %q, got %v", roots[0], roots[1], err)
		}
	}
	for _, roots := range [][2]string{{"", ""}, {"/tenants/a", "/tenants/a"}, {"/tenants/a", "/tenants/a/"}} {
		if err := ValidateChanges(sharedEtcdConfig(roots[0]), sharedEtcdConfig(roots[1])); err != nil {
			t.Fatalf("unchanged layout rejected: %v", err)
		}
	}
}
