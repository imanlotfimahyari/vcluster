package config

import (
	"fmt"
	"path"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	DefaultKubernetesPrefix = "/registry"
	DefaultMappingsPrefix   = "/vcluster/mappings/"
)

// EtcdStorageLayout contains the resolved storage paths for one vCluster.
// RootPrefix is empty for the legacy layout, where API-server extra arguments
// retain their existing behavior.
type EtcdStorageLayout struct {
	RootPrefix       string
	KubernetesPrefix string
	MappingsPrefix   string
}

// ResolveEtcdStorageLayout validates the opt-in external-etcd layout without
// mutating the config. A nonempty root also disables API-server compaction;
// the shared etcd administrator must provide a central compaction policy.
func (c *Config) ResolveEtcdStorageLayout() (EtcdStorageLayout, error) {
	layout := EtcdStorageLayout{
		KubernetesPrefix: DefaultKubernetesPrefix,
		MappingsPrefix:   DefaultMappingsPrefix,
	}
	root := c.ControlPlane.BackingStore.Etcd.External.StorePrefix
	if root == "" {
		return layout, nil
	}

	// Accept one optional trailing slash, but never silently clean an ambiguous
	// path: authorization ranges must agree with the configured storage paths.
	root = strings.TrimSuffix(root, "/")
	if root == "" || root == "/" || !utf8.ValidString(root) || !strings.HasPrefix(root, "/") || path.Clean(root) != root ||
		strings.Contains(root, "\\") || strings.ContainsAny(root, "\x00") ||
		strings.ContainsFunc(root, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
		return EtcdStorageLayout{}, fmt.Errorf("external etcd storePrefix must be an absolute, canonical non-root path")
	}
	// Legacy keyspaces are reserved. Otherwise a broad tenant range could
	// include an existing deployment's objects, mappings, or archive metadata.
	for _, reserved := range []string{"/registry", "/vcluster"} {
		if root == reserved || strings.HasPrefix(root, reserved+"/") || strings.HasPrefix(reserved, root+"/") {
			return EtcdStorageLayout{}, fmt.Errorf("external etcd storePrefix overlaps a reserved legacy keyspace")
		}
	}
	backingStore := c.ControlPlane.BackingStore
	if c.BackingStoreType() != StoreTypeExternalEtcd || backingStore.Etcd.Deploy.Enabled ||
		backingStore.Database.External.Enabled || backingStore.Database.Embedded.Enabled {
		return EtcdStorageLayout{}, fmt.Errorf("external etcd storePrefix requires the external etcd backing store")
	}
	apiServer := c.ControlPlane.Distro.K8S.APIServer
	if !c.ControlPlane.Distro.K8S.Enabled || !apiServer.Enabled || len(apiServer.Command) > 0 {
		return EtcdStorageLayout{}, fmt.Errorf("external etcd storePrefix requires the managed K8s API server without a custom command")
	}

	// Reject flag overrides with attached or separate values, including
	// single-dash spellings. Duplicates must not bypass the derived layout.
	for _, arg := range apiServer.ExtraArgs {
		if !strings.HasPrefix(arg, "-") {
			continue
		}
		flag, _, _ := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		// Kubernetes normalizes underscore separators before parsing flags.
		// Validate the same spelling so aliases cannot override tenant guards.
		flag = strings.ReplaceAll(flag, "_", "-")
		switch flag {
		case "etcd-prefix", "etcd-compaction-interval", "etcd-db-metric-poll-interval", "etcd-servers", "etcd-servers-overrides":
			return EtcdStorageLayout{}, fmt.Errorf("API-server flag --%s cannot be overridden when external etcd storePrefix is set", flag)
		}
	}

	layout.RootPrefix = root + "/"
	layout.KubernetesPrefix = root + "/registry"
	layout.MappingsPrefix = root + "/mappings/"
	return layout, nil
}

// APIServerArgs returns flags managed by the tenant storage layout. Returning
// no flags in legacy mode preserves existing API-server argument precedence.
func (l EtcdStorageLayout) APIServerArgs() []string {
	if l.RootPrefix == "" {
		return nil
	}
	return []string{
		"--etcd-prefix=" + l.KubernetesPrefix,
		"--etcd-compaction-interval=0",
		// Database status is cluster-wide and requires privileges beyond a
		// tenant's key range. The shared etcd operator owns these metrics.
		"--etcd-db-metric-poll-interval=0",
	}
}

// ValidateSnapshotStorageLayout prevents unscoped lifecycle operations until
// tenant-scoped archives and restore are implemented. Call before side effects.
func (c *Config) ValidateSnapshotStorageLayout() error {
	if c.ControlPlane.BackingStore.Etcd.External.StorePrefix != "" {
		return fmt.Errorf("built-in snapshot and restore are not supported with external etcd storePrefix; tenant-scoped backup and restore are required")
	}
	return nil
}

func validateStorePrefixChange(oldCfg, newCfg *Config) error {
	oldRoot := strings.TrimSuffix(oldCfg.ControlPlane.BackingStore.Etcd.External.StorePrefix, "/")
	newRoot := strings.TrimSuffix(newCfg.ControlPlane.BackingStore.Etcd.External.StorePrefix, "/")
	if oldRoot != newRoot {
		return fmt.Errorf("external etcd storePrefix cannot be changed on an existing vCluster; an explicit offline storage migration is required")
	}
	return nil
}
