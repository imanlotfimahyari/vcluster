package snapshot

import (
	"strings"
	"testing"

	"github.com/loft-sh/vcluster/pkg/config"
)

func TestTenantLayoutRejectsSnapshotAndRestoreBeforeSideEffects(t *testing.T) {
	t.Parallel()
	cfg := &config.VirtualClusterConfig{}
	cfg.ControlPlane.BackingStore.Etcd.External.StorePrefix = "/tenants/test"
	// Standalone setup and option validation must not run first. Invalid
	// storage options would otherwise produce a different error or touch disk.
	cfg.ControlPlane.Standalone.Enabled = true
	for name, run := range map[string]func() error{
		"snapshot": func() error { return (&Client{}).Run(t.Context(), cfg) },
		"restore":  func() error { return (&RestoreClient{}).Run(t.Context(), cfg) },
	} {
		t.Run(name, func(t *testing.T) {
			if err := run(); err == nil || !strings.Contains(err.Error(), "tenant-scoped") {
				t.Fatalf("expected early tenant-layout rejection, got %v", err)
			}
		})
	}
}
