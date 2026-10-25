package config

import (
	"strings"
	"testing"
)

func TestValidateConfigRejectsUnsafeStorageLayoutFirst(t *testing.T) {
	t.Parallel()
	vConfig := &VirtualClusterConfig{}
	vConfig.ControlPlane.BackingStore.Etcd.External.StorePrefix = "/"
	if err := ValidateConfigAndSetDefaults(vConfig); err == nil || !strings.Contains(err.Error(), "storePrefix") {
		t.Fatalf("expected storage layout rejection, got %v", err)
	}
}
