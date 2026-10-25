//go:build integration && linux

package sharedetcd_test

import (
	"context"
	"net/http"

	"github.com/loft-sh/vcluster/pkg/config"
	plugintypes "github.com/loft-sh/vcluster/pkg/plugin/types"
	pluginv1 "github.com/loft-sh/vcluster/pkg/plugin/v1"
	pluginv2 "github.com/loft-sh/vcluster/pkg/plugin/v2"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// isolatedPluginManager delegates to the real plugin managers. The only change
// from pkg/plugin's wiring is a dynamically allocated legacy listener port,
// because the normal fixed port assumes separate pod network namespaces.
// No external plugins are installed by this harness.
type isolatedPluginManager struct {
	legacy *pluginv1.Manager
	modern *pluginv2.Manager
}

var _ plugintypes.Manager = (*isolatedPluginManager)(nil)

func (m *isolatedPluginManager) Start(ctx context.Context, virtual *rest.Config, raw *clientcmdapi.Config, cfg *config.VirtualClusterConfig) error {
	options, err := cfg.LegacyOptions()
	if err != nil {
		return err
	}
	options.PluginListenAddress = "127.0.0.1:0"
	if err := m.legacy.Start(ctx, cfg.HostNamespace, cfg.HostNamespace, virtual, cfg.HostConfig, raw, options); err != nil {
		return err
	}
	return m.modern.Start(ctx, raw, cfg)
}
func (m *isolatedPluginManager) SetLeader(ctx context.Context) error {
	m.legacy.SetLeader(true)
	return m.modern.SetLeader(ctx)
}
func (m *isolatedPluginManager) MutateObject(ctx context.Context, obj client.Object, hook string, scheme *runtime.Scheme) error {
	if err := m.legacy.MutateObject(ctx, obj, hook, scheme); err != nil {
		return err
	}
	return m.modern.MutateObject(ctx, obj, hook, scheme)
}
func (m *isolatedPluginManager) HasClientHooks() bool {
	return m.legacy.HasClientHooks() || m.modern.HasClientHooks()
}
func (m *isolatedPluginManager) HasClientHooksForType(kind plugintypes.VersionKindType) bool {
	return m.legacy.HasClientHooksForType(kind) || m.modern.HasClientHooksForType(kind)
}
func (m *isolatedPluginManager) HasPlugins() bool {
	return m.legacy.HasPlugins() || m.modern.HasPlugins()
}
func (m *isolatedPluginManager) SetProFeatures(features map[string]bool) {
	m.modern.ProFeatures = features
}
func (m *isolatedPluginManager) WithInterceptors(next http.Handler) http.Handler {
	return m.modern.WithInterceptors(next)
}
