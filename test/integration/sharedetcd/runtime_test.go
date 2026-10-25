//go:build integration && linux

package sharedetcd_test

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	vclustercmd "github.com/loft-sh/vcluster/cmd/vcluster/cmd"
	debugetcd "github.com/loft-sh/vcluster/cmd/vcluster/cmd/debug/etcd"
	debugmappings "github.com/loft-sh/vcluster/cmd/vcluster/cmd/debug/mappings"
	rawconfig "github.com/loft-sh/vcluster/config"
	"github.com/loft-sh/vcluster/pkg/config"
	"github.com/loft-sh/vcluster/pkg/constants"
	"github.com/loft-sh/vcluster/pkg/etcd"
	"github.com/loft-sh/vcluster/pkg/mappings/store"
	"github.com/loft-sh/vcluster/pkg/plugin"
	pluginv1 "github.com/loft-sh/vcluster/pkg/plugin/v1"
	pluginv2 "github.com/loft-sh/vcluster/pkg/plugin/v2"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/yaml"
)

// TestVClusterRuntimeChild is invoked only as a subprocess. Remapping filesystem
// paths and ports lets the real in-cluster runtime run without containers. It
// does not replace the mappings backend, syncers, Kubernetes APIs, or etcd.
func TestVClusterRuntimeChild(t *testing.T) {
	configPath := os.Getenv("VCLUSTER_SHARED_ETCD_CHILD_CONFIG")
	if configPath == "" {
		t.Skip("subprocess-only helper")
	}
	dataDir := os.Getenv("VCLUSTER_SHARED_ETCD_CHILD_DATA")
	for _, path := range []*string{
		&constants.K3sKineEndpoint, &constants.K3sSqliteDatabase, &constants.DataDir, &constants.PKIDir,
		&constants.K8sKineEndpoint, &constants.K8sSqliteDatabase, &constants.EmbeddedEtcdData, &constants.EmbeddedCoreDNSAdminConf,
		&constants.ServerCAKey, &constants.ServerCACert, &constants.ClientCACert, &constants.ClientCAKey, &constants.RequestHeaderCACert,
		&constants.FrontProxyClientCert, &constants.FrontProxyClientKey, &constants.SAKey, &constants.SACert,
		&constants.APIServerCert, &constants.APIServerKey, &constants.APIServerKubeletClientCert, &constants.APIServerKubeletClientKey,
		&constants.AdminKubeConfig, &constants.ControllerManagerConf, &constants.SchedulerConf,
	} {
		*path = strings.Replace(*path, "/data", dataDir, 1)
	}
	ctrl.SetLogger(klog.NewKlogr())
	constants.K8sAPIServerBinary = asset(t, "kube-apiserver")
	plugin.DefaultManager = &isolatedPluginManager{legacy: pluginv1.NewManager(), modern: pluginv2.NewManager()}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	var err error
	switch os.Getenv("VCLUSTER_SHARED_ETCD_CHILD_ACTION") {
	case "keys":
		err = debugetcd.ExecuteKeys(ctx, &debugetcd.KeysOptions{Config: configPath, Prefix: "/"})
	case "clear":
		err = debugmappings.ExecuteClear(ctx, &debugmappings.ClearOptions{Config: configPath})
	default:
		err = vclustercmd.StartInCluster(ctx, &vclustercmd.StartOptions{Config: configPath})
	}
	if err != nil {
		t.Fatal(err)
	}
}

func kubeRestConfig(endpoint string, certs *etcd.Certificates) *rest.Config {
	return &rest.Config{Host: endpoint, TLSClientConfig: rest.TLSClientConfig{CAFile: certs.CaCert, CertFile: certs.ServerCert, KeyFile: certs.ServerKey}, QPS: 100, Burst: 200, Timeout: integrationTimeout(5 * time.Second)}
}

func writeKubeconfig(t *testing.T, path, endpoint, namespace string, certs *etcd.Certificates) {
	t.Helper()
	raw := clientcmdapi.Config{Clusters: map[string]*clientcmdapi.Cluster{"test": {Server: endpoint, CertificateAuthority: certs.CaCert}}, AuthInfos: map[string]*clientcmdapi.AuthInfo{"test": {ClientCertificate: certs.ServerCert, ClientKey: certs.ServerKey}}, Contexts: map[string]*clientcmdapi.Context{"test": {Cluster: "test", AuthInfo: "test", Namespace: namespace}}, CurrentContext: "test"}
	data, err := clientcmd.Write(raw)
	if err != nil {
		t.Fatal(err)
	}
	writeFixture(t, path, data)
}

func waitAPI(t *testing.T, process *testProcess, client kubernetes.Interface) {
	t.Helper()
	if err := wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, integrationTimeout(45*time.Second), true, func(ctx context.Context) (bool, error) {
		if err := process.check(); err != nil {
			return false, err
		}
		return client.Discovery().RESTClient().Get().AbsPath("/readyz").Do(ctx).Error() == nil, nil
	}); err != nil {
		t.Fatalf("Kubernetes API did not become ready: %v", err)
	}
}

func startHostAPI(t *testing.T, lab *etcdLab) (*kubernetes.Clientset, string, *etcd.Certificates) {
	t.Helper()
	kubeCA := newTestCA(t)
	server := kubeCA.identity(t, "host-api", true)
	admin := kubeCA.identity(t, "test-admin", false, "system:masters")
	endpoint := fmt.Sprintf("https://127.0.0.1:%d", unusedPort(t))
	_, port, _ := strings.Cut(strings.TrimPrefix(endpoint, "https://"), ":")
	root := lab.certs["root"]
	process := startProcess(t, "host-api", os.Environ(), asset(t, "kube-apiserver"),
		"--bind-address=127.0.0.1", "--advertise-address=127.0.0.1", "--secure-port="+port,
		"--etcd-servers="+lab.endpoint, "--etcd-prefix=/host/registry", "--etcd-cafile="+root.CaCert, "--etcd-certfile="+root.ServerCert, "--etcd-keyfile="+root.ServerKey, "--etcd-compaction-interval=0",
		"--client-ca-file="+kubeCA.path, "--tls-cert-file="+server.ServerCert, "--tls-private-key-file="+server.ServerKey,
		"--service-account-issuer=https://kubernetes.default.svc", "--service-account-signing-key-file="+server.ServerKey, "--service-account-key-file="+server.ServerCert,
		"--service-cluster-ip-range=10.96.0.0/12", "--authorization-mode=RBAC", "--endpoint-reconciler-type=none", "--disable-admission-plugins=ServiceAccount", "--profiling=false")
	client, err := kubernetes.NewForConfig(kubeRestConfig(endpoint, admin))
	if err != nil {
		t.Fatal(err)
	}
	waitAPI(t, process, client)
	kubeconfig := filepath.Join(t.TempDir(), "host.conf")
	writeKubeconfig(t, kubeconfig, endpoint, "default", admin)
	return client, kubeconfig, admin
}

type runtimeTenant struct {
	name, configPath, dataDir, hostKubeconfig string
	config                                    *config.VirtualClusterConfig
	client                                    *kubernetes.Clientset
	apiClient                                 *kubernetes.Clientset
	process                                   *testProcess
}

func prepareTenant(t *testing.T, lab *etcdLab, host *kubernetes.Clientset, hostKubeconfig string, name string, configure ...func(*config.VirtualClusterConfig)) *runtimeTenant {
	t.Helper()
	kubeCA, requestHeaderCA := newTestCA(t), newTestCA(t)
	admin := kubeCA.identity(t, "virtual-admin", false, "system:masters")
	cfg := lab.config(t, name)
	cfg.ServiceCIDR = "10.96.0.0/12"
	cfg.ControlPlane.Distro.K8S.ControllerManager.Enabled = false
	cfg.ControlPlane.Distro.K8S.Scheduler.Enabled = false
	cfg.ControlPlane.CoreDNS.Enabled = false
	cfg.Experimental.SyncSettings.SetOwner = false
	cfg.Sync = rawconfig.Sync{}
	cfg.Sync.ToHost.ConfigMaps.Enabled = true
	cfg.Sync.ToHost.ConfigMaps.All = true
	cfg.ControlPlane.Proxy.BindAddress = "127.0.0.1"
	cfg.ControlPlane.Proxy.Port = unusedPort(t)
	apiPort := unusedPort(t)
	endpoint := fmt.Sprintf("https://127.0.0.1:%d", apiPort)
	cfg.ControlPlane.Distro.K8S.APIServer.ExtraArgs = []string{fmt.Sprintf("--secure-port=%d", apiPort), "--disable-admission-plugins=ServiceAccount"}
	dataDir := t.TempDir()
	pkiDir := filepath.Join(dataDir, "pki")
	if err := os.MkdirAll(pkiDir, 0700); err != nil {
		t.Fatal(err)
	}
	server := kubeCA.identity(t, name+"-api", true)
	frontProxy := requestHeaderCA.identity(t, "front-proxy-client", false)
	copyFile := func(src, dst string) {
		data, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		writeFixture(t, filepath.Join(pkiDir, dst), data)
	}
	copyFile(server.ServerCert, "apiserver.crt")
	copyFile(server.ServerKey, "apiserver.key")
	copyFile(server.ServerCert, "sa.pub")
	copyFile(server.ServerKey, "sa.key")
	copyFile(frontProxy.ServerCert, "front-proxy-client.crt")
	copyFile(frontProxy.ServerKey, "front-proxy-client.key")
	adminConfig := filepath.Join(pkiDir, "admin.conf")
	writeKubeconfig(t, adminConfig, endpoint, "default", admin)
	cfg.Experimental.VirtualClusterKubeConfig = rawconfig.VirtualClusterKubeConfig{KubeConfig: adminConfig, ServerCACert: kubeCA.path, ServerCAKey: filepath.Join(kubeCA.dir, "ca.key"), ClientCACert: kubeCA.path, ClientCAKey: filepath.Join(kubeCA.dir, "ca.key"), RequestHeaderCACert: requestHeaderCA.path}
	for _, apply := range configure {
		apply(cfg)
	}
	configBytes, err := yaml.Marshal(cfg.Config)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dataDir, "config.yaml")
	writeFixture(t, configPath, configBytes)
	ctx := t.Context()
	if _, err := host.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.CoreV1().Secrets(name).Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "vc-config-" + name}, Data: map[string][]byte{"config.yaml": configBytes}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.CoreV1().Services(name).Create(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "https", Port: 443}}}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	client, err := kubernetes.NewForConfig(kubeRestConfig(endpoint, admin))
	if err != nil {
		t.Fatal(err)
	}
	proxyClient, err := kubernetes.NewForConfig(kubeRestConfig(fmt.Sprintf("https://127.0.0.1:%d", cfg.ControlPlane.Proxy.Port), admin))
	if err != nil {
		t.Fatal(err)
	}
	return &runtimeTenant{name: name, configPath: configPath, dataDir: dataDir, hostKubeconfig: hostKubeconfig, config: cfg, client: proxyClient, apiClient: client}
}

func (tenant *runtimeTenant) run(t *testing.T, action string) *testProcess {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	env := []string{"PATH=" + os.Getenv("PATH"), "GOMAXPROCS=2", "KUBECONFIG=" + tenant.hostKubeconfig, "NAMESPACE=" + tenant.name, "POD_NAMESPACE=" + tenant.name, "POD_NAME=" + tenant.name + "-0", "VCLUSTER_NAME=" + tenant.name, "VCLUSTER_STANDALONE=false",
		"VCLUSTER_SHARED_ETCD_ASSETS=" + os.Getenv("VCLUSTER_SHARED_ETCD_ASSETS"), "VCLUSTER_SHARED_ETCD_CHILD_CONFIG=" + tenant.configPath, "VCLUSTER_SHARED_ETCD_CHILD_DATA=" + tenant.dataDir, "VCLUSTER_SHARED_ETCD_CHILD_ACTION=" + action}
	return startProcess(t, tenant.name+"-"+action, env, executable, "-test.run=^TestVClusterRuntimeChild$", "-test.v")
}

func (tenant *runtimeTenant) start(t *testing.T) {
	t.Helper()
	tenant.process = tenant.run(t, "start")
	waitAPI(t, tenant.process, tenant.apiClient)
	_, err := tenant.apiClient.CoreV1().ServiceAccounts("default").Create(t.Context(), &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default"}}, metav1.CreateOptions{})
	if err != nil && !kerrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	waitAPI(t, tenant.process, tenant.client)
}

func awaitHostConfigMap(t *testing.T, host *kubernetes.Clientset, tenant *runtimeTenant, value string) string {
	t.Helper()
	var name string
	err := wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, integrationTimeout(45*time.Second), true, func(ctx context.Context) (bool, error) {
		if err := tenant.process.check(); err != nil {
			return false, err
		}
		list, err := host.CoreV1().ConfigMaps(tenant.name).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		for _, cm := range list.Items {
			if cm.Data["tenant"] == value {
				name = cm.Name
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("config map did not sync for %s: %v", tenant.name, err)
	}
	return name
}

func TestVClustersShareAuthenticatedEtcd(t *testing.T) {
	exerciseSharedEtcdRuntime(t)
}

// Writes must converge as soon as the tenant proxy accepts them after restart.
func TestWritesDuringControllerStartup(t *testing.T) {
	exerciseSharedEtcdRuntime(t)
}

func exerciseSharedEtcdRuntime(t *testing.T) {
	t.Helper()
	lab := newEtcdLab(t)
	_ = asset(t, "kube-apiserver")
	host, hostKubeconfig, _ := startHostAPI(t, lab)
	a := prepareTenant(t, lab, host, hostKubeconfig, "tenant-a")
	b := prepareTenant(t, lab, host, hostKubeconfig, "tenant-b")
	a.start(t)
	b.start(t)
	for _, tenant := range []*runtimeTenant{a, b} {
		_, err := tenant.client.CoreV1().ConfigMaps("default").Create(t.Context(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "same-name"}, Data: map[string]string{"tenant": tenant.name}}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Log("both virtual APIs are running; checking host synchronization")
	aHostName := awaitHostConfigMap(t, host, a, "tenant-a")
	bHostName := awaitHostConfigMap(t, host, b, "tenant-b")
	if aHostName == "" || bHostName == "" {
		t.Fatal("missing physical config maps")
	}

	// Restart one complete vCluster process group (API server and syncer),
	// retaining its etcd keys and local configuration. The other stays running.
	t.Log("both tenants synchronized identical names; restarting tenant A")
	a.process.stop()
	a.start(t)
	// Make the update immediately after proxy readiness, before waiting for
	// another reconciliation or interacting with the other tenant.
	cm, err := a.client.CoreV1().ConfigMaps("default").Get(t.Context(), "same-name", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cm.Data["tenant"] = "tenant-a-updated"
	if _, err := a.client.CoreV1().ConfigMaps("default").Update(t.Context(), cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if name := awaitHostConfigMap(t, host, a, "tenant-a-updated"); name != aHostName {
		t.Fatal("restart changed the physical mapping")
	}
	if name := awaitHostConfigMap(t, host, b, "tenant-b"); name != bHostName {
		t.Fatal("restart affected the other tenant")
	}
	t.Log("checking updates after restart")
	bValue := "tenant-b-after-restart"
	bCM, err := b.client.CoreV1().ConfigMaps("default").Get(t.Context(), "same-name", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bCM.Data["tenant"] = bValue
	if _, err := b.client.CoreV1().ConfigMaps("default").Update(t.Context(), bCM, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if name := awaitHostConfigMap(t, host, b, bValue); name != bHostName {
		t.Fatal("B mapping changed after A restart")
	}
	other, err := b.client.CoreV1().ConfigMaps("default").Get(t.Context(), "same-name", metav1.GetOptions{})
	if err != nil || other.Data["tenant"] != bValue {
		t.Fatalf("update crossed tenant boundary: %v", err)
	}
	if err := a.client.CoreV1().ConfigMaps("default").Delete(t.Context(), "same-name", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, 15*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := host.CoreV1().ConfigMaps(a.name).Get(ctx, aHostName, metav1.GetOptions{})
		if kerrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}); err != nil {
		t.Fatalf("host deletion did not complete: %v", err)
	}
	_ = awaitHostConfigMap(t, host, b, bValue)

	// Stop A before clearing persisted mappings. The actual debug command must
	// select A's range even while B continues synchronizing against the store.
	a.process.stop()
	for _, action := range []string{"keys", "clear"} {
		p := a.run(t, action)
		select {
		case <-p.done:
			if p.err != nil {
				t.Fatalf("debug %s failed: %v", action, p.err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("debug %s timed out", action)
		}
		if action == "keys" {
			out, err := os.ReadFile(p.logPath)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(out), "/tenants/tenant-a/") || strings.Contains(string(out), "/tenants/tenant-b/") {
				t.Fatal("debug keys escaped tenant scope")
			}
		}
	}
	wrapped, err := etcd.NewFromConfig(t.Context(), b.config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wrapped.Close() })
	layout, err := b.config.ResolveEtcdStorageLayout()
	if err != nil {
		t.Fatal(err)
	}
	mappings, err := store.NewEtcdBackend(wrapped, layout.MappingsPrefix).List(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, mapping := range mappings {
		if mapping.VirtualName.Namespace == "default" && mapping.VirtualName.Name == "same-name" && strings.EqualFold(mapping.Kind, "ConfigMap") && mapping.HostName.Name == bHostName {
			found = true
		}
	}
	if !found {
		t.Fatal("clearing A removed B's persisted config map mapping")
	}
	_ = awaitHostConfigMap(t, host, b, bValue)
	if os.Getenv("VCLUSTER_SHARED_ETCD_REQUIRE_STATUS_FALLBACK") == "1" {
		for _, tenant := range []*runtimeTenant{a, b} {
			data, err := os.ReadFile(tenant.process.logPath)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(data), "etcd Status is not authorized; disabling RequestWatchProgress optimization") || strings.Count(string(data), "/etcdserverpb.Maintenance/Status") > 1 {
				t.Fatalf("%s did not use the terminal permission-denied fallback", tenant.name)
			}
		}
	}
}
