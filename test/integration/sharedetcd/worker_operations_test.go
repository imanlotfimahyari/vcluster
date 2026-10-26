//go:build integration && linux

package sharedetcd_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/loft-sh/vcluster/pkg/config"
	"github.com/loft-sh/vcluster/pkg/tenantbackup"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/utils/ptr"
)

// Invoked only by the managed Pod's readiness probe. Use authenticated TLS;
// process liveness or an open TCP port does not establish API readiness.
func TestVClusterReadinessChild(t *testing.T) {
	path := os.Getenv("VCLUSTER_SHARED_ETCD_READINESS_CONFIG")
	if path == "" {
		t.Skip("managed-Pod probe only")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Timeout = integrationTimeout(3 * time.Second)
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Discovery().RESTClient().Get().AbsPath("/readyz").Do(t.Context()).Error(); err != nil {
		t.Fatal(err)
	}
}

// A real Deployment runs the API/syncer child. Fixture-only hostPath mounts
// preserve generated Kubernetes PKI/data. Host networking reaches loopback etcd.
// This does not qualify the production chart/image or normal host RBAC.
func startManagedWorkerTenant(t *testing.T, lab *etcdLab, host *kubernetes.Clientset, hostConfig string) *runtimeTenant {
	t.Helper()
	ctx := t.Context()
	t.Cleanup(func() {
		if t.Failed() {
			logManagedWorker(t, host, "tenant-a")
		}
	})
	tenant := prepareTenant(t, lab, host, hostConfig, "tenant-a", func(c *config.VirtualClusterConfig) {
		c.ControlPlane.BackingStore.Etcd.External.StorePrefix = ""
		c.ControlPlane.Distro.K8S.APIServer.ExtraArgs = append(c.ControlPlane.Distro.K8S.APIServer.ExtraArgs, "--etcd-compaction-interval=0", "--etcd-db-metric-poll-interval=0")
		tls := &c.ControlPlane.BackingStore.Etcd.External.TLS
		tls.CaFile = "/external-etcd/ca.crt"
		tls.CertFile = "/external-etcd/tls.crt"
		tls.KeyFile = "/external-etcd/tls.key"
		c.Sync.ToHost.Pods.Enabled = true
		c.Sync.ToHost.Secrets.Enabled = true
		c.Sync.ToHost.Services.Enabled = true
		c.Sync.ToHost.PersistentVolumeClaims.Enabled = true
	})
	tlsData := map[string][]byte{}
	for n, path := range map[string]string{"ca.crt": lab.certs["legacy"].CaCert, "tls.crt": lab.certs["legacy"].ServerCert, "tls.key": lab.certs["legacy"].ServerKey} {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		tlsData[n] = b
	}
	if _, err := host.CoreV1().Secrets(tenant.name).Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "dedicated-etcd"}, Data: tlsData}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	raw, err := clientcmd.LoadFromFile(hostConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := clientcmdapi.FlattenConfig(raw); err != nil {
		t.Fatal(err)
	}
	kubeconfig, err := clientcmd.Write(*raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CoreV1().Secrets(tenant.name).Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "worker-host-access"}, Data: map[string][]byte{"kubeconfig": kubeconfig}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	volumes := []corev1.Volume{
		{Name: "external-etcd", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "dedicated-etcd"}}},
		{Name: "config", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "vc-config-" + tenant.name}}},
		{Name: "host-access", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "worker-host-access"}}},
	}
	mounts := []corev1.VolumeMount{{Name: "external-etcd", MountPath: "/external-etcd", ReadOnly: true}, {Name: "config", MountPath: "/etc/vcluster", ReadOnly: true}, {Name: "host-access", MountPath: "/host", ReadOnly: true}}
	dirs := []string{tenant.dataDir, filepath.Dir(tenant.config.Experimental.VirtualClusterKubeConfig.ServerCACert), filepath.Dir(tenant.config.Experimental.VirtualClusterKubeConfig.RequestHeaderCACert)}
	for i, dir := range slices.Compact(dirs) {
		name := fmt.Sprintf("fixture-%d", i)
		volumes = append(volumes, corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: dir, Type: ptr.To(corev1.HostPathDirectory)}}})
		mounts = append(mounts, corev1.VolumeMount{Name: name, MountPath: dir, ReadOnly: dir != tenant.dataDir})
	}
	env := []corev1.EnvVar{{Name: "PATH", Value: "/bin:/binaries"}, {Name: "GOMAXPROCS", Value: "2"}, {Name: "KUBECONFIG", Value: "/host/kubeconfig"}, {Name: "NAMESPACE", Value: tenant.name}, {Name: "POD_NAMESPACE", Value: tenant.name}, {Name: "VCLUSTER_NAME", Value: tenant.name}, {Name: "VCLUSTER_STANDALONE", Value: "false"}, {Name: "VCLUSTER_SHARED_ETCD_ASSETS", Value: "/binaries"}, {Name: "VCLUSTER_SHARED_ETCD_VM", Value: "1"}, {Name: "VCLUSTER_SHARED_ETCD_CHILD_CONFIG", Value: "/etc/vcluster/config.yaml"}, {Name: "VCLUSTER_SHARED_ETCD_CHILD_DATA", Value: tenant.dataDir}, {Name: "VCLUSTER_SHARED_ETCD_READINESS_CONFIG", Value: filepath.Join(tenant.dataDir, "pki/admin.conf")}, {Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}}}
	port, err := strconv.Atoi(tenant.apiClient.CoreV1().RESTClient().Get().URL().Port())
	if err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{"app": "vcluster", "release": tenant.name}
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: tenant.name}, Spec: appsv1.DeploymentSpec{Replicas: ptr.To(int32(1)), Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{HostNetwork: true, DNSPolicy: corev1.DNSDefault, AutomountServiceAccountToken: ptr.To(false), TerminationGracePeriodSeconds: ptr.To(int64(10)), Volumes: volumes, Containers: []corev1.Container{{Name: "syncer", Image: "docker.io/library/vcluster-runtime-test:local", ImagePullPolicy: corev1.PullNever, Command: []string{"/test-runner", "-test.v", "-test.run=^TestVClusterRuntimeChild$"}, Env: env, VolumeMounts: mounts, StartupProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Host: "127.0.0.1", Port: intstr.FromInt(port)}}, PeriodSeconds: 5, FailureThreshold: 90}, ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"/test-runner", "-test.run=^TestVClusterReadinessChild$"}}}, PeriodSeconds: 15, TimeoutSeconds: 25, FailureThreshold: 12}}}}}}}
	if _, err := host.AppsV1().Deployments(tenant.name).Create(ctx, d, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	// Match runtimeTenant.start: seed the default account after backing-API
	// readiness, before proxy readiness. The virtual controllers are disabled.
	if err := wait.PollUntilContextTimeout(ctx, time.Second, integrationTimeout(90*time.Second), true, func(ctx context.Context) (bool, error) {
		return tenant.apiClient.Discovery().RESTClient().Get().AbsPath("/readyz").Do(ctx).Error() == nil, nil
	}); err != nil {
		t.Fatal("managed backing API did not become ready", err)
	}
	if _, err := tenant.apiClient.CoreV1().ServiceAccounts("default").Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Log("managed backing API Ready; default ServiceAccount seeded before proxy readiness")
	waitManagedWorker(t, host, tenant)
	return tenant
}

func logManagedWorker(t *testing.T, host *kubernetes.Clientset, namespace string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	events, _ := host.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
	if events != nil {
		for _, e := range events.Items {
			if e.Reason == "Unhealthy" || e.Reason == "Failed" {
				t.Logf("managed Pod event %s: %.1200s", e.Reason, e.Message)
			}
		}
	}
	pods, err := host.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return
	}
	for _, p := range pods.Items {
		t.Logf("managed Pod %s phase=%s", p.Name, p.Status.Phase)
		for _, c := range p.Status.ContainerStatuses {
			if c.State.Waiting != nil {
				t.Logf("container waiting: %s", c.State.Waiting.Reason)
			}
			if c.LastTerminationState.Terminated != nil {
				t.Logf("last exit: %d", c.LastTerminationState.Terminated.ExitCode)
			}
		}
		if len(p.Spec.Containers) > 0 && p.Spec.Containers[0].Name == "syncer" {
			data, _ := host.CoreV1().Pods(p.Namespace).GetLogs(p.Name, &corev1.PodLogOptions{Container: "syncer", TailLines: ptr.To(int64(35))}).DoRaw(ctx)
			t.Logf("managed runtime log tail: %.6000s", data)
		}
	}
}

func waitManagedWorker(t *testing.T, host *kubernetes.Clientset, tenant *runtimeTenant) types.UID {
	t.Helper()
	var uid types.UID
	if err := wait.PollUntilContextTimeout(t.Context(), time.Second, integrationTimeout(90*time.Second), true, func(ctx context.Context) (bool, error) {
		pods, err := host.CoreV1().Pods(tenant.name).List(ctx, metav1.ListOptions{LabelSelector: "app=vcluster,release=" + tenant.name})
		if err != nil {
			return false, err
		}
		for _, p := range pods.Items {
			for _, c := range p.Status.ContainerStatuses {
				if c.State.Waiting != nil && c.State.Waiting.Reason == "ErrImageNeverPull" {
					return false, fmt.Errorf("managed runtime image absent")
				}
				if c.RestartCount >= 2 && c.LastTerminationState.Terminated != nil && c.LastTerminationState.Terminated.ExitCode != 0 {
					return false, fmt.Errorf("managed runtime repeatedly exited: %d", c.LastTerminationState.Terminated.ExitCode)
				}
			}
		}
		d, err := host.AppsV1().Deployments(tenant.name).Get(ctx, tenant.name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if d.Status.ObservedGeneration < d.Generation || d.Status.ReadyReplicas != 1 || d.Status.UpdatedReplicas != 1 {
			return false, nil
		}
		if err := tenant.client.Discovery().RESTClient().Get().AbsPath("/readyz").Do(ctx).Error(); err != nil {
			return false, nil
		}
		for _, p := range pods.Items {
			if p.DeletionTimestamp == nil {
				uid = p.UID
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal("real managed runtime did not become Ready", err)
	}
	return uid
}

func TestWorkerControllerMigrationAndRecovery(t *testing.T) {
	if os.Getenv("VCLUSTER_SHARED_ETCD_VM") != "1" {
		t.Skip("requires offline VM with runtime image and CLI")
	}
	host, kubeconfig := startK3sHost(t)
	requireWorkerNetwork(t, host, kubeconfig)
	lab := newEtcdLab(t)
	retired := legacyWriter(t, lab)
	a := startManagedWorkerTenant(t, lab, host, kubeconfig)
	oldPod := waitManagedWorker(t, host, a)
	t.Log("real Deployment-managed legacy vCluster Ready")
	// The separate two-runtime worker suite checks another live API/workload.
	// Keep this controller lifecycle fixture bounded: a second authenticated
	// tenant writes and reads its own etcd range throughout the operation.
	foreign := rawEtcdClient(t, lab.endpoint, lab.certs["tenant-b"])
	foreignKey := "/tenants/tenant-b/registry/controller-sentinel"
	foreignProgress := func(value string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), integrationTimeout(5*time.Second))
		defer cancel()
		if _, err := foreign.Put(ctx, foreignKey, value); err != nil {
			t.Fatal("foreign tenant write failed", err)
		}
		got, err := foreign.Get(ctx, foreignKey)
		if err != nil || len(got.Kvs) != 1 || string(got.Kvs[0].Value) != value {
			t.Fatal("foreign tenant value changed", err)
		}
	}
	foreignProgress("before")
	prepareWorkerDNS(t, host, a, 1053)
	volume := prepareWorkerVolume(t, host, a)
	createManagedWorkerApplication(t, a)
	_ = waitScheduledPod(t, a, "server")
	_ = waitScheduledPod(t, a, "client")
	if out, err := workerExec(t.Context(), t, a, kubeconfig, "server", "/bin/busybox", "sh", "-c", `printf '%s' "$1" > /www/durable`, "fixture", "controller-durable"); err != nil {
		t.Fatalf("write actual mounted data: %v: %.400s", err, out)
	}
	createWorkerService(t, a, "web")
	awaitWorkerHTTP(t, a, kubeconfig, "http://web.default.svc.cluster.local/durable", "controller-durable")
	writeWorkerConfigMap(t, a, "snapshot-value")
	cfgSecret, err := host.CoreV1().Secrets(a.name).Get(t.Context(), "vc-config-"+a.name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tlsSecret, err := host.CoreV1().Secrets(a.name).Get(t.Context(), "dedicated-etcd", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "migration.gz")
	run := func(mode, archive string, extra ...string) {
		t.Helper()
		admin := lab.certs["root"]
		args := []string{"debug", "etcd", "tenant-" + mode, archive, "--name", a.name, "--namespace", a.name, "--host-kubeconfig", kubeconfig, "--host-context", "default", "--tls-secret", "dedicated-etcd", "--admin-ca", admin.CaCert, "--admin-cert", admin.ServerCert, "--admin-key", admin.ServerKey, "--issuer-cert", lab.ca.path, "--issuer-key", filepath.Join(lab.ca.dir, "ca.key"), "--timeout", "6m", "--restart-lease-ttl"}
		ctx, cancel := context.WithTimeout(t.Context(), 7*time.Minute)
		defer cancel()
		out, err := exec.CommandContext(ctx, asset(t, "vcluster"), append(args, extra...)...).CombinedOutput()
		if err != nil {
			logManagedWorker(t, host, a.name)
			t.Fatalf("worker %s CLI failed: %v: %.1500s", mode, err, out)
		}
	}
	run("migrate", archive, "--store-prefix", "/tenants/migrated")
	stopped, err := host.AppsV1().Deployments(a.name).Get(t.Context(), a.name, metav1.GetOptions{})
	if err != nil || stopped.Spec.Replicas == nil || *stopped.Spec.Replicas != 0 || stopped.Status.Replicas != 0 {
		t.Fatal("real controller has not stopped workload", err)
	}
	assertPermissionDenied(t, func() error { _, err := retired.Put(t.Context(), "/registry/retired-probe", "denied"); return err }())
	foreignProgress("during-migration")
	t.Log("migration verified: actual scale-down, retired-writer denial and foreign-root progress")
	run("migrate", archive, "--resume", "--commit")
	migratedPod := waitManagedWorker(t, host, a)
	if migratedPod == oldPod {
		t.Fatal("migration did not recreate real control-plane Pod")
	}
	assertWorkerConfigMap(t, a, "snapshot-value")
	awaitWorkerHTTP(t, a, kubeconfig, "http://web.default.svc.cluster.local/durable", "controller-durable")
	volume.verify(t, host, a)
	current, err := host.CoreV1().Secrets(a.name).Get(t.Context(), cfgSecret.Name, metav1.GetOptions{})
	if err != nil || current.UID != cfgSecret.UID {
		t.Fatal("configuration Secret identity changed", err)
	}
	rotated, err := host.CoreV1().Secrets(a.name).Get(t.Context(), tlsSecret.Name, metav1.GetOptions{})
	if err != nil || rotated.UID != tlsSecret.UID || string(rotated.Data["tls.crt"]) == string(tlsSecret.Data["tls.crt"]) {
		t.Fatal("migration did not rotate mounted TLS in place", err)
	}
	t.Log("migration cutover recreated Ready runtime; API value, DNS and mounted bytes survived")
	recovery := filepath.Join(t.TempDir(), "recovery.gz")
	if _, err := tenantbackup.Export(t.Context(), lab.admin, "/tenants/migrated/", recovery); err != nil {
		t.Fatal(err)
	}
	writeWorkerConfigMap(t, a, "changed-after-backup")
	run("recover", recovery, "--new-operation", "--commit")
	recoveredPod := waitManagedWorker(t, host, a)
	if recoveredPod == migratedPod {
		t.Fatal("recovery did not recreate control-plane Pod")
	}
	assertWorkerConfigMap(t, a, "snapshot-value")
	recoveredTLS, err := host.CoreV1().Secrets(a.name).Get(t.Context(), tlsSecret.Name, metav1.GetOptions{})
	if err != nil || recoveredTLS.UID != tlsSecret.UID || string(recoveredTLS.Data["tls.crt"]) == string(rotated.Data["tls.crt"]) {
		t.Fatal("recovery did not rotate mounted TLS in place", err)
	}
	for _, name := range []string{"client", "server"} {
		replaceWorkerPod(t, host, a, name)
	}
	createWorkerService(t, a, "after-recovery")
	awaitWorkerHTTP(t, a, kubeconfig, "http://after-recovery.default.svc.cluster.local/durable", "controller-durable")
	volume.verify(t, host, a)
	foreignProgress("after-recovery")
	if result, err := lab.admin.Get(t.Context(), "/registry/configmaps/default/same-name"); err != nil || len(result.Kvs) != 1 {
		t.Fatal("migration source was not retained", err)
	}
	t.Log("controller recovery restored API data and rotated credentials; original volume data survived both Pod replacements")
}

func createManagedWorkerApplication(t *testing.T, tenant *runtimeTenant) {
	t.Helper()
	for _, name := range []string{"server", "client"} {
		c := corev1.Container{Name: "busybox", Image: "docker.io/library/vcluster-test:local", ImagePullPolicy: corev1.PullNever, Command: []string{"/bin/busybox", "sleep", "3600"}}
		p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"app": name}}, Spec: corev1.PodSpec{AutomountServiceAccountToken: ptr.To(false), DNSPolicy: corev1.DNSClusterFirst}}
		if name == "server" {
			c.Command = []string{"/bin/busybox", "httpd", "-f", "-p", "8080", "-h", "/www"}
			c.VolumeMounts = []corev1.VolumeMount{{Name: "data", MountPath: "/www"}}
			p.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}}}
		}
		p.Spec.Containers = []corev1.Container{c}
		if _, err := tenant.client.CoreV1().Pods("default").Create(t.Context(), p, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
}

func writeWorkerConfigMap(t *testing.T, tenant *runtimeTenant, value string) {
	t.Helper()
	ctx := t.Context()
	current, err := tenant.client.CoreV1().ConfigMaps("default").Get(ctx, "same-name", metav1.GetOptions{})
	if kerrors.IsNotFound(err) {
		_, err = tenant.client.CoreV1().ConfigMaps("default").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "same-name"}, Data: map[string]string{"tenant": value}}, metav1.CreateOptions{})
	} else if err == nil {
		current.Data = map[string]string{"tenant": value}
		_, err = tenant.client.CoreV1().ConfigMaps("default").Update(ctx, current, metav1.UpdateOptions{})
	}
	if err != nil {
		t.Fatal(err)
	}
}
func assertWorkerConfigMap(t *testing.T, tenant *runtimeTenant, value string) {
	t.Helper()
	cm, err := tenant.client.CoreV1().ConfigMaps("default").Get(t.Context(), "same-name", metav1.GetOptions{})
	if err != nil || cm.Data["tenant"] != value {
		t.Fatal("recovered virtual ConfigMap has wrong value", err)
	}
}
