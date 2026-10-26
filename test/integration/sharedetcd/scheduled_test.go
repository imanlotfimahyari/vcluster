//go:build integration && linux

package sharedetcd_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/loft-sh/vcluster/pkg/config"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
)

func startK3sHost(t *testing.T) (*kubernetes.Clientset, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "k3s.yaml")
	p := startProcess(t, "k3s-host", os.Environ(), asset(t, "k3s"), "server",
		"--write-kubeconfig="+path, "--disable=traefik,servicelb,metrics-server,local-storage",
		"--disable-helm-controller", "--disable-cloud-controller", "--service-cidr=10.96.0.0/12",
		"--flannel-backend=host-gw", "--node-ip=192.0.2.1", "--flannel-iface=eth0",
		"--kubelet-arg=cgroup-driver=cgroupfs", "--resolv-conf=/etc/resolv.conf", "--prefer-bundled-bin")
	var client *kubernetes.Clientset
	if err := wait.PollUntilContextTimeout(t.Context(), time.Second, 8*time.Minute, true, func(ctx context.Context) (bool, error) {
		if err := p.check(); err != nil {
			return false, err
		}
		if _, err := os.Stat(path); err != nil {
			return false, nil
		}
		restConfig, err := clientcmd.BuildConfigFromFlags("", path)
		if err != nil {
			return false, nil
		}
		restConfig.Timeout = 30 * time.Second
		restConfig.QPS = 100
		restConfig.Burst = 200
		client, err = kubernetes.NewForConfig(restConfig)
		if err != nil {
			return false, err
		}
		nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, nil
		}
		for _, node := range nodes.Items {
			for _, condition := range node.Status.Conditions {
				if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
					return true, nil
				}
			}
		}
		return false, nil
	}); err != nil {
		t.Fatalf("offline K3s host did not become ready: %v", err)
	}
	return client, path
}

func TestScheduledPodsAndCNI(t *testing.T) {
	if os.Getenv("VCLUSTER_SHARED_ETCD_VM") != "1" {
		t.Skip("requires the disposable offline VM runner")
	}
	host, hostKubeconfig := startK3sHost(t)
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout(10*time.Second))
		defer cancel()
		for _, ns := range []string{"worker-network-probe", "tenant-a", "tenant-b"} {
			pods, err := host.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
			if err != nil {
				continue
			}
			for _, p := range pods.Items {
				t.Logf("worker Pod %s/%s phase=%s", ns, p.Name, p.Status.Phase)
				for _, c := range p.Status.ContainerStatuses {
					reason := ""
					if c.State.Waiting != nil {
						reason = c.State.Waiting.Reason
					}
					t.Logf("container %s ready=%t restarts=%d waiting=%s", c.Name, c.Ready, c.RestartCount, reason)
				}
			}
		}
	})
	requireWorkerNetwork(t, host, hostKubeconfig)
	t.Log("offline Kubernetes worker passed network preflight; starting authenticated shared etcd")
	lab := newEtcdLab(t)
	configure := func(c *config.VirtualClusterConfig) {
		c.Sync.ToHost.Pods.Enabled = true
		c.Sync.ToHost.Secrets.Enabled = true
		c.Sync.ToHost.Services.Enabled = true
		c.Sync.ToHost.PersistentVolumeClaims.Enabled = true
	}
	a := prepareTenant(t, lab, host, hostKubeconfig, "tenant-a", configure)
	b := prepareTenant(t, lab, host, hostKubeconfig, "tenant-b", configure)
	for _, tenant := range []*runtimeTenant{a, b} {
		t.Logf("starting %s control plane", tenant.name)
		tenant.start(t)
	}
	volumes := map[string]workerVolume{}
	for i, tenant := range []*runtimeTenant{a, b} {
		prepareWorkerDNS(t, host, tenant, int32(1053+i))
		volumes[tenant.name] = prepareWorkerVolume(t, host, tenant)
		for _, name := range []string{"server", "client"} {
			command := []string{"/bin/busybox", "sleep", "3600"}
			container := corev1.Container{Name: "busybox", Image: "docker.io/library/vcluster-test:local", ImagePullPolicy: corev1.PullNever}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"app": name}}, Spec: corev1.PodSpec{DNSPolicy: corev1.DNSClusterFirst, AutomountServiceAccountToken: ptr.To(false)}}
			if name == "server" {
				// Do not recreate the durable value on restart: a lost volume must fail.
				command = []string{"/bin/busybox", "sh", "-c", "test -f /www/index.html || echo " + tenant.name + " > /www/index.html; exec /bin/busybox httpd -f -p 8080 -h /www"}
				container.VolumeMounts = []corev1.VolumeMount{{Name: "data", MountPath: "/www"}}
				pod.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}}}
			}
			container.Command = command
			pod.Spec.Containers = []corev1.Container{container}
			if _, err := tenant.client.CoreV1().Pods("default").Create(t.Context(), pod, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		createWorkerService(t, tenant, "web")
		t.Logf("%s application Pods created; waiting for real scheduling", tenant.name)
		server := waitScheduledPod(t, tenant, "server")
		_ = waitScheduledPod(t, tenant, "client")
		if server.Status.PodIP == "" || server.Spec.NodeName == "" {
			t.Fatal("virtual Pod lacks real scheduling/network status")
		}
		if output, err := workerExec(t.Context(), t, tenant, hostKubeconfig, "server", "/bin/busybox", "sh", "-c", `printf '%s' "$1" > /www/durable`, "fixture", tenant.name+"-durable"); err != nil {
			t.Fatalf("write application volume: %v: %.400s", err, output)
		}
		awaitWorkerHTTP(t, tenant, hostKubeconfig, fmt.Sprintf("http://%s:8080", server.Status.PodIP), tenant.name)
		awaitWorkerHTTP(t, tenant, hostKubeconfig, "http://"+workerServiceIP(t, host, tenant, "web"), tenant.name)
		awaitWorkerHTTP(t, tenant, hostKubeconfig, "http://web.default.svc.cluster.local/durable", tenant.name+"-durable")
		t.Logf("%s real Pods, Pod-IP, Service, tenant DNS and mounted PVC traffic passed", tenant.name)
	}
	if volumes[a.name].physicalUID == volumes[b.name].physicalUID || volumes[a.name].volumeUID == volumes[b.name].volumeUID {
		t.Fatal("identically named claims shared a physical claim/volume")
	}
	a.process.stop()
	awaitWorkerHTTP(t, b, hostKubeconfig, "http://web.default.svc.cluster.local/durable", "tenant-b-durable")
	t.Log("tenant B serves its own PVC data through DNS while tenant A is offline")
	a.start(t)
	for _, name := range []string{"client", "server"} {
		replaceWorkerPod(t, host, a, name)
	}
	createWorkerService(t, a, "after-restart")
	awaitWorkerHTTP(t, a, hostKubeconfig, "http://after-restart.default.svc.cluster.local/durable", "tenant-a-durable")
	awaitWorkerHTTP(t, b, hostKubeconfig, "http://web.default.svc.cluster.local/durable", "tenant-b-durable")
	for _, tenant := range []*runtimeTenant{a, b} {
		volumes[tenant.name].verify(t, host, tenant)
	}
	t.Log("control-plane restart, real Pod replacement, fresh DNS record and isolated PVC durability passed")
}

func createWorkerService(t *testing.T, tenant *runtimeTenant, name string) {
	t.Helper()
	if _, err := tenant.client.CoreV1().Services("default").Create(t.Context(), &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "server"}, Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080)}}}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func workerServiceIP(t *testing.T, host *kubernetes.Clientset, tenant *runtimeTenant, name string) string {
	t.Helper()
	ref := awaitPhysicalObject(t, tenant, "Service", "default", name)
	var address string
	if err := wait.PollUntilContextTimeout(t.Context(), time.Second, integrationTimeout(time.Minute), true, func(ctx context.Context) (bool, error) {
		physical, err := host.CoreV1().Services(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		virtual, err := tenant.client.CoreV1().Services("default").Get(ctx, name, metav1.GetOptions{})
		if err != nil || virtual.Spec.ClusterIP == "" || virtual.Spec.ClusterIP != physical.Spec.ClusterIP {
			return false, nil
		}
		address = virtual.Spec.ClusterIP
		return true, nil
	}); err != nil {
		t.Fatal("Service addresses did not converge", err)
	}
	return address
}

func replaceWorkerPod(t *testing.T, host *kubernetes.Clientset, tenant *runtimeTenant, name string) {
	t.Helper()
	t.Logf("replacing %s/%s", tenant.name, name)
	old := waitScheduledPod(t, tenant, name)
	ref := awaitPhysicalObject(t, tenant, "Pod", "default", name)
	oldPhysical, err := host.CoreV1().Pods(ref.Namespace).Get(t.Context(), ref.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	grace := int64(0)
	if err := tenant.client.CoreV1().Pods("default").Delete(t.Context(), name, metav1.DeleteOptions{GracePeriodSeconds: &grace}); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(t.Context(), time.Second, integrationTimeout(time.Minute), true, func(ctx context.Context) (bool, error) {
		_, err := tenant.client.CoreV1().Pods("default").Get(ctx, name, metav1.GetOptions{})
		return kerrors.IsNotFound(err), nil
	}); err != nil {
		t.Fatal(err)
	}
	replacement := old.DeepCopy()
	replacement.ObjectMeta = metav1.ObjectMeta{Name: name, Labels: old.Labels}
	replacement.Status = corev1.PodStatus{}
	replacement.Spec.NodeName = ""
	if _, err := tenant.client.CoreV1().Pods("default").Create(t.Context(), replacement, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	fresh := waitScheduledPod(t, tenant, name)
	ref = awaitPhysicalObject(t, tenant, "Pod", "default", name)
	physical, err := host.CoreV1().Pods(ref.Namespace).Get(t.Context(), ref.Name, metav1.GetOptions{})
	if err != nil || fresh.UID == old.UID || physical.UID == oldPhysical.UID {
		t.Fatal("replacement did not create new virtual and physical Pod identities", err)
	}
}

func waitScheduledPod(t *testing.T, tenant *runtimeTenant, name string) *corev1.Pod {
	t.Helper()
	var pod *corev1.Pod
	if err := wait.PollUntilContextTimeout(t.Context(), time.Second, integrationTimeout(90*time.Second), true, func(ctx context.Context) (bool, error) {
		if err := tenant.process.check(); err != nil {
			return false, err
		}
		var err error
		pod, err = tenant.client.CoreV1().Pods("default").Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatalf("%s/%s did not become Ready: %v", tenant.name, name, err)
	}
	return pod
}
