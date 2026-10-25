//go:build integration && linux

package sharedetcd_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/loft-sh/vcluster/pkg/config"
	"github.com/loft-sh/vcluster/pkg/etcd"
	"github.com/loft-sh/vcluster/pkg/mappings/store"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
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
	t.Log("offline Kubernetes worker is Ready; starting authenticated shared etcd")
	lab := newEtcdLab(t)
	configure := func(c *config.VirtualClusterConfig) {
		c.Sync.ToHost.Pods.Enabled = true
		c.Sync.ToHost.Secrets.Enabled = true
		c.Sync.ToHost.Services.Enabled = true
	}
	a := prepareTenant(t, lab, host, hostKubeconfig, "tenant-a", configure)
	b := prepareTenant(t, lab, host, hostKubeconfig, "tenant-b", configure)
	a.start(t)
	b.start(t)
	t.Log("both vCluster proxies are ready; creating identically named real Pods")
	for _, tenant := range []*runtimeTenant{a, b} {
		for _, name := range []string{"server", "client"} {
			command := []string{"/bin/busybox", "sleep", "3600"}
			if name == "server" {
				command = []string{"/bin/busybox", "sh", "-c", "echo " + tenant.name + " > /www/index.html; exec /bin/busybox httpd -f -p 8080 -h /www"}
			}
			_, err := tenant.client.CoreV1().Pods("default").Create(t.Context(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"app": name}}, Spec: corev1.PodSpec{
				DNSPolicy: corev1.DNSNone, DNSConfig: &corev1.PodDNSConfig{Nameservers: []string{"10.96.0.10"}},
				Containers: []corev1.Container{{Name: "busybox", Image: "docker.io/library/vcluster-test:local", ImagePullPolicy: corev1.PullNever, Command: command}},
			}}, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
		}
		_, err := tenant.client.CoreV1().Services("default").Create(t.Context(), &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "web"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "server"}, Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080)}}}}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		server := waitScheduledPod(t, tenant, "server")
		_ = waitScheduledPod(t, tenant, "client")
		if server.Status.PodIP == "" || server.Spec.NodeName == "" {
			t.Fatal("virtual Pod lacks real scheduling/network status")
		}
		clientName := physicalPodName(t, tenant, "client")
		for _, useService := range []bool{false, true} {
			if err := wait.PollUntilContextTimeout(t.Context(), time.Second, integrationTimeout(30*time.Second), true, func(ctx context.Context) (bool, error) {
				address := fmt.Sprintf("http://%s:8080", server.Status.PodIP)
				if useService {
					// The syncer recreates virtual Services with their physical IP.
					// Fetch the current object instead of retaining the create reply.
					service, err := tenant.client.CoreV1().Services("default").Get(ctx, "web", metav1.GetOptions{})
					if err != nil {
						return false, nil
					}
					address = "http://" + service.Spec.ClusterIP
				}
				callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
				defer cancel()
				output, err := exec.CommandContext(callCtx, asset(t, "k3s"), "kubectl", "--kubeconfig="+hostKubeconfig, "exec", "-n", tenant.name, clientName, "--", "/bin/busybox", "wget", "-T", "5", "-qO-", address).CombinedOutput()
				return err == nil && strings.TrimSpace(string(output)) == tenant.name, nil
			}); err != nil {
				t.Fatalf("%s CNI/service traffic failed: %v", tenant.name, err)
			}
		}
		t.Logf("%s Pods scheduled; Pod-IP and Service traffic passed", tenant.name)
	}
	// Restart one control plane while its real workloads and the other tenant
	// remain running. Confirm that Pod status and a replacement still converge.
	a.process.stop()
	a.start(t)
	old := waitScheduledPod(t, a, "client")
	grace := int64(0)
	if err := a.client.CoreV1().Pods("default").Delete(t.Context(), "client", metav1.DeleteOptions{GracePeriodSeconds: &grace}); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(t.Context(), time.Second, integrationTimeout(time.Minute), true, func(ctx context.Context) (bool, error) {
		_, err := a.client.CoreV1().Pods("default").Get(ctx, "client", metav1.GetOptions{})
		return kerrors.IsNotFound(err), nil
	}); err != nil {
		t.Fatal(err)
	}
	replacement := old.DeepCopy()
	replacement.ObjectMeta = metav1.ObjectMeta{Name: "client"}
	replacement.Status = corev1.PodStatus{}
	replacement.Spec.NodeName = ""
	if _, err := a.client.CoreV1().Pods("default").Create(t.Context(), replacement, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	fresh := waitScheduledPod(t, a, "client")
	if fresh.UID == old.UID {
		t.Fatal("replacement retained the old UID")
	}
	_ = waitScheduledPod(t, b, "server")
	t.Log("control-plane restart and real Pod replacement passed; other tenant remained Ready")
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

func physicalPodName(t *testing.T, tenant *runtimeTenant, name string) string {
	t.Helper()
	wrapped, err := etcd.NewFromConfig(t.Context(), tenant.config)
	if err != nil {
		t.Fatal(err)
	}
	defer wrapped.Close()
	layout, err := tenant.config.ResolveEtcdStorageLayout()
	if err != nil {
		t.Fatal(err)
	}
	mappings, err := store.NewEtcdBackend(wrapped, layout.MappingsPrefix).List(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, mapping := range mappings {
		if mapping.VirtualName.Name == name && mapping.VirtualName.Namespace == "default" && strings.EqualFold(mapping.Kind, "Pod") {
			return mapping.HostName.Name
		}
	}
	t.Fatal("no physical Pod mapping")
	return ""
}
