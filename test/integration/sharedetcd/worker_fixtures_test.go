//go:build integration && linux

package sharedetcd_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/loft-sh/vcluster/pkg/util/translate"
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/utils/ptr"
)

// Check the guest before spending time on tenant APIs. The host's admission,
// scheduler, CNI, container runtime and DNS must work with an ordinary Pod.
func requireWorkerNetwork(t *testing.T, host *kubernetes.Clientset, kubeconfig string) {
	t.Helper()
	ctx := t.Context()
	const ns = "worker-network-probe"
	if _, err := host.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = host.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{})
	})
	if _, err := host.CoreV1().ServiceAccounts(ns).Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "probe"}, AutomountServiceAccountToken: ptr.To(false)}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.CoreV1().Pods(ns).Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "probe"}, Spec: corev1.PodSpec{ServiceAccountName: "probe", AutomountServiceAccountToken: ptr.To(false), DNSPolicy: corev1.DNSClusterFirst, Containers: []corev1.Container{{Name: "probe", Image: "docker.io/library/vcluster-test:local", ImagePullPolicy: corev1.PullNever, Command: []string{"/bin/busybox", "sleep", "3600"}}}}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, integrationTimeout(45*time.Second), true, func(ctx context.Context) (bool, error) {
		pod, err := host.CoreV1().Pods(ns).Get(ctx, "probe", metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, c := range pod.Status.ContainerStatuses {
			if c.State.Waiting != nil && (c.State.Waiting.Reason == "ErrImageNeverPull" || c.State.Waiting.Reason == "CreateContainerError") {
				return false, fmt.Errorf("offline image preparation failed: %s: %s", c.State.Waiting.Reason, c.State.Waiting.Message)
			}
		}
		for _, c := range pod.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				return pod.Status.PodIP != "" && pod.Spec.NodeName != "", nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal("guest regular Pod did not become Ready", err)
	}
	svc, err := host.CoreV1().Services("default").Get(ctx, "kubernetes", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, integrationTimeout(45*time.Second), true, func(ctx context.Context) (bool, error) {
		call, cancel := context.WithTimeout(ctx, integrationTimeout(10*time.Second))
		defer cancel()
		out, err := exec.CommandContext(call, asset(t, "k3s"), "kubectl", "--kubeconfig="+kubeconfig, "exec", "-n", ns, "probe", "--", "/bin/busybox", "nslookup", "kubernetes.default.svc.cluster.local").CombinedOutput()
		return err == nil && slices.Contains(strings.Fields(string(out)), svc.Spec.ClusterIP), nil
	}); err != nil {
		t.Fatal("guest Pod-to-host DNS preflight failed", err)
	}
	t.Log("guest regular Pod scheduling, CNI and host DNS preflight passed")
}

// Use the already-imported K3s CoreDNS image. Host networking lets this resolver
// reach the loopback process API. Its token is tenant-local and read-only.
// This does not pretend to validate the chart-managed CoreDNS Deployment.
func prepareWorkerDNS(t *testing.T, host *kubernetes.Clientset, tenant *runtimeTenant, port int32) {
	t.Helper()
	ctx := t.Context()
	deployment, err := host.AppsV1().Deployments("kube-system").Get(ctx, "coredns", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	const name = "worker-dns"
	if _, err := host.CoreV1().Secrets(tenant.name).Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name}, Data: map[string][]byte{"kubeconfig": workerDNSIdentity(t, tenant)}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	corefile := fmt.Sprintf(".:%d {\n errors\n kubernetes cluster.local {\n kubeconfig /etc/tenant/kubeconfig\n }\n cache 5\n}\n", port)
	if _, err := host.CoreV1().ConfigMaps(tenant.name).Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name}, Data: map[string]string{"Corefile": corefile}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := tenant.client.CoreV1().Services("kube-system").Create(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "kube-dns"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"fixture-dns": tenant.name}, Ports: []corev1.ServicePort{{Name: "dns", Protocol: corev1.ProtocolUDP, Port: 53, TargetPort: intstr.FromInt32(port)}, {Name: "dns-tcp", Protocol: corev1.ProtocolTCP, Port: 53, TargetPort: intstr.FromInt32(port)}}}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	ref := awaitPhysicalObject(t, tenant, "Service", "kube-system", "kube-dns")
	var svc *corev1.Service
	if err := wait.PollUntilContextTimeout(ctx, time.Second, integrationTimeout(time.Minute), true, func(ctx context.Context) (bool, error) {
		svc, err = host.CoreV1().Services(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		return err == nil && svc.Spec.ClusterIP != "" && len(svc.Spec.Selector) > 0, nil
	}); err != nil {
		t.Fatal("DNS Service did not translate", err)
	}
	if _, err := host.CoreV1().Pods(ref.Namespace).Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: svc.Spec.Selector}, Spec: corev1.PodSpec{
		HostNetwork: true, DNSPolicy: corev1.DNSDefault, AutomountServiceAccountToken: ptr.To(false),
		Containers: []corev1.Container{{Name: "dns", Image: deployment.Spec.Template.Spec.Containers[0].Image, ImagePullPolicy: corev1.PullNever, Args: []string{"-conf", "/etc/dns/Corefile"}, ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(port)}}, PeriodSeconds: 2}, VolumeMounts: []corev1.VolumeMount{{Name: "config", MountPath: "/etc/dns", ReadOnly: true}, {Name: "identity", MountPath: "/etc/tenant", ReadOnly: true}}}},
		Volumes:    []corev1.Volume{{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: name}}}}, {Name: "identity", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: name}}}},
	}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, integrationTimeout(90*time.Second), true, func(ctx context.Context) (bool, error) {
		pod, err := host.CoreV1().Pods(ref.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, c := range pod.Status.ContainerStatuses {
			if c.State.Waiting != nil && (c.State.Waiting.Reason == "ErrImageNeverPull" || c.State.Waiting.Reason == "InvalidImageName") {
				return false, fmt.Errorf("offline DNS image unavailable: %s", c.State.Waiting.Reason)
			}
			if c.RestartCount >= 2 && c.LastTerminationState.Terminated != nil && c.LastTerminationState.Terminated.ExitCode != 0 {
				return false, fmt.Errorf("DNS repeatedly exited: %s", c.LastTerminationState.Terminated.Reason)
			}
		}
		for _, c := range pod.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal("real tenant DNS resolver did not become Ready", err)
	}
	t.Logf("%s real tenant DNS resolver is Ready", tenant.name)
}

func workerDNSIdentity(t *testing.T, tenant *runtimeTenant) []byte {
	t.Helper()
	ctx := t.Context()
	const name = "worker-dns"
	if _, err := tenant.client.CoreV1().ServiceAccounts("kube-system").Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	rules := []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"services", "endpoints", "pods", "namespaces"}, Verbs: []string{"get", "list", "watch"}}, {APIGroups: []string{"discovery.k8s.io"}, Resources: []string{"endpointslices"}, Verbs: []string{"get", "list", "watch"}}}
	if _, err := tenant.client.RbacV1().ClusterRoles().Create(ctx, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: rules}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := tenant.client.RbacV1().ClusterRoleBindings().Create(ctx, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name}, RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: name, Namespace: "kube-system"}}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	token, err := tenant.client.CoreV1().ServiceAccounts("kube-system").CreateToken(ctx, name, &authv1.TokenRequest{Spec: authv1.TokenRequestSpec{ExpirationSeconds: ptr.To(int64(3600))}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ca, err := os.ReadFile(tenant.config.Experimental.VirtualClusterKubeConfig.ServerCACert)
	if err != nil {
		t.Fatal(err)
	}
	data, err := clientcmd.Write(clientcmdapi.Config{Clusters: map[string]*clientcmdapi.Cluster{"tenant": {Server: fmt.Sprintf("https://127.0.0.1:%d", tenant.config.ControlPlane.Proxy.Port), CertificateAuthorityData: ca}}, AuthInfos: map[string]*clientcmdapi.AuthInfo{"dns": {Token: token.Status.Token}}, Contexts: map[string]*clientcmdapi.Context{"dns": {Cluster: "tenant", AuthInfo: "dns"}}, CurrentContext: "dns"})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func awaitPhysicalObject(t *testing.T, tenant *runtimeTenant, kind, namespace, name string) types.NamespacedName {
	t.Helper()
	return awaitPhysicalObjectContext(t.Context(), t, tenant, kind, namespace, name)
}

// Services can be proxy-preallocated before a UID or persisted mapping exists.
// Pods/PVCs must also match the current virtual UID, rejecting stale objects.
func awaitPhysicalObjectContext(ctx context.Context, t *testing.T, tenant *runtimeTenant, kind, namespace, name string) types.NamespacedName {
	t.Helper()
	var result types.NamespacedName
	if err := wait.PollUntilContextTimeout(ctx, time.Second, integrationTimeout(time.Minute), true, func(ctx context.Context) (bool, error) {
		var objects []metav1.Object
		var uid types.UID
		switch kind {
		case "Service":
			v, err := tenant.client.CoreV1().Services(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return false, nil
			}
			uid = v.UID
			list, err := tenant.host.CoreV1().Services(tenant.name).List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, err
			}
			for i := range list.Items {
				objects = append(objects, &list.Items[i])
			}
		case "PersistentVolumeClaim":
			v, err := tenant.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return false, nil
			}
			uid = v.UID
			list, err := tenant.host.CoreV1().PersistentVolumeClaims(tenant.name).List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, err
			}
			for i := range list.Items {
				objects = append(objects, &list.Items[i])
			}
		case "Pod":
			v, err := tenant.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return false, nil
			}
			uid = v.UID
			list, err := tenant.host.CoreV1().Pods(tenant.name).List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, err
			}
			for i := range list.Items {
				objects = append(objects, &list.Items[i])
			}
		default:
			return false, fmt.Errorf("unsupported worker resource %s", kind)
		}
		matches := 0
		for _, o := range objects {
			a := o.GetAnnotations()
			if a[translate.NameAnnotation] == name && a[translate.NamespaceAnnotation] == namespace && (kind == "Service" || a[translate.UIDAnnotation] == string(uid)) && o.GetLabels()[translate.MarkerLabel] == tenant.name {
				result = types.NamespacedName{Name: o.GetName(), Namespace: o.GetNamespace()}
				matches++
			}
		}
		if matches > 1 {
			return false, fmt.Errorf("ambiguous physical %s identity", kind)
		}
		return matches == 1, nil
	}); err != nil {
		t.Fatalf("translated %s for %s/%s did not appear: %v", kind, namespace, name, err)
	}
	return result
}

type workerVolume struct {
	virtualUID, physicalUID, volumeUID types.UID
	claim                              types.NamespacedName
	volume                             string
}

// Real host PV binding and mounts, without claiming CSI/dynamic provisioning.
func prepareWorkerVolume(t *testing.T, host *kubernetes.Clientset, tenant *runtimeTenant) workerVolume {
	t.Helper()
	ctx := t.Context()
	claim, err := tenant.client.CoreV1().PersistentVolumeClaims("default").Create(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data"}, Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: ptr.To(""), AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("16Mi")}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ref := awaitPhysicalObject(t, tenant, "PersistentVolumeClaim", "default", claim.Name)
	physical, err := host.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	volume, err := host.CoreV1().PersistentVolumes().Create(ctx, &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "worker-data-" + tenant.name}, Spec: corev1.PersistentVolumeSpec{Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("16Mi")}, AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain, ClaimRef: &corev1.ObjectReference{Kind: "PersistentVolumeClaim", APIVersion: "v1", Name: ref.Name, Namespace: ref.Namespace, UID: physical.UID}, PersistentVolumeSource: corev1.PersistentVolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: t.TempDir(), Type: ptr.To(corev1.HostPathDirectory)}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, time.Second, integrationTimeout(time.Minute), true, func(ctx context.Context) (bool, error) {
		c, err := tenant.client.CoreV1().PersistentVolumeClaims("default").Get(ctx, claim.Name, metav1.GetOptions{})
		return err == nil && c.Status.Phase == corev1.ClaimBound && c.Spec.VolumeName != "", nil
	}); err != nil {
		t.Fatal("virtual claim did not report real binding", err)
	}
	t.Logf("%s virtual and physical PVC binding is ready", tenant.name)
	return workerVolume{virtualUID: claim.UID, physicalUID: physical.UID, volumeUID: volume.UID, claim: ref, volume: volume.Name}
}

func (v workerVolume) verify(t *testing.T, host *kubernetes.Clientset, tenant *runtimeTenant) {
	t.Helper()
	virtual, err := tenant.client.CoreV1().PersistentVolumeClaims("default").Get(t.Context(), "data", metav1.GetOptions{})
	if err != nil || virtual.UID != v.virtualUID || virtual.Status.Phase != corev1.ClaimBound {
		t.Fatal("virtual claim identity/binding changed", err)
	}
	physical, err := host.CoreV1().PersistentVolumeClaims(v.claim.Namespace).Get(t.Context(), v.claim.Name, metav1.GetOptions{})
	if err != nil || physical.UID != v.physicalUID || physical.Spec.VolumeName != v.volume || physical.Status.Phase != corev1.ClaimBound {
		t.Fatal("physical claim identity/binding changed", err)
	}
	volume, err := host.CoreV1().PersistentVolumes().Get(t.Context(), v.volume, metav1.GetOptions{})
	if err != nil || volume.UID != v.volumeUID || volume.Spec.ClaimRef == nil || volume.Spec.ClaimRef.UID != v.physicalUID {
		t.Fatal("persistent volume identity/ownership changed", err)
	}
}

func workerExec(ctx context.Context, t *testing.T, tenant *runtimeTenant, kubeconfig, pod string, command ...string) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, integrationTimeout(15*time.Second))
	defer cancel()
	ref := awaitPhysicalObjectContext(ctx, t, tenant, "Pod", "default", pod)
	args := []string{"kubectl", "--kubeconfig=" + kubeconfig, "exec", "-n", ref.Namespace, ref.Name, "--"}
	return exec.CommandContext(ctx, asset(t, "k3s"), append(args, command...)...).CombinedOutput()
}

func awaitWorkerHTTP(t *testing.T, tenant *runtimeTenant, kubeconfig, address, expected string) {
	t.Helper()
	var last []byte
	if err := wait.PollUntilContextTimeout(t.Context(), time.Second, integrationTimeout(45*time.Second), true, func(ctx context.Context) (bool, error) {
		out, err := workerExec(ctx, t, tenant, kubeconfig, "client", "/bin/busybox", "wget", "-T", "5", "-qO-", address)
		last = out
		return err == nil && strings.TrimSpace(string(out)) == expected, nil
	}); err != nil {
		t.Fatalf("%s workload traffic failed: %v; last response: %.400s", tenant.name, err, last)
	}
}
