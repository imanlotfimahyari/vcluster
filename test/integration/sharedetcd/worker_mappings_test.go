//go:build integration && linux

package sharedetcd_test

import (
	"github.com/loft-sh/vcluster/pkg/config"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"testing"
)

// Real APIs without workers: require actual translated objects and an existing
// workload ServiceAccount, but do not synthesize successful Pod/PVC status.
func TestWorkerServiceAndClaimTranslation(t *testing.T) {
	lab := newEtcdLab(t)
	host, hostConfig, _ := startHostAPI(t, lab)
	tenant := prepareTenant(t, lab, host, hostConfig, "tenant-a", func(c *config.VirtualClusterConfig) {
		c.Sync.ToHost.Services.Enabled = true
		c.Sync.ToHost.PersistentVolumeClaims.Enabled = true
		c.Sync.ToHost.Pods.Enabled = true
		c.Sync.ToHost.Secrets.Enabled = true
	})
	tenant.start(t)
	if _, err := tenant.client.CoreV1().Services("kube-system").Create(t.Context(), &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "kube-dns"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"fixture-dns": tenant.name}, Ports: []corev1.ServicePort{{Name: "dns", Protocol: corev1.ProtocolUDP, Port: 53, TargetPort: intstr.FromInt32(1053)}}}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	ref := awaitPhysicalObject(t, tenant, "Service", "kube-system", "kube-dns")
	if _, err := host.CoreV1().Services(ref.Namespace).Get(t.Context(), ref.Name, metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := tenant.client.CoreV1().PersistentVolumeClaims("default").Create(t.Context(), &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data"}, Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: ptr.To(""), AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("16Mi")}}}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	ref = awaitPhysicalObject(t, tenant, "PersistentVolumeClaim", "default", "data")
	if _, err := host.CoreV1().PersistentVolumeClaims(ref.Namespace).Get(t.Context(), ref.Name, metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := tenant.client.CoreV1().Pods("default").Create(t.Context(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "probe"}, Spec: corev1.PodSpec{AutomountServiceAccountToken: ptr.To(false), Containers: []corev1.Container{{Name: "probe", Image: "test.invalid/fixture"}}}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	ref = awaitPhysicalObject(t, tenant, "Pod", "default", "probe")
	pod, err := host.CoreV1().Pods(ref.Namespace).Get(t.Context(), ref.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.CoreV1().ServiceAccounts(pod.Namespace).Get(t.Context(), pod.Spec.ServiceAccountName, metav1.GetOptions{}); err != nil {
		t.Fatal("translated Pod references absent workload ServiceAccount", err)
	}
}
