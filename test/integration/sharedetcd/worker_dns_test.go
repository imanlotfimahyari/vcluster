//go:build integration && linux

package sharedetcd_test

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"strings"
	"testing"
	"time"
)

func TestWorkerDNSIdentity(t *testing.T) {
	lab := newEtcdLab(t)
	host, hostConfig, _ := startHostAPI(t, lab)
	a := prepareTenant(t, lab, host, hostConfig, "tenant-a")
	b := prepareTenant(t, lab, host, hostConfig, "tenant-b")
	a.start(t)
	b.start(t)
	_ = workerDNSIdentity(t, b) // Equal RBAC in B must not accept A's token.
	cfg, err := clientcmd.RESTConfigFromKubeConfig(workerDNSIdentity(t, a))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Timeout = 5 * time.Second
	reader, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, 15*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := reader.CoreV1().Services("").List(ctx, metav1.ListOptions{})
		return err == nil, nil
	}); err != nil {
		t.Fatal("DNS identity cannot read own services", err)
	}
	if _, err := reader.DiscoveryV1().EndpointSlices("").List(t.Context(), metav1.ListOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.CoreV1().Secrets("default").List(t.Context(), metav1.ListOptions{}); !kerrors.IsForbidden(err) {
		t.Fatal("DNS identity obtained Secret access", err)
	}
	if _, err := reader.CoreV1().ConfigMaps("default").Create(t.Context(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "dns-must-not-write"}}, metav1.CreateOptions{}); !kerrors.IsForbidden(err) {
		t.Fatal("DNS identity obtained write access", err)
	}
	cfg.Host = fmt.Sprintf("https://127.0.0.1:%d", b.config.ControlPlane.Proxy.Port)
	cfg.CAData, err = os.ReadFile(b.config.Experimental.VirtualClusterKubeConfig.ServerCACert)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := foreign.CoreV1().Services("").List(t.Context(), metav1.ListOptions{}); !kerrors.IsUnauthorized(err) && !(kerrors.IsForbidden(err) && strings.Contains(err.Error(), `User "system:anonymous"`)) {
		t.Fatal("B proxy did not reject A DNS token", err)
	}
	// Proxy fallback may be anonymous (403); backing API must reject the token.
	raw := b.apiClient.CoreV1().RESTClient().Get().URL()
	cfg.Host = raw.Scheme + "://" + raw.Host
	foreign, err = kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := foreign.CoreV1().Services("").List(t.Context(), metav1.ListOptions{}); !kerrors.IsUnauthorized(err) {
		t.Fatal("B backing API accepted A DNS token", err)
	}
}
