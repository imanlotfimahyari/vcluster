package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	snapshotapi "github.com/loft-sh/api/v5/pkg/snapshot"
	"github.com/loft-sh/log"
	"github.com/loft-sh/vcluster/pkg/cli/find"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func TestRestoreRejectsDestinationTenantLayoutBeforePause(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/namespaces/test/secrets/test-config" {
			t.Errorf("restore performed an unexpected operation before rejecting the layout: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		secret := &corev1.Secret{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
			Data: map[string][]byte{"config.yaml": []byte(`controlPlane:
  distro:
    k8s:
      enabled: true
      apiServer:
        enabled: true
  backingStore:
    etcd:
      external:
        enabled: true
        storePrefix: /tenants/test
`)},
		}
		if err := json.NewEncoder(w).Encode(secret); err != nil {
			t.Errorf("write config response: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	kubeClient, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	vCluster := &find.VCluster{
		Name: "test", Namespace: "test",
		StatefulSet: &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{Name: "vcluster-config", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "test-config"}}}},
		}}}},
	}
	// Source archive claims a legacy layout. The destination must still be
	// inspected before scaling it down or launching a restore pod.
	options := &snapshotapi.Options{Release: &snapshotapi.HelmRelease{Values: []byte("{}")}}
	err = restoreVCluster(t.Context(), kubeClient, nil, vCluster, options, nil, false, log.Discard)
	if err == nil || !strings.Contains(err.Error(), "tenant-scoped") {
		t.Fatalf("expected early destination-layout rejection, got %v", err)
	}
}
