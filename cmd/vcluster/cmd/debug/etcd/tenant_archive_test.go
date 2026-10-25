package etcd

import (
	"testing"

	setupconfig "github.com/loft-sh/vcluster/pkg/setup/config"
	"github.com/loft-sh/vcluster/pkg/tenantbackup"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func stoppedFixture() (*fake.Clientset, tenantbackup.Ownership) {
	zero := int32(0)
	owner := tenantbackup.Ownership{Version: "1", Root: "/tenants/test/", DeploymentUID: "deployment-identity", Endpoint: "etcd.example.com:2379"}
	host := fake.NewSimpleClientset(
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "vc-config-test", Namespace: "host", UID: types.UID(owner.DeploymentUID)}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: setupconfig.StorageLayoutMarkerName("test"), Namespace: "host"}, Data: map[string]string{"version": "1", "rootPrefix": owner.Root, "deploymentUID": owner.DeploymentUID, "etcdEndpoint": owner.Endpoint}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "host", UID: "controller", Generation: 3}, Spec: appsv1.DeploymentSpec{Replicas: &zero}, Status: appsv1.DeploymentStatus{ObservedGeneration: 3}},
	)
	return host, owner
}

func TestStoppedTenantRefusesIncompleteQuiescence(t *testing.T) {
	for _, scenario := range []string{"stopped deployment", "stopped statefulset", "replicas requested", "unobserved generation", "status replicas", "owned replicaset active", "owned pod", "terminating orphan pod", "ambiguous workload", "different identity", "different marker", "unrelated pod"} {
		t.Run(scenario, func(t *testing.T) {
			host, owner := stoppedFixture()
			ctx := t.Context()
			deployment, err := host.AppsV1().Deployments("host").Get(ctx, "test", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			zero, one := int32(0), int32(1)
			wantErr := true
			switch scenario {
			case "stopped deployment":
				wantErr = false
			case "stopped statefulset":
				wantErr = false
				if err := host.AppsV1().Deployments("host").Delete(ctx, "test", metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
				_, err = host.AppsV1().StatefulSets("host").Create(ctx, &appsv1.StatefulSet{ObjectMeta: deployment.ObjectMeta, Spec: appsv1.StatefulSetSpec{Replicas: &zero}, Status: appsv1.StatefulSetStatus{ObservedGeneration: 3}}, metav1.CreateOptions{})
			case "replicas requested":
				deployment.Spec.Replicas = &one
			case "unobserved generation":
				deployment.Status.ObservedGeneration = 2
			case "status replicas":
				deployment.Status.Replicas = 1
			case "owned replicaset active":
				_, err = host.AppsV1().ReplicaSets("host").Create(ctx, &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "old-revision", Namespace: "host", UID: "replicaset", OwnerReferences: []metav1.OwnerReference{{UID: deployment.UID}}}, Spec: appsv1.ReplicaSetSpec{Replicas: &one}}, metav1.CreateOptions{})
			case "owned pod":
				_, err = host.CoreV1().Pods("host").Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test-old", Namespace: "host", OwnerReferences: []metav1.OwnerReference{{UID: deployment.UID}}}}, metav1.CreateOptions{})
			case "terminating orphan pod":
				now := metav1.Now()
				_, err = host.CoreV1().Pods("host").Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test-orphan", Namespace: "host", Labels: map[string]string{"app": "vcluster", "release": "test"}, DeletionTimestamp: &now, Finalizers: []string{"test"}}}, metav1.CreateOptions{})
			case "ambiguous workload":
				_, err = host.AppsV1().StatefulSets("host").Create(ctx, &appsv1.StatefulSet{ObjectMeta: deployment.ObjectMeta, Spec: appsv1.StatefulSetSpec{Replicas: &zero}, Status: appsv1.StatefulSetStatus{ObservedGeneration: 3}}, metav1.CreateOptions{})
			case "different identity":
				owner.DeploymentUID = "replacement"
			case "different marker":
				owner.Endpoint = "another.example.com:2379"
			case "unrelated pod":
				wantErr = false
				_, err = host.CoreV1().Pods("host").Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: "host"}}, metav1.CreateOptions{})
			}
			if err != nil {
				t.Fatal(err)
			}
			if scenario != "stopped statefulset" {
				if _, err := host.AppsV1().Deployments("host").Update(ctx, deployment, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			err = stoppedTenant(ctx, host, "host", "test", owner)
			if (err != nil) != wantErr {
				t.Fatalf("error=%v, want refusal=%v", err, wantErr)
			}
		})
	}
}
