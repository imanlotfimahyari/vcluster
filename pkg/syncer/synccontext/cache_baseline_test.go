package synccontext

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestInitialEventsPreserveUnsynchronizedBaseline(t *testing.T) {
	t.Parallel()
	baseline := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "same-name", Namespace: "default", ResourceVersion: "10"}, Data: map[string]string{"value": "before"}}
	updated := baseline.DeepCopy()
	updated.ResourceVersion = "11"
	updated.Data["value"] = "after"
	cache := newObjectCache()
	handler := cache.baselineHandler()
	handler.OnAdd(baseline, true)
	handler.OnUpdate(baseline, updated)
	baseline.Data["value"] = "mutated-informer-object"
	stored, ok := cache.Get(client.ObjectKeyFromObject(updated))
	if !ok || stored.(*corev1.ConfigMap).Data["value"] != "before" || stored.GetResourceVersion() != "10" {
		t.Fatalf("initial baseline was overwritten or aliased: %+v", stored)
	}
	// Once reconciliation advances the baseline, later informer callbacks must
	// not roll it back to an older event snapshot.
	cache.Put(updated.DeepCopy())
	handler.OnUpdate(baseline, updated)
	stored, _ = cache.Get(client.ObjectKeyFromObject(updated))
	if stored.(*corev1.ConfigMap).Data["value"] != "after" {
		t.Fatal("baseline regressed after successful reconciliation")
	}
}

func TestUpdateSeedsOldObjectWhenInitialEventIsUnavailable(t *testing.T) {
	t.Parallel()
	old := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}, Data: map[string]string{"value": "old"}}
	current := old.DeepCopy()
	current.Data["value"] = "new"
	cache := newObjectCache()
	cache.baselineHandler().OnUpdate(old, current)
	stored, ok := cache.Get(client.ObjectKeyFromObject(current))
	if !ok || stored.(*corev1.ConfigMap).Data["value"] != "old" {
		t.Fatal("unsynchronized update was treated as the baseline")
	}
}
