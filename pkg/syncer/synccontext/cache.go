package synccontext

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/loft-sh/vcluster/pkg/constants"
	"github.com/loft-sh/vcluster/pkg/scheme"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

func NewBidirectionalObjectCache(obj client.Object, mapper Mapper) *BidirectionalObjectCache {
	return &BidirectionalObjectCache{
		vCache: newObjectCache(),
		pCache: newObjectCache(),

		obj: obj,

		mapper: mapper,
	}
}

type BidirectionalObjectCache struct {
	vCache *ObjectCache
	pCache *ObjectCache

	mapper Mapper

	obj client.Object
}

func (o *BidirectionalObjectCache) Virtual() *ObjectCache {
	return o.vCache
}

func (o *BidirectionalObjectCache) Host() *ObjectCache {
	return o.pCache
}

func (o *BidirectionalObjectCache) Start(ctx *RegisterContext) error {
	gvk, err := apiutil.GVKForObject(o.obj, scheme.Scheme)
	if err != nil {
		return fmt.Errorf("gvk for object: %w", err)
	}

	if ctx.Config.ControlPlane.BackingStore.Etcd.External.StorePrefix != "" {
		if err := o.initializeBaseline(ctx); err != nil {
			return err
		}
	}

	go func() {
		wait.Until(func() {
			syncContext := ctx.ToSyncContext("bidirectional-object-cache")
			// clear up host cache
			o.pCache.cache.Range(func(key, _ any) bool {
				// check physical object
				pName := key.(types.NamespacedName)
				if objectExists(ctx, ctx.HostManager.GetClient(), pName, o.obj.DeepCopyObject().(client.Object)) {
					return true
				}

				// check virtual object
				vName := o.mapper.HostToVirtual(syncContext, pName, nil)
				if vName.Name == "" {
					o.pCache.cache.Delete(key)
					klog.FromContext(syncContext).V(1).Info("Delete from host cache", "gvk", gvk.String(), "key", pName.String())
					return true
				} else if objectExists(ctx, ctx.VirtualManager.GetClient(), vName, o.obj.DeepCopyObject().(client.Object)) {
					return true
				}

				// both host & virtual was not found, so we delete the object
				o.pCache.cache.Delete(key)
				o.vCache.cache.Delete(vName)
				klog.FromContext(syncContext).V(1).Info("Delete from virtual & host cache", "gvk", gvk.String(), "key", pName.String())
				return true
			})

			// clear up virtual cache
			o.vCache.cache.Range(func(key, _ any) bool {
				// check virtual object
				vName := key.(types.NamespacedName)
				if objectExists(ctx, ctx.VirtualManager.GetClient(), vName, o.obj.DeepCopyObject().(client.Object)) {
					return true
				}

				// check host object
				pName := o.mapper.VirtualToHost(syncContext, vName, nil)
				if pName.Name == "" {
					o.vCache.cache.Delete(key)
					klog.FromContext(syncContext).V(1).Info("Delete from virtual cache", "gvk", gvk.String(), "key", vName.String())
					return true
				} else if objectExists(ctx, ctx.HostManager.GetClient(), pName, o.obj.DeepCopyObject().(client.Object)) {
					return true
				}

				// both host & virtual was not found, so we delete the object in both caches
				o.vCache.cache.Delete(key)
				o.pCache.cache.Delete(vName)
				klog.FromContext(syncContext).V(1).Info("Delete from virtual & host cache", "gvk", gvk.String(), "key", vName.String())
				return true
			})
		}, time.Minute, ctx.Done())
	}()

	return nil
}

func objectExists(ctx context.Context, kubeClient client.Client, key types.NamespacedName, obj client.Object) bool {
	err := kubeClient.Get(ctx, key, obj)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			klog.FromContext(ctx).Error(err, "error getting object in object cache garbage collection")
			return true
		}

		return false
	}

	return true
}

func newObjectCache() *ObjectCache {
	return &ObjectCache{
		cache: sync.Map{},
	}
}

type ObjectCache struct {
	cache sync.Map
}

func (o *ObjectCache) Delete(obj client.Object) {
	o.cache.Delete(types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	})
}

func (o *ObjectCache) Put(obj client.Object) {
	o.cache.Store(types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}, obj)
}

func (o *ObjectCache) Get(key types.NamespacedName) (client.Object, bool) {
	val, ok := o.cache.Load(key)
	if ok {
		return val.(client.Object), ok
	}

	var d client.Object
	return d, false
}

// initializeBaseline attaches to the same real informers used by reconciliation.
// Seed only once: advancing baselines on later informer events would mark an
// unsynchronized user update as already delivered to the other cluster.
func (o *BidirectionalObjectCache) initializeBaseline(ctx *RegisterContext) error {
	warmCtx, cancel := context.WithTimeout(ctx, constants.DefaultCacheSyncTimeout)
	defer cancel()
	for _, side := range []struct {
		source ctrlcache.Cache
		target *ObjectCache
	}{
		{ctx.VirtualManager.GetCache(), o.vCache}, {ctx.HostManager.GetCache(), o.pCache},
	} {
		informer, err := side.source.GetInformer(warmCtx, o.obj)
		if err != nil {
			return fmt.Errorf("initialize bidirectional cache informer: %w", err)
		}
		registration, err := informer.AddEventHandler(side.target.baselineHandler())
		if err != nil {
			return fmt.Errorf("initialize bidirectional cache handler: %w", err)
		}
		if !toolscache.WaitForCacheSync(warmCtx.Done(), registration.HasSynced) {
			return fmt.Errorf("bidirectional cache initial events did not synchronize")
		}
	}
	return nil
}

func (o *ObjectCache) seed(obj interface{}) {
	value, ok := obj.(client.Object)
	if !ok || value == nil {
		return
	}
	o.cache.LoadOrStore(client.ObjectKeyFromObject(value), value.DeepCopyObject().(client.Object))
}

func (o *ObjectCache) baselineHandler() toolscache.ResourceEventHandler {
	return toolscache.ResourceEventHandlerFuncs{
		AddFunc:    o.seed,
		UpdateFunc: func(oldObj, _ interface{}) { o.seed(oldObj) },
	}
}
