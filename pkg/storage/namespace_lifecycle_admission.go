package storage

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/util"
	"sync"
)

func (w *WALEngine) admitNamespaceLifecycle(ctx context.Context) (func(), error) {
	return util.LockContext(ctx, w.mutationMu.RLock, w.mutationMu.RUnlock)
}

// AdmitNamespaceLifecycle waits for native snapshot admission before a catalog
// mutation. Callers release it before ordinary WAL methods acquire their own
// mutation lease, avoiding recursive RWMutex acquisition behind a snapshot.
func AdmitNamespaceLifecycle(ctx context.Context, engine Engine, namespace string) (func(), error) {
	release, err := admitNamespaceCreation(ctx, engine, namespace)
	if err != nil {
		return nil, err
	}
	var once sync.Once
	return func() { once.Do(release) }, nil
}

func admitNamespaceCreation(ctx context.Context, engine Engine, namespace string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if w, ok := engine.(*WALEngine); ok {
		releaseWAL, err := w.admitNamespaceLifecycle(ctx)
		if err != nil {
			return nil, err
		}
		release, err := admitNamespaceCreation(ctx, w.engine, namespace)
		if err != nil {
			releaseWAL()
			return nil, err
		}
		return func() { release(); releaseWAL() }, nil
	}
	if owner, ok := engine.(interface{ namespaceLifetimeOwner() *BadgerEngine }); ok {
		b := owner.namespaceLifetimeOwner()
		release, err := b.beginNamespaceMutation(ctx, func() []string {
			if namespace == "" {
				return nil
			}
			return []string{namespace}
		}(), nil, true, namespacePublicationFresh)
		if err != nil {
			return nil, err
		}
		if err := b.checkNamespaceOperation(namespace); err != nil {
			release()
			return nil, err
		}
		return release, nil
	}
	if w, ok := engine.(EngineUnwrapper); ok {
		return admitNamespaceCreation(ctx, w.GetInnerEngine(), namespace)
	}
	return func() {}, nil
}
