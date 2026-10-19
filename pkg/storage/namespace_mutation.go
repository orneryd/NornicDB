package storage

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/util"
)

type namespacePublicationMode uint8

const (
	namespacePublicationFresh namespacePublicationMode = iota
	namespacePublicationReplay
	namespacePublicationLifecycle
)

// beginNamespaceMutation is the physical admission owner. Capture the logical
// lifetime before waiting, then validate it while publication excludes DROP.
// Empty scope means whole-store maintenance; replay alone carries a previously
// acknowledged publication and therefore may cross a prepared drain fence.
func (b *BadgerEngine) beginNamespaceMutation(ctx context.Context, namespaces []string, revision *uint64, exclusive bool, mode namespacePublicationMode) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	begin := b.namespaceLifetimeRevision()
	if revision != nil {
		begin = *revision
	}
	lock, unlock := b.writeBarrier.RLock, b.writeBarrier.RUnlock
	if exclusive {
		lock, unlock = b.writeBarrier.Lock, b.writeBarrier.Unlock
	}
	release, err := util.LockContext(ctx, lock, unlock)
	if err != nil {
		return nil, err
	}
	b.mu.RLock()
	closed := b.closed || (b.closing && mode != namespacePublicationReplay)
	b.mu.RUnlock()
	if closed {
		release()
		return nil, ErrStorageClosed
	}
	if mode == namespacePublicationFresh {
		if len(namespaces) == 0 {
			if prefixes := b.deletingPrefixes.Load(); prefixes != nil && len(*prefixes) != 0 {
				release()
				return nil, fmt.Errorf("%w: prefix deletion prevents whole-store mutation", ErrConflict)
			}
			if state := b.namespaceLifetimes.Load(); state != nil && len(state.active) != 0 {
				release()
				return nil, fmt.Errorf("%w: lifecycle operation prevents whole-store mutation", ErrConflict)
			}
		}
		for _, namespace := range namespaces {
			if err := b.checkNamespaceAdmission(namespace, begin); err != nil {
				release()
				return nil, err
			}
		}
	}
	return release, nil
}

// All entry protocols use this predicate under their publication lease or
// namespace journal gate. The lease, not a point check alone, makes it atomic.
func (b *BadgerEngine) checkNamespaceAdmission(namespace string, revision uint64) error {
	if state := b.namespaceLifetimes.Load(); state != nil {
		if state.active[namespace] != nil || state.retired[namespace] > revision {
			return fmt.Errorf("%w: namespace %q was dropped or is retiring after operation began", ErrConflict, namespace)
		}
	}
	return b.checkNamespacePublication(namespace)
}

func namespaceOfNode(node *Node) string {
	if node == nil {
		return ""
	}
	return namespaceForNodeID(node.ID)
}
func namespaceOfEdge(edge *Edge) string {
	if edge == nil {
		return ""
	}
	return namespaceForEdgeID(edge.ID)
}

func (b *BadgerEngine) withNamespaceMutation(ctx context.Context, namespace string, write func() error) error {
	release, err := b.beginNamespaceMutation(ctx, []string{namespace}, nil, false, namespacePublicationFresh)
	if err != nil {
		return err
	}
	defer release()
	return write()
}

func (b *BadgerEngine) withNamespaceMaintenance(ctx context.Context, write func() error) error {
	release, err := b.beginNamespaceMutation(ctx, nil, nil, false, namespacePublicationFresh)
	if err != nil {
		return err
	}
	defer release()
	return write()
}
