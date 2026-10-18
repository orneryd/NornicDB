package storage

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/util"
)

// checkNamespacePublication is the admission boundary for new MVCC commits,
// including native database views. Replay uses its previously acknowledged
// version and is allowed to retire the sealed generations.
func (b *BadgerEngine) checkNamespacePublication(namespace string) error {
	prefixes := b.deletingPrefixes.Load()
	if prefixes == nil {
		return nil
	}
	for _, prefix := range *prefixes {
		if prefixesOverlap(namespace+":", prefix) {
			return fmt.Errorf("%w: namespace %q is being deleted", ErrConflict, namespace)
		}
	}
	return nil
}

func (b *BadgerEngine) addDeletingPrefix(prefix string) (func(), error) {
	b.prefixDeleteMu.Lock()
	defer b.prefixDeleteMu.Unlock()
	var next []string
	if current := b.deletingPrefixes.Load(); current != nil {
		for _, other := range *current {
			if prefixesOverlap(prefix, other) {
				return nil, fmt.Errorf("%w: prefix deletion already in progress", ErrConflict)
			}
		}
		next = append(next, (*current)...)
	}
	next = append(next, prefix)
	b.deletingPrefixes.Store(&next)
	return func() {
		b.prefixDeleteMu.Lock()
		defer b.prefixDeleteMu.Unlock()
		current := b.deletingPrefixes.Load()
		remaining := make([]string, 0, len(*current)-1)
		for _, other := range *current {
			if other != prefix {
				remaining = append(remaining, other)
			}
		}
		if len(remaining) == 0 {
			b.deletingPrefixes.Store(nil)
		} else {
			b.deletingPrefixes.Store(&remaining)
		}
	}, nil
}

func (b *BadgerEngine) beginSchemaWriteContext(ctx context.Context) (func(), error) {
	release, err := util.LockContext(ctx, b.writeBarrier.Lock, b.writeBarrier.Unlock)
	if err != nil {
		return nil, err
	}
	b.mu.RLock()
	closed := b.closed
	b.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	if closed {
		release()
		return nil, ErrStorageClosed
	}
	return release, nil
}

// beginPrefixDeletion fences only the affected namespace(s), captures one
// fixed frontier while all earlier publications have finished, then releases
// the exclusive barrier before replay. Later foreign generations cannot
// extend the wait. Cancellation removes the fence without deleting data.
func (b *BadgerEngine) beginPrefixDeletion(ctx context.Context, prefix string) (func(), error) {
	release, err := b.beginSchemaWriteContext(ctx)
	if err != nil {
		return nil, err
	}
	unfence, err := b.addDeletingPrefix(prefix)
	if err != nil {
		release()
		return nil, err
	}
	var frontier *CommitBuffer
	if b.writeBehind != nil {
		frontier = b.writeBehind.captureFrontier()
	}
	release()

	fail := func(err error) (func(), error) { unfence(); return nil, err }
	if b.writeBehind != nil {
		if err := b.writeBehind.waitThrough(ctx, frontier); err != nil {
			return fail(err)
		}
	}
	release, err = b.beginSchemaWriteContext(ctx)
	if err != nil {
		return fail(err)
	}
	if b.writeBehind != nil {
		pending := 0
		if namespace, whole := strings.CutSuffix(prefix, ":"); whole && !strings.Contains(namespace, ":") {
			pending = b.writeBehind.PendingOpsInNamespace(namespace)
		} else {
			pending = b.writeBehind.pendingOpsForPrefix(prefix)
		}
		if pending != 0 {
			release()
			return fail(fmt.Errorf("%w: target prefix still has pending writes", ErrConflict))
		}
	}
	return func() { release(); unfence() }, nil
}
