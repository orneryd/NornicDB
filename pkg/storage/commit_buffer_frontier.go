package storage

import (
	"context"
	"strings"
	"time"
)

// captureFrontier seals the active generation and returns the last generation
// acknowledged so far. The immutable frontier never includes later appends.
func (w *WriteBehindBuffer) captureFrontier() *CommitBuffer {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.rotateLocked()
	if len(w.draining) == 0 {
		return nil
	}
	return w.draining[len(w.draining)-1]
}

// waitThrough waits for the FIFO replay owner, not for an empty live buffer.
// Replay runs on the existing worker so a cancelled waiter cannot be trapped
// in an apply callback. A failed apply is returned, rather than retried here.
func (w *WriteBehindBuffer) waitThrough(ctx context.Context, frontier *CommitBuffer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if frontier == nil {
		return nil
	}
	w.signal()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !frontier.IsApplied() {
		if err := w.LastErr(); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.done:
			return ErrStorageClosed
		case <-tick.C:
		}
	}
	return ctx.Err()
}

// PendingOpsInNamespace reports unapplied mutations belonging to exactly one
// database, including the active generation. It never tests foreign traffic.
func (w *WriteBehindBuffer) PendingOpsInNamespace(namespace string) int {
	return w.pendingOpsMatching(func(candidate string) bool { return candidate == namespace })
}

func (w *WriteBehindBuffer) pendingOpsForPrefix(prefix string) int {
	return w.pendingOpsMatching(func(namespace string) bool {
		return prefixesOverlap(namespace+":", prefix)
	})
}

func prefixesOverlap(left, right string) bool {
	return strings.HasPrefix(left, right) || strings.HasPrefix(right, left)
}

func (w *WriteBehindBuffer) pendingOpsMatching(matches func(string) bool) int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	total := 0
	count := func(buf *CommitBuffer) {
		if buf.IsApplied() {
			return
		}
		for _, commit := range buf.commits {
			if matches(commit.namespace) {
				total += max(1, len(commit.ops)+len(commit.writes)+len(commit.deletes))
			}
		}
		// Legacy append APIs carry IDs rather than captured transaction namespaces.
		for _, op := range buf.order {
			id := op.id
			if op.kind == bufferedOpDelete && len(id) > 0 && id[0] < 0x20 {
				id = id[1:]
			}
			namespace, _, ok := ParseDatabasePrefix(id)
			if ok && matches(namespace) {
				total++
			}
		}
	}
	count(w.active)
	for _, buf := range w.draining {
		count(buf)
	}
	return total
}
