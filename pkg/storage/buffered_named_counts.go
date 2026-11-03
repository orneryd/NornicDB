package storage

// Author: storage-format-owners lane (Codex), 1 November 2026.

import "context"

// sumBufferedNamedCount retains the scoped map lookup while sharing the
// cross-namespace scan and its bounded request cancellation checks.
func sumBufferedNamedCount[K comparable](ctx context.Context, cancel *bulkReadCancellation, deltas map[K]int64, key K, scoped bool, matches func(K) bool) (int64, error) {
	if scoped {
		return deltas[key], nil
	}
	var total int64
	for key, delta := range deltas {
		if err := cancel.check(ctx); err != nil {
			return 0, err
		}
		if matches(key) {
			total += delta
		}
	}
	return total, nil
}

// namedCountDelta is the single buffered label/type-count reader. Public
// context-free adapters pass their own lifetime; request reads pass their ctx.
// That lifetime governs the read admission too, so a canceled count leaves the
// wait with the caller's own error instead of waiting for the buffer to be free.
func (w *WriteBehindBuffer) namedCountDelta(ctx context.Context, kind byte, namespace, name string, scoped bool) (int64, error) {
	if err := w.mu.RLockContext(ctx); err != nil {
		return 0, err
	}
	defer w.mu.RUnlock()
	var total int64
	var cancel bulkReadCancellation
	sum := func(buf *CommitBuffer) error {
		if buf.countsApplied.Load() {
			return nil
		}
		for _, commit := range buf.commits {
			if err := cancel.check(ctx); err != nil {
				return err
			}
			var delta int64
			var err error
			if kind == countDeltaKindEdgeType {
				delta, err = sumBufferedNamedCount(ctx, &cancel, commit.edgeTypeDeltas, namespaceEdgeType{namespace: namespace, edgeType: name}, scoped, func(key namespaceEdgeType) bool { return key.edgeType == name })
			} else {
				delta, err = sumBufferedNamedCount(ctx, &cancel, commit.labelDeltas, namespaceLabel{namespace: namespace, label: name}, scoped, func(key namespaceLabel) bool { return key.label == name })
			}
			if err != nil {
				return err
			}
			total += delta
		}
		return nil
	}
	if err := sum(w.active); err != nil {
		return 0, err
	}
	for i := len(w.draining) - 1; i >= 0; i-- {
		if w.draining[i].IsApplied() {
			continue
		}
		if err := sum(w.draining[i]); err != nil {
			return 0, err
		}
	}
	return total, ctx.Err()
}
