package storage

// Author: storage-format-owners lane (Codex), 1 November 2026.

import (
	"context"
)

// sumBufferedNamedCount retains the scoped map lookup while sharing the
// cross-namespace scan and its bounded request cancellation checks. A caller
// that sums several names of one scope passes singleLookup false with the
// predicate it needs.
func sumBufferedNamedCount[K comparable](ctx context.Context, cancel *bulkReadCancellation, deltas map[K]int64, key K, singleLookup bool, matches func(K) bool) (int64, error) {
	if singleLookup {
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

// countStoreDelta is the one buffered reader of the derived count families. It
// sums the deltas the not-yet-applied commits add to the requested count.
// ReadCountStore uses its locked form while proving visibility from the same
// buffer generation and persisted snapshot.
func (w *WriteBehindBuffer) countStoreDelta(ctx context.Context, request CountStoreReadRequest) (int64, error) {
	if err := w.mu.RLockContext(ctx); err != nil {
		return 0, err
	}
	defer w.mu.RUnlock()
	return w.countStoreDeltaLocked(ctx, request)
}

// countStoreDeltaLocked sums one count-store read over the live generations.
// The caller holds w.mu.
func (w *WriteBehindBuffer) countStoreDeltaLocked(ctx context.Context, request CountStoreReadRequest) (int64, error) {
	scoped := request.Namespace != ""
	// A positional read counts (endpoint label, type) deltas, so the requested
	// types play the name role of the shared scope filter.
	byName := inScopeNamedCountFilter(request.Namespace, []string{request.Name})
	typeFilter := inScopeNamedCountFilter(request.Namespace, request.Types)
	sub := prefixMVCCMetaEdgeTypeStartLabelCount
	if request.Kind == CountStoreRelationshipsByEndLabel {
		sub = prefixMVCCMetaEdgeTypeEndLabelCount
	}
	var cancel bulkReadCancellation
	var total int64
	for _, buf := range w.liveBuffers() {
		if buf.countsApplied.Load() {
			continue
		}
		for _, commit := range buf.commits {
			if err := cancel.check(ctx); err != nil {
				return 0, err
			}
			switch request.Kind {
			case CountStoreNodesByLabel:
				delta, err := sumBufferedNamedCount(ctx, &cancel, commit.labelDeltas,
					namespaceLabel{namespace: request.Namespace, label: request.Name}, scoped, byName)
				if err != nil {
					return 0, err
				}
				total += delta
			case CountStoreRelationshipsByType:
				single := ""
				if len(request.Types) == 1 {
					single = request.Types[0]
				}
				delta, err := sumBufferedNamedCount(ctx, &cancel, commit.edgeTypeDeltas,
					namespaceEdgeType{namespace: request.Namespace, edgeType: single}, scoped && len(request.Types) == 1, func(key namespaceEdgeType) bool {
						return typeFilter(namespaceLabel{namespace: key.namespace, label: key.edgeType})
					})
				if err != nil {
					return 0, err
				}
				total += delta
			case CountStoreRelationshipsByStartLabel, CountStoreRelationshipsByEndLabel:
				for key, delta := range commit.edgeTypeLabelDeltas {
					if key.sub != sub || key.label != request.Name {
						continue
					}
					if !typeFilter(namespaceLabel{namespace: key.namespace, label: key.edgeType}) {
						continue
					}
					total += delta
				}
			}
		}
	}
	return total, ctx.Err()
}

// namedCountDelta is the buffered delta of one named count family. Public
// context-free adapters pass their own lifetime; request reads pass their ctx.
// That lifetime governs the read admission too, so a canceled count leaves the
// wait with the caller's own error instead of waiting for the buffer to be free.
func (w *WriteBehindBuffer) namedCountDelta(ctx context.Context, kind byte, namespace, name string, scoped bool) (int64, error) {
	request := CountStoreReadRequest{Kind: CountStoreRelationshipsByType, Namespace: namespace, Types: []string{name}}
	if kind == countDeltaKindLabel {
		request.Kind = CountStoreNodesByLabel
		request.Name = name
		request.Types = nil
	}
	total, err := w.countStoreDelta(ctx, request)
	return total, err
}

// collectCountLabelNamesLocked contributes only label-index names, never
// buffered entity bodies. It visits each label once per generation rather than
// every commit. Namespace is a conservative candidate scope: the subsequent
// canonical overlay/index read proves whether that label is present there.
// The caller holds w.mu across its count snapshot.
func (w *WriteBehindBuffer) collectCountLabelNamesLocked(ctx context.Context, namespace string, wanted func(namespaceLabel) bool, names map[namespaceLabel]struct{}) error {
	var cancel bulkReadCancellation
	for _, buf := range w.liveBuffers() {
		if buf.countsApplied.Load() {
			continue
		}
		for label := range buf.nodesByLabel {
			if err := cancel.check(ctx); err != nil {
				return err
			}
			key := namespaceLabel{namespace: namespace, label: label}
			if wanted(key) {
				names[key] = struct{}{}
			}
		}
	}
	return nil
}
