package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"

	badger "github.com/dgraph-io/badger/v4"
)

var _ CountStoreReadEngine = (*BadgerEngine)(nil)

// positionalSub maps a positional count family to its persisted tier byte.
func positionalSub(kind CountStoreReadKind) byte {
	if kind == CountStoreRelationshipsByEndLabel {
		return prefixMVCCMetaEdgeTypeEndLabelCount
	}
	return prefixMVCCMetaEdgeTypeStartLabelCount
}

// ReadCountStore answers one count-store read: the counter value and whether
// that value describes the rows a read in the same scope returns, both from one
// persisted snapshot and one buffered view, under the count families' read
// holds. A persisted count that cannot be decoded is repaired by its family's
// canonical rebuild and read again, as the other derived-count readers do.
func (b *BadgerEngine) ReadCountStore(ctx context.Context, request CountStoreReadRequest) (CountStoreRead, error) {
	if err := b.pendingNamespaceConflict(request.Namespace); err != nil {
		return CountStoreRead{}, err
	}
	if err := b.ensureOpen(false); err != nil {
		return CountStoreRead{}, err
	}
	answer, err := b.readCountStoreSnapshot(ctx, request)
	if !errors.Is(err, errMalformedDerivedCount) {
		return answer, err
	}
	if rebuildErr := b.rebuildCountStoreFamilyLocked(ctx, request.Kind); rebuildErr != nil {
		return CountStoreRead{}, rebuildErr
	}
	return b.readCountStoreSnapshot(ctx, request)
}

// readCountStoreSnapshot holds the count families' read locks and answers the
// request from one view and one buffered view. Both halves read the label
// counts - the counter's own family lock orders the count against folds and
// replay, and the label lock orders the visibility half the same way - so the
// locks are taken in the publication path's order: label, then relationship.
func (b *BadgerEngine) readCountStoreSnapshot(ctx context.Context, request CountStoreReadRequest) (CountStoreRead, error) {
	labelMu := b.namedCountMutex(countDeltaKindLabel)
	if err := labelMu.RLockContext(ctx); err != nil {
		return CountStoreRead{}, err
	}
	defer labelMu.RUnlock()
	if request.Kind != CountStoreNodesByLabel {
		edgeMu := b.namedCountMutex(countDeltaKindEdgeType)
		if err := edgeMu.RLockContext(ctx); err != nil {
			return CountStoreRead{}, err
		}
		defer edgeMu.RUnlock()
	}

	if b.writeBehind != nil {
		if err := b.writeBehind.mu.RLockContext(ctx); err != nil {
			return CountStoreRead{}, err
		}
		defer b.writeBehind.mu.RUnlock()
	}
	var answer CountStoreRead
	var hidden bool
	err := b.withView(func(txn *badger.Txn) error {
		count, err := b.sumCountStoreInTxn(ctx, txn, request)
		if err != nil {
			return err
		}
		answer.Count = count
		if request.HiddenLabelPrefix == "" {
			return nil
		}
		hidden, err = b.scopeHasRelevantHiddenInTxn(ctx, txn, request)
		return err
	})
	if err != nil {
		return CountStoreRead{}, err
	}
	if b.writeBehind != nil {
		// The same buffer hold answers the count and the visibility half of the
		// commits it still holds.
		delta, err := b.writeBehind.countStoreDeltaLocked(ctx, request)
		if err != nil {
			return CountStoreRead{}, err
		}
		answer.Count += delta
	}
	answer.DescribesRows = !hidden
	return answer, nil
}

// rebuildCountStoreFamilyLocked rebuilds one count family from authoritative
// state under its write hold.
func (b *BadgerEngine) rebuildCountStoreFamilyLocked(ctx context.Context, kind CountStoreReadKind) error {
	if kind == CountStoreNodesByLabel {
		b.labelCountWriteMu.Lock()
		defer b.labelCountWriteMu.Unlock()
		return b.rebuildAuthoritativeLabelCountsLocked(ctx)
	}
	b.edgeTypeCountWriteMu.Lock()
	defer b.edgeTypeCountWriteMu.Unlock()
	return b.rebuildAuthoritativeEdgeTypeCountsLocked(ctx)
}

// sumCountStoreInTxn reads one count family from the snapshot txn: the label
// counts, the relationship-type counts of the requested types (every type when
// none is named), or their positional (endpoint label, type) counts.
func (b *BadgerEngine) sumCountStoreInTxn(ctx context.Context, txn *badger.Txn, request CountStoreReadRequest) (int64, error) {
	switch request.Kind {
	case CountStoreNodesByLabel:
		if request.Name == "" {
			return 0, fmt.Errorf("count-store read: a node label count needs a label")
		}
		if request.Namespace != "" {
			return b.readNamedCountInTxn(ctx, txn, countDeltaKindLabel, request.Namespace, request.Name)
		}
		return b.sumNamedCountsInTxn(ctx, txn, countDeltaKindLabel, []string{request.Name})
	case CountStoreRelationshipsByType:
		// An empty type list counts every relationship type the scope holds, so
		// the answer stays in the counter keyspace: no relationship is read.
		if request.Namespace != "" {
			return b.sumEveryNamedCountInTxn(ctx, txn, countDeltaKindEdgeType, request.Namespace, request.Types)
		}
		return b.sumNamedCountsInTxn(ctx, txn, countDeltaKindEdgeType, request.Types)
	case CountStoreRelationshipsByStartLabel, CountStoreRelationshipsByEndLabel:
		if request.Name == "" {
			return 0, fmt.Errorf("count-store read: a positional count needs an endpoint label")
		}
		return b.sumEdgeTypeLabelCountsInTxn(ctx, txn, positionalSub(request.Kind), request.Namespace, request.Name, request.Types)
	}
	return 0, fmt.Errorf("count-store read: unknown count family %d", request.Kind)
}

// scopeHasRelevantHiddenInTxn asks whether hidden nodes can change this count.
// It visits only internal-label indexes or their positional counters, never the
// counted population. The caller holds the count-family and buffer read locks.
func (b *BadgerEngine) scopeHasRelevantHiddenInTxn(ctx context.Context, txn *badger.Txn, request CountStoreReadRequest) (bool, error) {
	namespace, prefix := request.Namespace, request.HiddenLabelPrefix
	hidden := func(candidate namespaceLabel) bool {
		return (namespace == "" || candidate.namespace == namespace) && strings.HasPrefix(candidate.label, prefix)
	}
	basePrefix := labelCountPrefix()
	deltaPrefix := countDeltaAllPrefix(countDeltaKindLabel)
	if namespace != "" {
		basePrefix = append(labelCountNamespacePrefix(namespace), prefix...)
		deltaPrefix = append(countDeltaNamespacePrefix(countDeltaKindLabel, namespace), prefix...)
	}
	candidates := make(map[namespaceLabel]struct{})
	collect := func(candidate namespaceLabel, _ []byte) error { candidates[candidate] = struct{}{}; return nil }
	if err := visitCountKeysInTxn(ctx, txn, basePrefix, namedCountKeyParts, hidden, collect); err != nil {
		return false, err
	}
	if err := visitCountKeysInTxn(ctx, txn, deltaPrefix, countDeltaLabelKeyParts, hidden, collect); err != nil {
		return false, err
	}
	if b.writeBehind != nil {
		if err := b.writeBehind.collectCountLabelNamesLocked(ctx, namespace, hidden, candidates); err != nil {
			return false, err
		}
	}
	for candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if request.Kind == CountStoreNodesByLabel {
			relevant, err := b.hiddenLabelIntersectsNodeLabelInTxn(ctx, txn, candidate, request.Name)
			if err != nil || relevant {
				return relevant, err
			}
			continue
		}
		for _, kind := range []CountStoreReadKind{CountStoreRelationshipsByStartLabel, CountStoreRelationshipsByEndLabel} {
			count, err := b.sumEdgeTypeLabelCountsInTxn(ctx, txn, positionalSub(kind), candidate.namespace, candidate.label, request.Types)
			if err != nil {
				return false, err
			}
			if b.writeBehind != nil {
				delta, err := b.writeBehind.countStoreDeltaLocked(ctx, CountStoreReadRequest{Kind: kind, Namespace: candidate.namespace, Name: candidate.label, Types: request.Types})
				if err != nil {
					return false, err
				}
				count += delta
			}
			if count > 0 {
				return true, nil
			}
		}
	}
	return false, nil
}

// hiddenLabelIntersectsNodeLabelInTxn proves relevance using existing label
// membership keys and the canonical buffered label overlay. It does not hydrate
// the visible population or any embedding body.
func (b *BadgerEngine) hiddenLabelIntersectsNodeLabelInTxn(ctx context.Context, txn *badger.Txn, candidate namespaceLabel, wanted string) (bool, error) {
	scope := ""
	if candidate.namespace != "" {
		scope = candidate.namespace + ":"
	}
	var touched map[NodeID]bool
	if b.writeBehind != nil {
		overlay, err := b.readBufferedEntities(ctx, b.writeBehind, storedRead, bufferedEntityRead{kind: bufferedLabel, label: candidate.label, prefix: scope})
		if err != nil {
			return false, err
		}
		touched = overlay.nodesTouched
		for _, node := range overlay.nodes {
			if hasLabel(node.Labels, wanted) {
				return true, nil
			}
		}
	}
	ready, err := b.labelIndexReadyInTxn(txn)
	if err != nil {
		return false, err
	}
	if !ready {
		return true, nil
	}
	reader, err := b.newLabelIndexReader(ctx, txn, scope, candidate.label, "", storedRead, false)
	if err != nil {
		return false, err
	}
	defer reader.Close()
	for reader.Next() {
		if touched[reader.NodeID()] {
			continue
		}
		number, ok := extractNodeNumIDFromLabelIndex(reader.IndexKey(), len(candidate.label))
		if !ok {
			continue
		}
		key := labelIndexKey(wanted, number)
		if _, err := txn.Get(key); err == nil {
			return true, nil
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return false, err
		}
	}
	return false, reader.Err()
}
