package storage

import (
	"context"
	"errors"
	"sort"
	"strconv"

	"github.com/dgraph-io/badger/v4"
)

// RelationshipPropertySeeker is an optional equality access path. A seek returns
// candidate relationships in the engine's read view; callers still apply Cypher
// equality and endpoint predicates. Unsupported views/values return answered=false,
// never an incomplete index miss. For example, a namespaced engine returns local
// IDs, while a transaction returns its snapshot overlaid with its pending writes.
type RelationshipPropertySeeker interface {
	RelationshipPropertySeeksAvailable() bool
	SeekRelationships(ctx context.Context, relType string, properties []string, values []interface{}) (edges []*Edge, answered bool, err error)
}

// ScopedRelationshipPropertySeeker is the inner engine's database-bound seek.
// namespace is a database name, without the ID separator.
// Its availability is separate from the local form: a transparent wrapper may
// expose both methods while its immediate inner view supports only one form.
type ScopedRelationshipPropertySeeker interface {
	ScopedRelationshipPropertySeeksAvailable() bool
	SeekRelationshipsInNamespace(context.Context, string, string, []string, []interface{}) ([]*Edge, bool, error)
}

// RelationshipEqualityIndexes returns RANGE definitions for this relationship
// type, including constraint backing indexes, in deterministic name order.
func (sm *SchemaManager) RelationshipEqualityIndexes(relType string) []RangeIndex {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	var indexes []RangeIndex
	for _, idx := range sm.rangeIndexes {
		if idx.EntityType != ConstraintEntityRelationship || idx.Label != relType || idx.effectiveKind() != IndexKindRange {
			continue
		}
		properties := idx.Properties
		if len(properties) == 0 && idx.Property != "" {
			properties = []string{idx.Property}
		}
		if len(properties) > 0 {
			indexes = append(indexes, RangeIndex{Name: idx.Name, Label: idx.Label, Properties: append([]string(nil), properties...)})
		}
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i].Name < indexes[j].Name })
	return indexes
}

// RelationshipPropertySeeksAvailable reports whether the maintained index
// describes this engine's read view. Other views keep the existing scan path.
func (b *BadgerEngine) RelationshipPropertySeeksAvailable() bool {
	return b.writeBehind == nil && !b.decayEnabled
}

// ScopedRelationshipPropertySeeksAvailable describes the qualified backing view.
func (b *BadgerEngine) ScopedRelationshipPropertySeeksAvailable() bool {
	return b.RelationshipPropertySeeksAvailable()
}

func relationshipSeekValues(properties []string, values []interface{}) ([]interface{}, bool) {
	if len(properties) == 0 || len(properties) != len(values) {
		return nil, false
	}
	props := make(map[string]interface{}, len(values))
	for i, p := range properties {
		props[p] = values[i]
	}
	return relationshipConstraintValueKeys(properties, props, true)
}

// RelationshipIndexValuesSupported identifies values the equality owner
// can key completely. Other Cypher values continue through the existing scan.
func RelationshipIndexValuesSupported(values []interface{}) bool {
	properties := make([]string, len(values))
	for i := range properties {
		properties[i] = strconv.Itoa(i)
	}
	_, ok := relationshipSeekValues(properties, values)
	return ok
}

// SeekRelationshipsInNamespace uses the same populated, maintained equality
// index as uniqueness validation. It is derived from existing edge records and
// rebuilt lazily after restart; no new persisted representation is introduced.
func (b *BadgerEngine) SeekRelationshipsInNamespace(ctx context.Context, namespace, relType string, properties []string, values []interface{}) ([]*Edge, bool, error) {
	if !b.RelationshipPropertySeeksAvailable() || namespace == "" {
		return nil, false, nil
	}
	canonical, ok := relationshipSeekValues(properties, values)
	if !ok {
		return nil, false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	// Writers retain this existing lease through derived-index publication.
	// Drain their publication tails, pin the body view and copy its matching
	// candidates before another commit can move a value between relationships.
	b.writeBarrier.Lock()
	publicationHeld := true
	defer func() {
		if publicationHeld {
			b.writeBarrier.Unlock()
		}
	}()
	key := relationshipUniqueIndexKey{namespace: namespace, relType: relType, properties: relationshipPropertyKey(properties)}
	var edges []*Edge
	// Point reads bypass body caches: equality publication must not depend on a
	// cache notification from an overlapping writer arriving in commit order.
	err := b.withView(func(txn *badger.Txn) error {
		ids, err := b.relationshipEqualityCandidates(ctx, key, properties, canonical, func(visit func(*Edge) error) error {
			return b.StreamStoredEdgesByTypeInScope(ctx, namespace+":", relType, visit)
		})
		if err != nil {
			return err
		}
		// The physical view keeps these bodies immutable. Release writers now,
		// rather than excluding them for every candidate's body/decode work.
		b.writeBarrier.Unlock()
		publicationHeld = false
		for _, id := range ids {
			if err := ctx.Err(); err != nil {
				return err
			}
			edge, err := b.readEdgeInTxn(txn, EdgeID(id))
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if edge != nil && edge.Type == relType && !b.hidesEdge(visibleRead, edge, DecayScoringTime()) {
				edges = append(edges, edge)
			}
		}
		return nil
	})
	return edges, true, err
}

// RelationshipPropertySeeksAvailable forwards the inner seek capability.
func (n *NamespacedEngine) RelationshipPropertySeeksAvailable() bool {
	p, ok := n.inner.(ScopedRelationshipPropertySeeker)
	return ok && p.ScopedRelationshipPropertySeeksAvailable()
}

// SeekRelationships returns only this database's candidates, with local IDs.
func (n *NamespacedEngine) SeekRelationships(ctx context.Context, relType string, properties []string, values []interface{}) ([]*Edge, bool, error) {
	p, ok := n.inner.(ScopedRelationshipPropertySeeker)
	if !ok {
		return nil, false, nil
	}
	edges, answered, err := p.SeekRelationshipsInNamespace(ctx, n.namespace, relType, properties, values)
	if err != nil || !answered {
		return nil, answered, err
	}
	var out []*Edge
	for _, edge := range edges {
		if n.hasEdgePrefix(edge.ID) {
			out = append(out, n.toUserEdge(edge))
		}
	}
	return out, true, nil
}

// RelationshipPropertySeeksAvailable reports whether this transaction has a
// pinned snapshot usable by the equality index owner.
func (tx *BadgerTransaction) RelationshipPropertySeeksAvailable() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.engine.RelationshipPropertySeeksAvailable() && tx.snapshotTx != nil
}

// SeekRelationships reads an immutable index for the pinned physical/MVCC view,
// then merges pending changes. A current-state index cannot answer an old snapshot:
// a peer may have moved or deleted a relationship since the transaction began.
func (tx *BadgerTransaction) SeekRelationships(ctx context.Context, relType string, properties []string, values []interface{}) ([]*Edge, bool, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return nil, true, err
	}
	if !tx.engine.RelationshipPropertySeeksAvailable() || tx.snapshotTx == nil || tx.namespace == "" || tx.readTS.IsZero() {
		return nil, false, nil
	}
	canonical, ok := relationshipSeekValues(properties, values)
	if !ok {
		return nil, false, nil
	}
	if tx.relationshipSeekIndexes == nil {
		tx.relationshipSeekIndexes = &relationshipUniqueIndexSet{}
	}
	key := relationshipUniqueIndexKey{namespace: tx.namespace, relType: relType, properties: relationshipPropertyKey(properties)}
	ids, err := tx.engine.relationshipEqualityCandidatesInSet(ctx, tx.relationshipSeekIndexes, key, properties, canonical, func(visit func(*Edge) error) error {
		return tx.engine.streamEdgesByTypeAtSnapshotWithView(ctx, tx.labelScanScopeLocked(), relType, tx.readTS, tx.withSnapshotViewLocked, storedRead, visit)
	})
	if err != nil {
		return nil, true, err
	}
	var edges []*Edge
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, true, err
		}
		edgeID := EdgeID(id)
		if _, changed := tx.pendingEdges[edgeID]; changed {
			continue
		}
		if _, deleted := tx.deletedEdges[edgeID]; deleted {
			continue
		}
		edge, err := tx.getCommittedEdgeReadLocked(edgeID, visibleRead)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, true, err
		}
		edges = append(edges, edge)
	}
	for id, edge := range tx.pendingEdges {
		if _, deleted := tx.deletedEdges[id]; deleted || edge.Type != relType || tx.engine.hidesEdge(visibleRead, edge, DecayScoringTime()) {
			continue
		}
		keys, valid := relationshipConstraintValueKeys(properties, edge.Properties, true)
		if !valid {
			continue
		}
		matches := true
		for i := range keys {
			if keys[i] != canonical[i] {
				matches = false
				break
			}
		}
		if matches {
			edges = append(edges, copyEdge(edge))
		}
	}
	return edges, true, nil
}
