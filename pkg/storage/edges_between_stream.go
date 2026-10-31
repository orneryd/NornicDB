// Caller-ownership correction author: Codex, NOR-160, 29 October 2026.
package storage

import (
	"errors"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/callerowned"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// EdgesBetweenMatcher finds the edges of one type from a start node to an
// end node that satisfy a predicate over some of their properties: a
// relationship MERGE with bound endpoints compares every candidate's
// identity properties, and Neo4j's Expand(Into) reads just those.
//
// match sees each candidate with its ID, Type, StartNode, EndNode and at
// least the listed properties it has. The candidate is read-only and may share
// its values with a cache or the transaction (edgeMatchCandidate): match must
// not change it or keep it past the call. A wrapper that shows it with other
// IDs does so on its own shallow copy.
// The edges it accepts are returned whole, as GetEdgesBetween returns them,
// from the same read.
type EdgesBetweenMatcher interface {
	MatchEdgesBetween(startID, endID NodeID, edgeType string, properties []string, match func(*Edge) bool) ([]*Edge, error)
}

// MatchEdgesBetween is engine's EdgesBetweenMatcher, or GetEdgesBetween and
// the same filter for an engine without one: both give the same edges.
func MatchEdgesBetween(engine Engine, startID, endID NodeID, edgeType string, properties []string, match func(*Edge) bool) ([]*Edge, error) {
	if matcher, ok := engine.(EdgesBetweenMatcher); ok {
		return matcher.MatchEdgesBetween(startID, endID, edgeType, properties, match)
	}
	edges, err := engine.GetEdgesBetween(startID, endID)
	if err != nil {
		return nil, err
	}
	return filterEdgesBetween(edges, startID, endID, edgeType, properties, match), nil
}

// filterEdgesBetween returns the edges of edges from startID to endID of
// type edgeType that match accepts, shown their properties projected.
func filterEdgesBetween(edges []*Edge, startID, endID NodeID, edgeType string, properties []string, match func(*Edge) bool) []*Edge {
	var matched []*Edge
	for _, edge := range edges {
		if edge == nil || edge.StartNode != startID || edge.EndNode != endID || edge.Type != edgeType {
			continue
		}
		if match(edgeMatchCandidate(edge, properties)) {
			matched = append(matched, edge)
		}
	}
	return matched
}

// edgeMatchCandidate is the read-only image a MatchEdgesBetween predicate
// inspects: the requested properties (all of them for nil), selected by the
// result projection. Nothing is copied for a candidate; only an accepted edge
// is, once, when it is delivered (NOR-160).
func edgeMatchCandidate(edge *Edge, properties []string) *Edge {
	if properties == nil {
		return edge
	}
	view := *edge
	view.Properties = (QueryResultProjection{}).EdgeProperties(edge, properties)
	return &view
}

// MatchEdgesBetween matches the engine's edges between two nodes. Under
// write-behind it overlays the buffer the same way GetEdgesBetween does: a
// buffered endpoint delete hides committed rows, a buffered edge delete
// hides that row, and buffered live edges of the type are appended
// (PR #1043 read-overlay parity — MERGE identity reads).
func (b *BadgerEngine) MatchEdgesBetween(startID, endID NodeID, edgeType string, properties []string, match func(*Edge) bool) ([]*Edge, error) {
	if err := b.pendingNamespaceConflict(namespaceForNodeID(endID)); err != nil {
		return nil, err
	}

	if err := b.pendingNamespaceConflict(namespaceForNodeID(startID)); err != nil {
		return nil, err
	}

	if b.writeBehind == nil {
		return b.committedMatchEdgesBetween(startID, endID, edgeType, properties, match)
	}
	var overlay bufferedEntityResult
	view, err := b.captureReadView(func(w *WriteBehindBuffer) {
		overlay = b.readBufferedEntities(w, visibleRead, bufferedEntityRead{kind: bufferedBetween, source: startID, target: endID})
	}, readViewOptions{scope: namespaceForNodeID(startID)})
	if err != nil {
		return nil, err
	}
	defer view.Close()
	if overlay.sourceDeleted || overlay.targetDeleted {
		return nil, nil
	}
	matched, err := b.committedMatchEdgesBetween(startID, endID, edgeType, properties, func(edge *Edge) bool {
		if _, dead := overlay.suppressed[edge.ID]; dead {
			return false
		}
		return match(edge)
	}, view.withView)
	if err != nil {
		return nil, err
	}
	return append(matched, overlay.ownedEdges(func(edge *Edge) bool {
		return edge != nil && edge.Type == edgeType && match(edgeMatchCandidate(edge, properties))
	})...), nil
}

// committedMatchEdgesBetween reads the typed edge-between set index, matches
// each candidate from the edge cache or, on a miss, from its decoded header and
// only the requested properties, and returns the accepted ones whole. A pair with no set-index entry at all may
// predate the index: its legacy outgoing index is read as GetEdgesBetween
// reads it.
func (b *BadgerEngine) committedMatchEdgesBetween(startID, endID NodeID, edgeType string, properties []string, match func(*Edge) bool, views ...func(func(*badger.Txn) error) error) ([]*Edge, error) {
	withView := b.withView
	if len(views) > 0 {
		withView = views[0]
	}
	if startID == "" || endID == "" {
		return nil, ErrInvalidID
	}
	if match == nil {
		return nil, ErrInvalidData
	}
	if err := b.ensureOpen(false); err != nil {
		return nil, err
	}
	startNum, startOK := b.idDict.lookupNodeNumID(startID)
	endNum, endOK := b.idDict.lookupNodeNumID(endID)
	if !startOK || !endOK {
		return nil, nil
	}
	include := propertyProjectionSet(properties)
	var matched []*Edge
	indexed := false
	err := withView(func(txn *badger.Txn) error {
		checkTombstones := b.decayEnabled && !b.revealAll.Load()
		nowNanos := DecayScoringTime()
		prefix := typedEdgeBetweenIndexPrefix(startNum, endNum, edgeType)
		opts := badgerPrefixIteratorOptions(prefix)
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			indexed = true
			if checkTombstones && hasIndexTombstone(txn, it.Item().Key()) {
				continue
			}
			var edgeID EdgeID
			if err := it.Item().Value(func(val []byte) error {
				edgeID = EdgeID(val)
				return nil
			}); err != nil {
				return err
			}
			if edgeID == "" {
				continue
			}
			// A cached body is the committed edge GetEdge would return:
			// match it without reading or decoding. The predicate sees its
			// own projected candidate and only an accepted edge is delivered.
			if cached, hit := b.cacheLoadEdge(edgeID, edgeCacheRead{match: func(cached *Edge) bool {
				return cached.StartNode == startID && cached.EndNode == endID && cached.Type == edgeType &&
					!b.filterEntityByDecay(visibleRead, cached, nowNanos) && match(edgeMatchCandidate(cached, properties))
			}}); hit {
				if cached != nil {
					matched = append(matched, cached)
				}
				continue
			}
			item, err := txn.Get(edgeKey(edgeID))
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if err := item.Value(func(data []byte) error {
				candidate, _, _, err := b.decodeEdgeCompactV2(namespaceForEdgeID(edgeID), data, compactEdgeProjection{Start: startNum, End: endNum, Include: include})
				if err != nil {
					return err
				}
				if candidate == nil {
					return nil
				}
				candidate.ID, candidate.StartNode, candidate.EndNode = edgeID, startID, endID
				if candidate.Type != edgeType || b.filterEntityByDecay(visibleRead, candidate, nowNanos) || !match(candidate) {
					return nil
				}
				edge, err := b.decodeEdgeBodyByID(data, edgeID)
				if err != nil {
					return err
				}
				matched = append(matched, edge)
				return nil
			}); err != nil {
				return err
			}
		}
		if !indexed {
			// Other edges between the pair mean it is indexed.
			pairIt := txn.NewIterator(badgerPrefixIteratorOptions(edgeBetweenIndexPrefix(startNum, endNum)))
			defer pairIt.Close()
			pairIt.Rewind()
			indexed = pairIt.Valid()
		}
		return nil
	})
	if err != nil || indexed {
		return matched, err
	}
	legacy, err := b.edgesBetweenFromLegacyOutgoingIndex(startID, endID, edgeType, withView)
	if err != nil {
		return nil, err
	}
	if len(legacy) > 0 {
		_ = b.selfHealEdgeBetweenIndexes(legacy)
	}
	return filterEdgesBetween(legacy, startID, endID, edgeType, properties, match), nil
}

// MatchEdgesBetween matches the engine's edges as this transaction sees
// them: its deleted edges are gone, and its created or updated ones replace
// the stored versions, as in GetEdgesBetween.
func (tx *BadgerTransaction) MatchEdgesBetween(startID, endID NodeID, edgeType string, properties []string, match func(*Edge) bool) ([]*Edge, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return nil, err
	}
	if err := tx.pinNamespaceFromIDLocked(string(startID)); err != nil {
		return nil, err
	}
	if err := tx.pinNamespaceFromIDLocked(string(endID)); err != nil {
		return nil, err
	}
	matched, err := tx.engine.MatchEdgesBetween(startID, endID, edgeType, properties, func(edge *Edge) bool {
		if _, deleted := tx.deletedEdges[edge.ID]; deleted {
			return false
		}
		if _, pending := tx.pendingEdges[edge.ID]; pending {
			return false
		}
		return match(edge)
	})
	if err != nil {
		return nil, err
	}
	// Engine matches are caller-owned; only pending images are copied here.
	var accepted []*Edge
	for _, edge := range tx.pendingEdges {
		if edge == nil || edge.StartNode != startID || edge.EndNode != endID || edge.Type != edgeType {
			continue
		}
		if match(edgeMatchCandidate(edge, properties)) {
			accepted = append(accepted, edge)
		}
	}
	return append(matched, callerowned.Each(accepted, copyEdge)...), nil
}

// MatchEdgesBetween matches the engine's edges with this namespace's IDs.
func (n *NamespacedEngine) MatchEdgesBetween(startID, endID NodeID, edgeType string, properties []string, match func(*Edge) bool) ([]*Edge, error) {
	edges, err := MatchEdgesBetween(n.inner, n.PrefixNodeID(startID), n.PrefixNodeID(endID), edgeType, properties, func(edge *Edge) bool {
		if !n.hasEdgePrefix(edge.ID) {
			return false
		}
		// The candidate is read-only: show this namespace's IDs on a shallow copy.
		view := *edge
		view.ID = EdgeID(strings.TrimPrefix(string(edge.ID), n.scope))
		view.StartNode, view.EndNode = startID, endID
		return match(&view)
	})
	if err != nil {
		return nil, err
	}
	for i, edge := range edges {
		edges[i] = n.toUserEdge(edge)
	}
	return edges, nil
}

// MatchEdgesBetween delegates to the underlying engine.
func (w *WALEngine) MatchEdgesBetween(startID, endID NodeID, edgeType string, properties []string, match func(*Edge) bool) ([]*Edge, error) {
	return MatchEdgesBetween(w.engine, startID, endID, edgeType, properties, match)
}

// MatchEdgesBetween matches the edges of every readable constituent, each
// once, as GetEdgesBetween reads them.
func (c *CompositeEngine) MatchEdgesBetween(startID, endID NodeID, edgeType string, properties []string, match func(*Edge) bool) ([]*Edge, error) {
	seen := make(map[EdgeID]struct{})
	var matched []*Edge
	for _, alias := range c.getConstituentsForRead() {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}
		edges, err := MatchEdgesBetween(engine, startID, endID, edgeType, properties, func(edge *Edge) bool {
			if _, dup := seen[edge.ID]; dup {
				return false
			}
			return match(edge)
		})
		if err != nil {
			return nil, localizedError(localization.StorageCompositeConstituentQueryFailed(alias, err), err)
		}
		for _, edge := range edges {
			if _, dup := seen[edge.ID]; dup {
				continue
			}
			seen[edge.ID] = struct{}{}
			matched = append(matched, edge)
		}
	}
	return matched, nil
}
