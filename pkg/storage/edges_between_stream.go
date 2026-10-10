package storage

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// EdgesBetweenMatcher finds the edges of one type from a start node to an
// end node that satisfy a predicate over some of their properties: a
// relationship MERGE with bound endpoints compares every candidate's
// identity properties, and Neo4j's Expand(Into) reads just those.
//
// match sees each candidate with its ID, Type, StartNode, EndNode and at
// least the listed properties it has; the candidate is valid only during the
// call, and match must not change or keep it. The edges it accepts are
// returned whole, as GetEdgesBetween returns them, from the same read.
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
		if match(projectEdgeProperties(edge, properties)) {
			matched = append(matched, edge)
		}
	}
	return matched
}

// projectEdgeProperties returns a copy of edge's identity with only the
// listed properties (all of them when properties is nil).
func projectEdgeProperties(edge *Edge, properties []string) *Edge {
	if properties == nil {
		return copyEdge(edge)
	}
	projected := &Edge{
		ID:                   edge.ID,
		Type:                 edge.Type,
		StartNode:            edge.StartNode,
		EndNode:              edge.EndNode,
		VisibilitySuppressed: edge.VisibilitySuppressed,
	}
	for _, key := range properties {
		if value, ok := edge.Properties[key]; ok {
			if projected.Properties == nil {
				projected.Properties = make(map[string]any, len(properties))
			}
			projected.Properties[key] = value
		}
	}
	return projected
}

// MatchEdgesBetween reads the typed edge-between set index, matches each
// candidate from the edge cache or, on a miss, from its decoded header and
// only the requested properties, and returns the accepted ones whole. A pair with no set-index entry at all may
// predate the index: its legacy outgoing index is read as GetEdgesBetween
// reads it.
func (b *BadgerEngine) MatchEdgesBetween(startID, endID NodeID, edgeType string, properties []string, match func(*Edge) bool) ([]*Edge, error) {
	if startID == "" || endID == "" {
		return nil, ErrInvalidID
	}
	if match == nil {
		return nil, ErrInvalidData
	}
	if err := b.ensureOpen(); err != nil {
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
	err := b.withView(func(txn *badger.Txn) error {
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
			// match it without reading or decoding (the cache is read-only;
			// a match is returned as a copy).
			if cached, hit := b.cacheLoadEdge(edgeID); hit {
				if cached.StartNode != startID || cached.EndNode != endID || cached.Type != edgeType ||
					b.filterEdgeByDecay(cached, nowNanos) || !match(cached) {
					continue
				}
				matched = append(matched, copyEdge(cached))
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
				candidate, err := b.projectedEdge(data, edgeID, startNum, endNum, include)
				if err != nil {
					return err
				}
				if candidate == nil {
					return nil
				}
				candidate.StartNode, candidate.EndNode = startID, endID
				if candidate.Type != edgeType || b.filterEdgeByDecay(candidate, nowNanos) || !match(candidate) {
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
	legacy, err := b.edgesBetweenFromLegacyOutgoingIndex(startID, endID, edgeType)
	if err != nil {
		return nil, err
	}
	if len(legacy) > 0 {
		_ = b.selfHealEdgeBetweenIndexes(legacy)
	}
	return filterEdgesBetween(legacy, startID, endID, edgeType, properties, match), nil
}

// projectedEdge decodes an edge body's header and the properties in include
// (all of them when include is nil). It returns nil for an edge whose
// endpoints aren't startNum / endNum.
func (b *BadgerEngine) projectedEdge(data []byte, edgeID EdgeID, startNum, endNum uint64, include map[string]struct{}) (*Edge, error) {
	edge, gotStart, gotEnd, offset, err := decodeEdgeCompactHeader(data, edgeFormatCompactV2)
	if err != nil {
		return nil, err
	}
	if gotStart != startNum || gotEnd != endNum {
		return nil, nil
	}
	edge.ID = edgeID
	if offset < len(data) && (include == nil || len(include) > 0) {
		props, err := b.decodeTokenizedPropertiesProjected(namespaceForEdgeID(edgeID), data[offset:], include)
		if err != nil {
			return nil, fmt.Errorf("compact edge v2: %w", err)
		}
		edge.Properties = props
	}
	return edge, nil
}

// MatchEdgesBetween matches the engine's edges as this transaction sees
// them: its deleted edges are gone, and its created or updated ones replace
// the stored versions, as in GetEdgesBetween.
func (tx *BadgerTransaction) MatchEdgesBetween(startID, endID NodeID, edgeType string, properties []string, match func(*Edge) bool) ([]*Edge, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
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
	for _, edge := range tx.pendingEdges {
		if edge == nil || edge.StartNode != startID || edge.EndNode != endID || edge.Type != edgeType {
			continue
		}
		if match(projectEdgeProperties(edge, properties)) {
			matched = append(matched, copyEdge(edge))
		}
	}
	return matched, nil
}

// MatchEdgesBetween matches the engine's edges with this namespace's IDs.
func (n *NamespacedEngine) MatchEdgesBetween(startID, endID NodeID, edgeType string, properties []string, match func(*Edge) bool) ([]*Edge, error) {
	var candidate Edge
	edges, err := MatchEdgesBetween(n.inner, n.prefixNodeID(startID), n.prefixNodeID(endID), edgeType, properties, func(edge *Edge) bool {
		if !n.hasEdgePrefix(edge.ID) {
			return false
		}
		candidate = *edge
		candidate.ID = n.unprefixEdgeID(edge.ID)
		candidate.StartNode, candidate.EndNode = startID, endID
		return match(&candidate)
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
