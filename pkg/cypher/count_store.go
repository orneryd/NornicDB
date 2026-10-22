package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Counts answered from the store's counters: the one owner of the shapes
// Neo4j answers from its count store (NodeCountFromCountStore,
// RelationshipCountFromCountStore). count(*) or count(<variable>) over
//   - one node pattern with one label and no property map, or
//   - one fixed-length relationship pattern with no property map, at most
//     one labelled endpoint (one label), a type when an endpoint is labelled,
//     and no repeated endpoint variable.
//
// The routes that recognise a plain count (the pipeline's simple read plans,
// the traversal route) check only what their parse knows (no WHERE, no path
// variable, one RETURN item) and call nodeCountFromStore or
// relationshipCountFromStore; ok=false sends the statement to the general
// path (#1076).

// countStoreDescribesReads reports whether the counters count what a read
// sees: not with decay filtering, which hides entities the counters include,
// nor in a temporal viewport, which reads another version.
func countStoreDescribesReads(ctx context.Context, store storage.Engine) bool {
	if storageHasDecayFiltering(store) {
		return false
	}
	if viewport, ok := TemporalViewportFromContext(ctx); ok && viewport.Enabled() {
		return false
	}
	return true
}

// countsRows reports whether item is count(*) or count(<one of variables>),
// which count the pattern's rows: none of the variables is null in a MATCH.
func countsRows(item returnItem, variables ...string) bool {
	if !isAggregateFuncName(item.expr, "count") {
		return false
	}
	inner := strings.TrimSpace(extractFuncInner(item.expr))
	if inner == "*" {
		return true
	}
	for _, variable := range variables {
		if variable != "" && inner == variable {
			return true
		}
	}
	return false
}

// nodeCountFromStore answers count over a one-label node pattern from the
// label counter.
func (e *StorageExecutor) nodeCountFromStore(ctx context.Context, node nodePatternInfo, item returnItem) (int64, bool, error) {
	if len(node.labels) != 1 || len(node.properties) > 0 || !countsRows(item, node.variable) {
		return 0, false, nil
	}
	store := e.getStorage(ctx)
	if !countStoreDescribesReads(ctx, store) {
		return 0, false, nil
	}
	counter, ok := storage.ReadCapabilityForEngine[interface{ NodeCountByLabel(string) (int64, error) }](store)
	if !ok {
		return 0, false, nil
	}
	count, err := counter.NodeCountByLabel(node.labels[0])
	return count, true, err
}

// relationshipCountFromStore answers count over a single relationship
// pattern from the relationship counters: the total, the per-type counters,
// or, with one labelled endpoint, the positional (label, type) counters.
func (e *StorageExecutor) relationshipCountFromStore(ctx context.Context, matches *TraversalMatch, item returnItem) (int64, bool, error) {
	if matches == nil || matches.IsChained || matches.PathVariable != "" ||
		matches.Relationship.MinHops != 1 || matches.Relationship.MaxHops != 1 ||
		len(matches.Relationship.Properties) > 0 ||
		len(matches.StartNode.properties) > 0 || len(matches.EndNode.properties) > 0 ||
		!countsRows(item, matches.Relationship.Variable) {
		return 0, false, nil
	}
	// A repeated endpoint variable requires self-loops only, which the
	// counters can't express.
	if matches.StartNode.variable != "" && matches.StartNode.variable == matches.EndNode.variable {
		return 0, false, nil
	}
	start, end := matches.StartNode.labels, matches.EndNode.labels
	// A counter holds one label: two labelled endpoints, or an endpoint with
	// two labels, would count the relationships of one label whatever the
	// other (#1076).
	if len(start) > 1 || len(end) > 1 || (len(start) > 0 && len(end) > 0) {
		return 0, false, nil
	}
	store := e.getStorage(ctx)
	if !countStoreDescribesReads(ctx, store) {
		return 0, false, nil
	}
	types := matches.Relationship.Types
	direction := matches.Relationship.Direction

	if len(start) > 0 || len(end) > 0 {
		if direction == "both" || len(types) == 0 {
			return 0, false, nil
		}
		// The labelled endpoint is the relationship's start node when it is
		// the source of an outgoing pattern or the target of an incoming one.
		label, startTier := "", false
		switch {
		case len(start) == 1:
			label, startTier = start[0], direction == "outgoing"
		default:
			label, startTier = end[0], direction == "incoming"
		}
		var total int64
		for _, edgeType := range types {
			var n int64
			var err error
			if startTier {
				n, err = store.EdgeCountByStartLabel(label, edgeType)
			} else {
				n, err = store.EdgeCountByEndLabel(label, edgeType)
			}
			if err != nil {
				return 0, true, err
			}
			total += n
		}
		return total, true, nil
	}

	if direction == "both" {
		return countUndirectedRelationshipMatches(store, types)
	}
	if len(types) == 0 {
		n, err := store.EdgeCount()
		return n, true, err
	}
	// Every engine keeps a per-type counter (#638): typed counts are point
	// reads.
	var total int64
	for _, edgeType := range types {
		n, err := store.EdgeCountByType(edgeType)
		if err != nil {
			return 0, true, err
		}
		total += n
	}
	return total, true, nil
}

// countUndirectedRelationshipMatches is an undirected pattern's cardinality,
// not the number of relationships: a relationship matches once from each
// endpoint, a self-loop once.
func countUndirectedRelationshipMatches(store storage.Engine, types []string) (int64, bool, error) {
	var candidates []*storage.Edge
	if len(types) == 0 {
		edges, err := store.AllEdges()
		if err != nil {
			return 0, true, err
		}
		candidates = edges
	} else {
		seen := make(map[storage.EdgeID]struct{})
		for _, relationshipType := range types {
			edges, err := store.GetEdgesByType(relationshipType)
			if err != nil {
				return 0, true, err
			}
			for _, edge := range edges {
				if edge == nil {
					continue
				}
				if _, exists := seen[edge.ID]; exists {
					continue
				}
				seen[edge.ID] = struct{}{}
				candidates = append(candidates, edge)
			}
		}
	}
	var count int64
	for _, edge := range candidates {
		if edge == nil {
			continue
		}
		count++
		if edge.StartNode != edge.EndNode {
			count++
		}
	}
	return count, true, nil
}
