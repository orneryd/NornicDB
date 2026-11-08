package cypher

import (
	"context"
	"errors"
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

// countStoreVisibilityPrefix reports the label prefix of the nodes this read
// hides, and whether the counters can answer the read at all: not with decay
// filtering, which hides entities the counters include, nor in a temporal
// viewport, which reads another version. The system database hides nothing, so
// its counters count what it reads, internal labels included. Everything else
// leaves the answer to the store's count-store read, which decides from one
// snapshot whether its counters describe the rows.
func countStoreVisibilityPrefix(ctx context.Context, store storage.Engine) (string, bool) {
	if storageHasDecayFiltering(store) {
		return "", false
	}
	if viewport, ok := TemporalViewportFromContext(ctx); ok && viewport.Enabled() {
		return "", false
	}
	if !shouldHideSystemNodes(store) {
		return "", true
	}
	return internalLabelPrefix, true
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

// countStoreRead answers one count-store request through the store: the counter
// value and its visibility proof come from the store's one consistent read, so
// a commit that lands while the read runs cannot separate them. answered is
// false when the counters do not describe the read's rows, which leaves the
// count to those rows.
func countStoreRead(ctx context.Context, store storage.Engine, request storage.CountStoreReadRequest) (int64, bool, error) {
	reader, ok := store.(storage.CountStoreReadEngine)
	if !ok {
		// An unproven store is not a described one: the rows answer.
		return 0, false, nil
	}
	answer, err := reader.ReadCountStore(ctx, request)
	if err != nil {
		return 0, true, err
	}
	if !answer.DescribesRows {
		return 0, false, nil
	}
	return answer.Count, true, nil
}

// nodeCountFromStore answers count over a one-label node pattern from the
// label counter.
func (e *StorageExecutor) nodeCountFromStore(ctx context.Context, node nodePatternInfo, item returnItem) (int64, bool, error) {
	if len(node.labels) != 1 || len(node.properties) > 0 || !countsRows(item, node.variable) {
		return 0, false, nil
	}
	store := e.getStorage(ctx)
	// A label that is itself internal names exactly the nodes a MATCH hides,
	// so the read counts none of them: the visibility decision answers it
	// without a store read, which the label counter cannot (it counts the
	// hidden nodes carrying the label).
	if shouldHideSystemNodes(store) && isInternalLabel(node.labels[0]) {
		return 0, true, nil
	}
	hiddenPrefix, eligible := countStoreVisibilityPrefix(ctx, store)
	if !eligible {
		return 0, false, nil
	}
	return countStoreRead(ctx, store, storage.CountStoreReadRequest{
		Kind:              storage.CountStoreNodesByLabel,
		Name:              node.labels[0],
		HiddenLabelPrefix: hiddenPrefix,
	})
}

// relationshipCountFromStore answers count over a single relationship
// pattern from the relationship counters: the per-type counters, or, with one
// labelled endpoint, the positional (label, type) counters.
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
	types := matches.Relationship.Types
	direction := matches.Relationship.Direction
	// A relationship row exists only while both its endpoints do, and an
	// internal label names exactly the endpoints a MATCH hides: a pattern with
	// such an endpoint keeps no row, whatever the counters hold, so the
	// visibility decision answers it without a store read.
	if shouldHideSystemNodes(store) {
		if len(start) == 1 && isInternalLabel(start[0]) {
			return 0, true, nil
		}
		if len(end) == 1 && isInternalLabel(end[0]) {
			return 0, true, nil
		}
	}
	hiddenPrefix, eligible := countStoreVisibilityPrefix(ctx, store)
	if !eligible {
		return 0, false, nil
	}

	if len(start) > 0 || len(end) > 0 {
		// The labelled endpoint is the relationship's start node when it is
		// the source of an outgoing pattern or the target of an incoming one.
		label := ""
		kind := storage.CountStoreRelationshipsByEndLabel
		switch {
		case len(start) == 1:
			label = start[0]
			if direction == "outgoing" {
				kind = storage.CountStoreRelationshipsByStartLabel
			}
		default:
			label = end[0]
			if direction == "incoming" {
				kind = storage.CountStoreRelationshipsByStartLabel
			}
		}
		if direction == "both" || len(types) == 0 {
			return 0, false, nil
		}
		return countStoreRead(ctx, store, storage.CountStoreReadRequest{
			Kind:              kind,
			Name:              label,
			Types:             types,
			HiddenLabelPrefix: hiddenPrefix,
		})
	}

	if direction == "both" {
		return e.countUndirectedRelationshipMatches(ctx, store, types)
	}
	// Every relationship type the scope holds is one request, so a multi-type
	// or untyped count is one snapshot too.
	return countStoreRead(ctx, store, storage.CountStoreReadRequest{
		Kind:              storage.CountStoreRelationshipsByType,
		Types:             types,
		HiddenLabelPrefix: hiddenPrefix,
	})
}

// countUndirectedRelationshipMatches is an undirected pattern's cardinality,
// not the number of relationships: a relationship matches once from each
// endpoint, a self-loop once.
func (e *StorageExecutor) countUndirectedRelationshipMatches(ctx context.Context, store storage.Engine, types []string) (int64, bool, error) {
	var candidates []*storage.Edge
	if len(types) == 0 {
		edges, err := store.AllEdges(ctx)
		if err != nil {
			return 0, true, err
		}
		candidates = edges
	} else {
		seen := make(map[storage.EntityIdentity]struct{})
		for _, relationshipType := range types {
			edges, err := store.GetEdgesByType(ctx, relationshipType)
			if err != nil {
				return 0, true, err
			}
			for _, edge := range edges {
				if edge == nil {
					continue
				}
				if _, exists := seen[edge.Identity()]; exists {
					continue
				}
				seen[edge.Identity()] = struct{}{}
				candidates = append(candidates, edge)
			}
		}
	}
	var count int64
	reader := e.newRelationshipEndpointReader(store)
	for _, edge := range candidates {
		if edge == nil {
			continue
		}
		if _, err := reader.read(ctx, edge.StartNode, nil); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return 0, true, err
		}
		if _, err := reader.read(ctx, edge.EndNode, nil); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return 0, true, err
		}
		count++
		if edge.StartNode != edge.EndNode {
			count++
		}
	}
	return count, true, nil
}
