package cypher

import (
	"container/heap"
	"context"
	"errors"
	"github.com/orneryd/nornicdb/internal/ctxsort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) indexedOrderRequiresNonNull(ctx context.Context, variable, property, where string) bool {
	for _, term := range splitTopLevelAndConjuncts(ctx, unwrapOuterParens(strings.TrimSpace(where))) {
		required, ok := e.parseSimpleIndexedIsNotNull(ctx, variable, unwrapOuterParens(strings.TrimSpace(term)))
		if ok && strings.EqualFold(required, property) {
			return true
		}
	}
	return false
}

// indexedOrderHeap retains only the requested window; its root is the worst
// candidate under the complete ORDER BY comparator.
type indexedOrderHeap struct {
	nodes   []*storage.Node
	compare func(*storage.Node, *storage.Node) int
}

func (h indexedOrderHeap) Len() int                { return len(h.nodes) }
func (h indexedOrderHeap) Less(i, j int) bool      { return h.compare(h.nodes[i], h.nodes[j]) > 0 }
func (h indexedOrderHeap) Swap(i, j int)           { h.nodes[i], h.nodes[j] = h.nodes[j], h.nodes[i] }
func (h *indexedOrderHeap) Push(value interface{}) { h.nodes = append(h.nodes, value.(*storage.Node)) }
func (h *indexedOrderHeap) Pop() interface{} {
	last := len(h.nodes) - 1
	value := h.nodes[last]
	h.nodes[last] = nil
	h.nodes = h.nodes[:last]
	return value
}

// collectIndexedOrderWindow applies filtering and all sort keys before
// truncation. A primary-key group may be much larger than the requested page;
// only the best limit nodes are retained while every boundary tie is examined.
// bounds (impliedPropertyBounds) skips the primary-key groups where can't
// match.
func (e *StorageExecutor) collectIndexedOrderWindow(ctx context.Context, pattern nodePatternInfo, where string, specs []nodeOrderSpec, label string, limit int, bounds storage.PropertyIndexBounds) ([]*storage.Node, bool, error) {
	schema := orderedPropertyIndexSchema(e.storage)
	if schema == nil {
		return nil, false, nil
	}
	compareWithinGroup := func(a, b *storage.Node) int {
		if cmp := e.compareNodeOrderSpecs(a, b, specs); cmp != 0 {
			return cmp
		}
		return strings.Compare(string(a.ID), string(b.ID))
	}
	nodes := make([]*storage.Node, 0, limit)
	// The window keeps only a page of the ordered index. Internal system
	// nodes (labels starting with "_") are not MATCH rows, so they are
	// skipped before the page is filled: a page that counted them would
	// return fewer visible rows than the LIMIT asks for, and the rows the
	// query does show would be the wrong ones.
	hideSystemNodes := shouldHideSystemNodes(e.getStorage(ctx))
	var filter func(*storage.Node) bool
	if strings.TrimSpace(where) != "" {
		filter = e.compileNodeWhereFilter(ctx, pattern.variable, where)
	}
	var visitErr error
	visited := false
	found, err := schema.VisitPropertyIndexGroupsInRange(ctx, label, specs[0].propName, specs[0].descending, bounds, func(ids []storage.NodeID) bool {
		visited = true
		remaining := limit - len(nodes)
		groupTop := &indexedOrderHeap{
			nodes:   make([]*storage.Node, 0, min(remaining, len(ids))),
			compare: compareWithinGroup,
		}
		for _, id := range ids {
			if err := ctx.Err(); err != nil {
				visitErr = err
				return false
			}
			node, err := e.getQueryNode(ctx, e.storage, id)
			if err != nil || node == nil {
				continue
			}
			if hideSystemNodes && isSystemNode(node) {
				continue
			}
			if (len(pattern.labels) > 0 && !mergeNodeHasLabels(node, pattern.labels)) || !e.nodeMatchesProps(node, pattern.properties) {
				continue
			}
			if filter != nil && !filter(node) {
				continue
			}
			if groupTop.Len() < remaining {
				heap.Push(groupTop, node)
			} else if compareWithinGroup(node, groupTop.nodes[0]) < 0 {
				groupTop.nodes[0] = node
				heap.Fix(groupTop, 0)
			}
		}
		if err := ctxsort.Sort(ctx, groupTop.nodes, compareWithinGroup, false); err != nil {
			visitErr = err
			return false
		}
		if err := ctx.Err(); err != nil {
			visitErr = err
			return false
		}
		nodes = append(nodes, groupTop.nodes...)
		if len(nodes) == limit {
			return false
		}
		return true
	})
	if errors.Is(err, storage.ErrPropertyIndexValueOrderUnavailable) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if visitErr != nil {
		return nil, false, visitErr
	}
	if !found || !visited {
		return nil, false, nil
	}
	return nodes, true, nil
}
