package storage

import (
	"context"
	"slices"
	"sort"
	"strings"
	"time"
)

type pendingTemporalScope struct {
	namespace string
	edgeType  string
	start     string
	end       string
}

type pendingTemporalEdge struct {
	edge      *Edge
	interval  temporalInterval
	maxEnd    time.Time
	unbounded bool
}

type pendingTemporalEdges []pendingTemporalEdge

// overlapping returns a candidate range that contains every interval that can
// overlap query. The caller still checks the exact half-open bounds and key.
func (edges pendingTemporalEdges) overlapping(query temporalInterval) pendingTemporalEdges {
	last := len(edges)
	if query.hasEnd {
		last = sort.Search(last, func(i int) bool {
			start := edges[i].interval.start
			return !start.IsZero() && !start.Before(query.end)
		})
	}
	first := 0
	if !query.start.IsZero() {
		first = sort.Search(last, func(i int) bool {
			return edges[i].unbounded || query.start.Before(edges[i].maxEnd)
		})
	}
	return edges[first:last]
}

// pendingTemporalEdges builds a start-ordered view once per type and pair of
// bound properties in a validation pass. Prefix maxima let a check avoid
// parsing and comparing every pending relationship for each candidate.
func (r *temporalValidationRead) pendingTemporalEdges(ctx context.Context, tx *BadgerTransaction, scope pendingTemporalScope) (pendingTemporalEdges, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if index, ok := r.pending[scope]; ok {
		return index, nil
	}

	var index pendingTemporalEdges
	var cancel bulkReadCancellation
	prefix := scope.namespace + ":"
	for id, edge := range tx.pendingEdges {
		if err := cancel.check(ctx); err != nil {
			return nil, err
		}
		if edge.Type != scope.edgeType || (scope.namespace != "" && !strings.HasPrefix(string(id), prefix)) {
			continue
		}
		start, end, hasEnd, ok := ReadTemporalBounds(edge.Properties[scope.start], edge.Properties[scope.end])
		if ok {
			index = append(index, pendingTemporalEdge{
				edge: edge, interval: temporalInterval{start: start, end: end, hasEnd: hasEnd},
			})
		}
	}

	slices.SortFunc(index, func(a, b pendingTemporalEdge) int {
		aStart, bStart := a.interval.start, b.interval.start
		if aStart.IsZero() != bStart.IsZero() {
			if aStart.IsZero() {
				return -1
			}
			return 1
		}
		return aStart.Compare(bStart)
	})
	var maxEnd time.Time
	var unbounded bool
	for i := range index {
		interval := index[i].interval
		if !interval.hasEnd {
			unbounded = true
		} else if i == 0 || maxEnd.Before(interval.end) {
			maxEnd = interval.end
		}
		index[i].maxEnd, index[i].unbounded = maxEnd, unbounded
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.pending == nil {
		r.pending = make(map[pendingTemporalScope]pendingTemporalEdges)
	}
	r.pending[scope] = index
	return index, nil
}
