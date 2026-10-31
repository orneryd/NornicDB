// NOR-160 lane (Claude), 30 October 2026: cached relationship hits are caller-owned.
package cypher

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func nor160RelationshipHitResult() *ExecuteResult {
	edge := func(id storage.EdgeID) *storage.Edge {
		return &storage.Edge{ID: id, Type: "T", StartNode: "test:a", EndNode: "test:b",
			Properties: map[string]any{"payload": map[string]any{"value": "original"}}}
	}
	passage := search.SearchResult{ID: "test:p", Relationship: edge("test:p")}
	hit := search.SearchResult{ID: "test:e", Relationship: edge("test:e"), Passages: []search.SearchPassage{passage}}
	return &ExecuteResult{Columns: []string{"hit"}, Rows: [][]any{{hit}}}
}

// A relationship search hit in a cached row: changing a hit, or the result
// that was put, never changes a later hit (both directions).
func TestNOR160RetentionRelationshipHitCaches(t *testing.T) {
	type cache struct {
		put func(*ExecuteResult)
		get func() (*ExecuteResult, bool)
	}
	caches := map[string]func() cache{
		"QueryCache": func() cache {
			qc := NewQueryCache(2)
			return cache{
				put: func(r *ExecuteResult) { qc.Put("relationship-hit", nil, r, time.Hour) },
				get: func() (*ExecuteResult, bool) { return qc.Get("relationship-hit", nil) },
			}
		},
		"SmartQueryCache": func() cache {
			sc := NewSmartQueryCache(2)
			return cache{
				put: func(r *ExecuteResult) { sc.Put("relationship-hit", nil, r, time.Hour) },
				get: func() (*ExecuteResult, bool) { return sc.Get("relationship-hit", nil) },
			}
		},
	}
	for name, newCache := range caches {
		t.Run(name, func(t *testing.T) {
			c := newCache()
			input := nor160RelationshipHitResult()
			c.put(input)
			want := readownership.Capture(nor160RelationshipHitResult())
			// Store side: the caller that put the result changes it afterwards.
			input.Rows[0][0].(search.SearchResult).Relationship.Properties["payload"].(map[string]any)["value"] = "caller"
			readownership.Mutate(input)
			for pass := 0; pass < 3; pass++ {
				hit, ok := c.get()
				require.True(t, ok)
				require.Equal(t, want, readownership.Capture(hit), "pass %d", pass)
				// Caller side: a hit's relationship and its passage's.
				cell := hit.Rows[0][0].(search.SearchResult)
				cell.Relationship.Properties["payload"].(map[string]any)["value"] = "caller"
				cell.Passages[0].Relationship.Properties["payload"].(map[string]any)["value"] = "caller"
				readownership.Mutate(hit)
			}
		})
	}
}

// The result cache's copier leaves no exported reference shared, for rows
// carrying every graph-bearing cell kind.
func TestNOR160CopierCompleteExecuteResult(t *testing.T) {
	result := nor160RetainedResult()
	hit := nor160RelationshipHitResult().Rows[0][0]
	edge := &storage.Edge{ID: "test:e", Type: "T", Properties: map[string]any{"payload": map[string]any{"value": "original"}}}
	result.Rows[0] = append(result.Rows[0], hit, edge)
	result.Administration = &AdministrationOutcome{CreatedDatabase: "db"}
	result.MapKeyOrders = map[uintptr][]string{1: {"k"}}
	require.Empty(t, readownership.Unpopulated(result))
	require.Empty(t, readownership.SharedReferences(result, callerOwnedResult(result)))
}

// A pointer to a search hit in a cached row comes back as a pointer to the
// caller's own hit (one Copier dispatch rule, type preserved).
func TestNOR160RetentionHitPointerKeepsType(t *testing.T) {
	hit := nor160RelationshipHitResult().Rows[0][0].(search.SearchResult)
	cache := NewQueryCache(2)
	cache.Put("pointer-hit", nil, &ExecuteResult{Columns: []string{"hit"}, Rows: [][]any{{&hit}}}, time.Hour)
	for pass := 0; pass < 2; pass++ {
		got, ok := cache.Get("pointer-hit", nil)
		require.True(t, ok)
		cell, ok := got.Rows[0][0].(*search.SearchResult)
		require.True(t, ok, "the cached cell changed type to %T", got.Rows[0][0])
		require.NotSame(t, &hit, cell)
		require.Equal(t, "original", cell.Relationship.Properties["payload"].(map[string]any)["value"], "pass %d", pass)
		cell.Relationship.Properties["payload"].(map[string]any)["value"] = "caller"
	}
	require.Equal(t, "original", hit.Relationship.Properties["payload"].(map[string]any)["value"])
}
