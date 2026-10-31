// NOR-160 lane (Claude), 30 October 2026: retained query results are caller-owned.
package cypher

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture/readownership"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func nor160RetainedResult() *ExecuteResult {
	return &ExecuteResult{
		Columns: []string{"n", "m"},
		Rows: [][]any{{
			&storage.Node{ID: "n1", Labels: []string{"Held"}, Properties: map[string]any{
				"payload": map[string]any{"value": "original"},
				"list":    []any{"a", []any{"b"}},
			}},
			map[string]any{"k": []any{"v", map[string]any{"deep": "x"}}},
		}},
		Stats:    &QueryStats{NodesCreated: 1},
		Metadata: map[string]any{"plan": map[string]any{"x": "y"}},
	}
}

// Every query-result cache hit is a caller copy: mutating a hit, or the
// result that was put, never changes a later hit.
func TestNOR160RetentionQueryResultCaches(t *testing.T) {
	type cache struct {
		put func(*ExecuteResult)
		get func() (*ExecuteResult, bool)
	}
	caches := map[string]func() cache{
		"QueryCache": func() cache {
			qc := NewQueryCache(8)
			return cache{
				put: func(r *ExecuteResult) { qc.Put("MATCH (n) RETURN n", nil, r, time.Hour) },
				get: func() (*ExecuteResult, bool) { return qc.Get("MATCH (n) RETURN n", nil) },
			}
		},
		"SmartQueryCache": func() cache {
			sc := NewSmartQueryCache(8)
			return cache{
				put: func(r *ExecuteResult) { sc.Put("MATCH (n) RETURN n", nil, r, time.Hour) },
				get: func() (*ExecuteResult, bool) { return sc.Get("MATCH (n) RETURN n", nil) },
			}
		},
	}
	for name, newCache := range caches {
		t.Run(name, func(t *testing.T) {
			c := newCache()
			input := nor160RetainedResult()
			c.put(input)
			want := readownership.Capture(nor160RetainedResult())
			readownership.Mutate(input)
			for pass := 0; pass < 3; pass++ {
				hit, ok := c.get()
				require.True(t, ok)
				require.Equal(t, want, readownership.Capture(hit), "pass %d", pass)
				readownership.Mutate(hit)
			}
		})
	}
}
