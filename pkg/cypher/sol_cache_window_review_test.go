package cypher

import (
	"runtime"
	"testing"
	"time"
	"weak"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type ownershipTestResultCache interface {
	Put(string, map[string]interface{}, *ExecuteResult, time.Duration)
	Get(string, map[string]interface{}) (*ExecuteResult, bool)
}

func TestSolCachedResultWindowReleasesHiddenRows(t *testing.T) {
	for name, cache := range map[string]ownershipTestResultCache{"smart": NewSmartQueryCache(2), "plain": NewQueryCache(2)} {
		t.Run(name, func(t *testing.T) {
			ref := func() weak.Pointer[storage.Node] {
				node := &storage.Node{ID: "retired-row", Properties: map[string]any{"blob": make([]byte, 8<<20)}}
				ref := weak.Make(node)
				result := &ExecuteResult{Columns: []string{"n"}, Rows: [][]any{{"visible"}, {node}}}
				// Execute's maxResults boundary truncates before cache publication.
				result.Rows = result.Rows[:1]
				cache.Put("RETURN 1", nil, result, time.Hour)
				got, found := cache.Get("RETURN 1", nil)
				if !found || got == nil || len(got.Rows) != 1 || got.Rows[0][0] != "visible" {
					t.Fatalf("cached row changed: %v, %t", got, found)
				}
				return ref
			}()
			runtime.GC()
			if ref.Value() != nil {
				t.Error("cache retains hidden result row after maxResults truncation")
			}
			runtime.KeepAlive(cache)
		})
	}
}

func TestCachedResultWindowPreservesCaller(t *testing.T) {
	for name, cache := range map[string]ownershipTestResultCache{"smart": NewSmartQueryCache(2), "plain": NewQueryCache(2)} {
		t.Run(name, func(t *testing.T) {
			rows := [][]any{{"visible"}, {&storage.Node{ID: "caller-owned"}}}
			result := &ExecuteResult{Columns: []string{"n"}, Rows: rows[:1]}
			cache.Put("RETURN 1", nil, result, time.Hour)
			got, found := cache.Get("RETURN 1", nil)
			if !found || got == nil || len(got.Rows) != 1 || got.Rows[0][0] != "visible" {
				t.Fatalf("cached row changed: %v, %t", got, found)
			}
			if rows[1][0].(*storage.Node).ID != "caller-owned" {
				t.Fatal("publication cleared a caller-owned hidden row")
			}
			result.Rows = nil
			rows[0] = []any{"replaced"}
			if len(got.Rows) != 1 || got.Rows[0][0] != "visible" {
				t.Fatal("caller changed the cached row window after publication")
			}
		})
	}
}
