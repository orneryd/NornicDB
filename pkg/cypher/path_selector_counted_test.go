package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestSelectedPathCountedCompilation(t *testing.T) {
	for _, bound := range []int{100000, 1000000} {
		t.Run(fmt.Sprint(bound), func(t *testing.T) {
			pattern := fmt.Sprintf("(a)(()-[:T]->()){1,%d}(b)", bound)
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			plan, _, ok := buildSelectedPathPlan(pattern, "p", "", nil)
			runtime.ReadMemStats(&after)
			require.True(t, ok)
			require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(128<<10))
			require.Len(t, plan.forward.counters, 1)
			require.Less(t, len(plan.forward.slots), 10)
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "counted"))
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			result, err := e.Execute(ctx, "MATCH p = ANY SHORTEST "+pattern+" RETURN count(p)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
			canceled, stop := context.WithCancel(context.Background())
			stop()
			p, _, ok := compileSelectedPathPlanContext(canceled, pattern, "p", "", nil)
			require.False(t, ok)
			require.Nil(t, p)
		})
	}
}
func TestSelectedPathPlanByteLimit(t *testing.T) {
	pattern := "(a)-[:T]->(b)"
	predicate := "a.text = '" + strings.Repeat("x", selectedPathPlanCacheEntryBytes) + "'"
	plan, filters, ok := compileSelectedPathPlan(pattern, "p", predicate, nil)
	require.True(t, ok)
	require.Greater(t, selectedPathPlanBytes(plan, filters, len(pattern)), selectedPathPlanCacheEntryBytes)
	key := pattern + "\x00p\x00" + predicate + "\x00"
	_, cached := selectedPathPlans.get(key)
	require.False(t, cached)
}
func TestSelectedPathCounterBounds(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "counterbounds"))
	ctx := context.Background()
	_, err := e.Execute(ctx, "CREATE (:S {id:1})-[:T]->(:S {id:2})-[:T]->(:S {id:3})-[:T]->(:S {id:4})", nil)
	require.NoError(t, err)
	for _, test := range []struct {
		pattern string
		want    int64
	}{
		{"(()-[:T]->()){2,3}", 2}, {"(()-[:T]->()){3,3}", 1}, {"(()-[:T]->()){4,1000000}", 0},
		{"(()-[:T]->()){0,1}", 2}, {"(()-[:T]->()){2,}", 2}, {"(()-[:T]->()){1,2}(()-[:T]->()){1,2}", 3},
	} {
		t.Run(test.pattern, func(t *testing.T) {
			r, err := e.Execute(ctx, "MATCH (a:S {id:1}) MATCH p = ALL SHORTEST (a)"+test.pattern+"(b) RETURN count(p)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{test.want}}, r.Rows)
		})
	}
}
