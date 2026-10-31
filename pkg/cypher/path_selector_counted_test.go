package cypher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSelectedPathCountedCompilation(t *testing.T) {
	const isolated = "NORNIC_COUNTED_COMPILATION_ISOLATED"
	if os.Getenv(isolated) != "1" {
		output, err := runCypherTestProcess(t, "TestSelectedPathCountedCompilation", isolated+"=1")
		require.NoError(t, err, string(output))
		require.Contains(t, string(output), "TestSelectedPathCountedCompilation/100000")
		require.Contains(t, string(output), "TestSelectedPathCountedCompilation/1000000")
		t.Log(string(output))
		return
	}
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
			// Initialize shared parser/runtime state on a DIFFERENT, tiny
			// statement. The large counted statement and its plan stay cold;
			// this budget measures counted compilation, not ANTLR's one-time
			// automaton initialization (which exceeds 8 MiB independently of
			// the counted upper bound).
			prime := "MATCH p = ANY SHORTEST (warm_a)(()-[:T]->()){1,2}(warm_b) RETURN count(p)"
			warmResult, warmErr := e.Execute(context.Background(), prime, nil)
			require.NoError(t, warmErr)
			require.Equal(t, [][]interface{}{{int64(0)}}, warmResult.Rows)
			query := "MATCH p = ANY SHORTEST " + pattern + " RETURN count(p)"
			require.False(t, e.hasCachedValidSyntax(query, syntaxStatementAccepted))
			_, cached := selectedPathPlans.get(pattern + "\x00p\x00\x00")
			require.False(t, cached)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
			defer cancel()
			runtime.ReadMemStats(&before)
			started := time.Now()
			result, err := e.Execute(ctx, query, nil)
			runtime.ReadMemStats(&after)
			require.Less(t, time.Since(started), 100*time.Millisecond)
			require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(8<<20))
			if err != nil {
				require.True(t, errors.Is(err, context.DeadlineExceeded))
			} else {
				require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
			}
			t.Logf("upper=%d compiled bytes<128KiB; empty graph bytes=%d elapsed=%s error=%v", bound, after.TotalAlloc-before.TotalAlloc, time.Since(started), err)
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
