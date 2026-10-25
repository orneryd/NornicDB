package storage_test

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

// TestNodeIndexEntriesConcurrentCounters runs counter updates on nodes that
// exist throughout, read-only seeks on the same keys, and churn (renames,
// deletes, label moves, re-creates) on other nodes of the label, over every
// public route in the default synchronous mode. Every counter update and every
// seek must match its node, and each counter must end at exactly the number
// of increments that reported success.
func TestNodeIndexEntriesConcurrentCounters(t *testing.T) {
	stable := []string{"s0", "s1", "s2"}
	pool := []string{"p0", "p1", "p2", "p3", "p4", "p5"}
	counterForms := []string{
		"MATCH (n:Doc {id:$id}) SET n.c = n.c + 1 RETURN n.id",
		"MATCH (n:Doc {id:$id}) SET n += {c: n.c + 1, note: $note} RETURN n.id",
		"MATCH (n:Doc {id:$id}) SET n = {id: n.id, c: n.c + 1, kind: 'stable'} RETURN n.id",
		"MATCH (n:Doc {id:$id}) SET n:Tagged, n.c = n.c + 1 RETURN n.id",
		"MATCH (n:Doc {id:$id}) REMOVE n:Tagged SET n.c = n.c + 1 RETURN n.id",
		"MERGE (n:Doc {id:$id}) ON MATCH SET n.c = n.c + 1 RETURN n.id",
	}
	churnForms := []string{
		"CREATE (:Doc {id:$p, kind:'churn'})",
		"MERGE (n:Doc {id:$p}) ON CREATE SET n.kind = 'churn' ON MATCH SET n.m = coalesce(n.m, 0) + 1",
		"MATCH (n:Doc {id:$p}) WHERE n.kind = 'churn' SET n.id = $q",
		"MATCH (n:Doc {id:$p}) WHERE n.kind = 'churn' DETACH DELETE n",
		"MATCH (n:Doc {id:$p}) WHERE n.kind = 'churn' REMOVE n:Doc SET n:Parked",
		"MATCH (n:Parked) WITH n LIMIT 1 SET n:Doc REMOVE n:Parked",
		"MATCH (n:Doc {id:$p}) WHERE n.kind = 'churn' SET n = {id: $q, kind: 'churn'}",
		"MATCH (n:Doc {id:$p}) WHERE n.kind = 'churn' SET n.touch = $q",
		"MATCH (n:Doc {id:$p}) WHERE n.kind = 'churn' REMOVE n.id",
	}
	for _, kind := range indexKinds[:2] {
		t.Run(kind.name, func(t *testing.T) {
			env := openIndexEnv(t)
			admin := cypher.NewStorageExecutor(env.store)
			mustRunIndex(t, admin, kind.ddl, nil)
			for _, id := range stable {
				mustRunIndex(t, admin, "CREATE (:Doc {id:$id, c:0, kind:'stable'})", map[string]any{"id": id})
			}
			for _, id := range pool[:3] {
				mustRunIndex(t, admin, "CREATE (:Doc {id:$id, kind:'churn'})", map[string]any{"id": id})
			}

			var mu sync.Mutex
			increments := map[string]int64{}
			tally := map[string]int{}
			var empty []string
			record := func(outcome string, sample string) {
				mu.Lock()
				defer mu.Unlock()
				tally[outcome]++
				if strings.HasPrefix(outcome, "EMPTY") || strings.HasPrefix(outcome, "UNEXPECTED") {
					empty = append(empty, sample)
				}
			}
			var wg sync.WaitGroup
			start := make(chan struct{})
			worker := func(index, iterations int, body func(rng *rand.Rand, exec *cypher.StorageExecutor, route indexRoute, i int)) {
				wg.Add(1)
				go func() {
					defer wg.Done()
					rng := rand.New(rand.NewSource(int64(index+1)*7919 + time.Now().UnixNano()))
					exec := cypher.NewStorageExecutor(env.store)
					<-start
					for i := 0; i < iterations; i++ {
						body(rng, exec, indexRoutes[(index+i)%len(indexRoutes)], i)
					}
				}()
			}
			opCtx := func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 60*time.Second)
			}
			for w := 0; w < 4; w++ {
				worker(w, 60, func(rng *rand.Rand, exec *cypher.StorageExecutor, route indexRoute, i int) {
					id, form := stable[rng.Intn(len(stable))], rng.Intn(len(counterForms))
					ctx, cancel := opCtx()
					defer cancel()
					outcome := env.run(ctx, exec, route, counterForms[form], map[string]any{"id": id, "note": fmt.Sprintf("w%d-%d", w, i)})
					switch {
					case outcome.err == nil && len(outcome.rows) == 1:
						mu.Lock()
						increments[id]++
						mu.Unlock()
						record("counter ok "+route.String(), "")
					case outcome.err == nil:
						record("EMPTY counter", fmt.Sprintf("%s form %d on %s matched %d rows", route, form, id, len(outcome.rows)))
					case strings.HasPrefix(outcome.code, "Neo.TransientError.") || strings.Contains(outcome.code, "ConstraintValidationFailed"):
						// Conflicts roll back without incrementing; the
						// exact-count check below proves it.
						record("counter rejected "+outcome.code, "")
					default:
						record("UNEXPECTED counter", fmt.Sprintf("%s form %d on %s: %s %v", route, form, id, outcome.code, outcome.err))
					}
				})
			}
			for w := 4; w < 8; w++ {
				worker(w, 150, func(rng *rand.Rand, exec *cypher.StorageExecutor, route indexRoute, i int) {
					id := stable[rng.Intn(len(stable))]
					ctx, cancel := opCtx()
					defer cancel()
					outcome := env.run(ctx, exec, route, "MATCH (n:Doc {id:$id}) RETURN n.c", map[string]any{"id": id})
					switch {
					case outcome.err == nil && len(outcome.rows) == 1:
						record("seek ok "+route.String(), "")
					case outcome.err == nil:
						record("EMPTY seek", fmt.Sprintf("%s on %s matched %d rows", route, id, len(outcome.rows)))
					default:
						record("UNEXPECTED seek", fmt.Sprintf("%s on %s: %s %v", route, id, outcome.code, outcome.err))
					}
				})
			}
			for w := 8; w < 10; w++ {
				worker(w, 60, func(rng *rand.Rand, exec *cypher.StorageExecutor, route indexRoute, i int) {
					p, q := pool[rng.Intn(len(pool))], pool[rng.Intn(len(pool))]
					ctx, cancel := opCtx()
					defer cancel()
					outcome := env.run(ctx, exec, route, churnForms[rng.Intn(len(churnForms))], map[string]any{"p": p, "q": q})
					if outcome.err == nil {
						record("churn ok", "")
					} else {
						record("churn rejected "+outcome.code, "")
					}
				})
			}
			began := time.Now()
			close(start)
			wg.Wait()

			outcomes := make([]string, 0, len(tally))
			for outcome := range tally {
				outcomes = append(outcomes, outcome)
			}
			sort.Strings(outcomes)
			for _, outcome := range outcomes {
				t.Logf("%-70s %d", outcome, tally[outcome])
			}
			t.Logf("workload took %s; successful increments %v", time.Since(began), increments)
			require.Empty(t, empty, "a counter update or seek on an existing node matched no rows or failed unexpectedly")
			for _, route := range indexRoutes {
				require.Positive(t, tally["seek ok "+route.String()], "route %s served no seek", route)
				require.Positive(t, tally["counter ok "+route.String()], "route %s applied no increment", route)
			}
			for _, id := range stable {
				rows := mustRunIndex(t, admin, "MATCH (n:Doc {id:$id}) RETURN n.c", map[string]any{"id": id})
				require.Len(t, rows, 1, "exactly one node holds %s", id)
				require.Equal(t, increments[id], indexInt(t, rows[0][0]), "counter %s must equal its successful increments", id)
			}
		})
	}
}
