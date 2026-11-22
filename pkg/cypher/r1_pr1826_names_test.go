package cypher

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// Review lane R1 of PR #1826: a reader in one database names properties a node
// does not have, through ordinary statements; a writer commits in another
// database of the same server.
func TestR1PR1826AbsentNamesThroughCypher(t *testing.T) {
	ctx := context.Background()
	manager, err := multidb.NewDatabaseManager(ctx, newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.NoError(t, manager.CreateDatabase(ctx, multidb.DatabaseCreateRequest{Name: "tenant"}))
	readerStore, err := manager.GetStorage("tenant")
	require.NoError(t, err)
	writerStore, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	reader := NewStorageExecutor(readerStore, QueryCachePolicy{MaxEntries: 0})
	writer := NewStorageExecutor(writerStore, QueryCachePolicy{MaxEntries: 0})
	for _, statement := range []string{"CREATE CONSTRAINT r1_names IF NOT EXISTS FOR (n:Doc) REQUIRE n.key IS UNIQUE", "CREATE (:Doc {key:'one', path:'guide'})"} {
		_, err := reader.Execute(ctx, statement, nil)
		require.NoError(t, err)
	}
	_, err = writer.Execute(ctx, "CREATE (:Counter {id:1, n:0})", nil)
	require.NoError(t, err)
	next := 0
	statement := func(count int) string {
		items := make([]string, count)
		for i := range items {
			items[i] = fmt.Sprintf("n.absent_name_%08d", next)
			next++
		}
		return "MATCH (n:Doc {key:'one'}) RETURN " + strings.Join(items, ", ")
	}
	write := func(count int) (p50, worst time.Duration) {
		var took []time.Duration
		for i := 0; i < count; i++ {
			start := time.Now()
			_, err := writer.Execute(ctx, "MATCH (c:Counter) SET c.n = c.n + 1", nil)
			require.NoError(t, err)
			took = append(took, time.Since(start))
		}
		sort.Slice(took, func(i, j int) bool { return took[i] < took[j] })
		return took[len(took)/2], took[len(took)-1]
	}
	for _, target := range []int{2000, 50000, 150000, 300000} {
		for next < target {
			_, err := reader.Execute(ctx, statement(1000), nil)
			require.NoError(t, err)
		}
		var one time.Duration
		for i := 0; i < 5; i++ {
			query := statement(1)
			start := time.Now()
			_, err := reader.Execute(ctx, query, nil)
			require.NoError(t, err)
			if took := time.Since(start); took > one {
				one = took
			}
		}
		stop := make(chan struct{})
		var readers sync.WaitGroup
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = reader.Execute(ctx, statement(1), nil)
				}
			}
		})
		p50, worst := write(60)
		close(stop)
		readers.Wait()
		idle50, idleWorst := write(60)
		t.Logf("R1NAMES names=%d one-more-name statement=%s; commit in the other database meanwhile p50=%s max=%s, idle p50=%s max=%s", next, one, p50, worst, idle50, idleWorst)
	}
}
