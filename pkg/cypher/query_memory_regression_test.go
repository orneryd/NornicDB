package cypher

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func codeIntelReadFixture(t testing.TB, nodes, edges int) *StorageExecutor {
	t.Helper()
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "codeintel"))
	exec.cache = nil
	ctx := context.Background()
	_, err = exec.Execute(ctx, "CREATE CONSTRAINT ci_node_key FOR (n:CodeIntel) REQUIRE n.id IS UNIQUE", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, fmt.Sprintf("UNWIND range(0,%d) AS i CREATE (:CodeIntel {id:i, body:$body})", nodes-1), map[string]interface{}{"body": strings.Repeat("x", 4096)})
	require.NoError(t, err)
	_, err = exec.Execute(ctx, fmt.Sprintf("UNWIND range(0,%d) AS i MATCH (a:CodeIntel {id:i %% %d}) MATCH (b:CodeIntel {id:(i * 37 + 1) %% %d}) CREATE (a)-[:CALLS {_ci_key:toString(i), props_hash:'fixed'}]->(b)", edges-1, nodes, nodes), nil)
	require.NoError(t, err)
	return exec
}

func TestCodeIntelLabelledCountAllocations(t *testing.T) {
	const edges = 8192
	exec := codeIntelReadFixture(t, 256, edges)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	result, err := exec.Execute(context.Background(), "MATCH (:CodeIntel)-[r]->(:CodeIntel) RETURN count(r) AS n", nil)
	runtime.ReadMemStats(&after)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(edges)}}, result.Rows)
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("labelled count: %d allocated bytes for %d edges", allocated, edges)
	// The input is deliberately much larger than the scalar output. This
	// ceiling catches retained paths/binding maps and decoding node bodies.
	require.Less(t, allocated, uint64(32<<20))
}

func TestCodeIntelRelationshipReadSemantics(t *testing.T) {
	exec := codeIntelReadFixture(t, 4, 8)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Other {id:99})", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		query string
		want  int64
	}{
		{"MATCH (:CodeIntel)-[r]->(:CodeIntel) RETURN count(r)", 8},
		{"MATCH (:Other)-[r]->(:CodeIntel) RETURN count(r)", 0},
		{"MATCH (:CodeIntel)<-[r]-(:CodeIntel) RETURN count(r)", 8},
		{"MATCH (:CodeIntel)-[r]-(:CodeIntel) RETURN count(r)", 16},
		{"MATCH (a:CodeIntel)-[r:CALLS]->(b:CodeIntel) WHERE a.id = 1 RETURN count(r)", 2},
		{"UNWIND [0,1] AS i MATCH (a:CodeIntel {id:i})-[r]->() RETURN count(r)", 4},
		{"MATCH (a:CodeIntel)-[r]->(a) RETURN count(r)", 0},
	} {
		result, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.query)
		require.Equal(t, [][]interface{}{{tc.want}}, result.Rows, tc.query)
	}
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (a:CodeIntel {id:0}), (b:Other {id:99}) CREATE (a)-[:PENDING]->(b)", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (:CodeIntel)-[r:PENDING]->(:Other) RETURN count(r)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
}

func BenchmarkCodeIntelRelationshipReads(b *testing.B) {
	exec := codeIntelReadFixture(b, 128, 2048)
	for _, query := range []struct{ name, text string }{
		{"labelled_count", "MATCH (:CodeIntel)-[r]->(:CodeIntel) RETURN count(r)"},
		} {
		b.Run(query.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				run := executeStreamed(context.Background(), exec, query.text, 16)
				if run.stream.streamed() {
					for {
						_, done := run.stream.Next(128)
						if done { break }
					}
				}
				<-run.stream.Done()
				if run.err != nil { b.Fatal(run.err) }
			}
		})
	}
}
func TestCodeIntelRelationshipIdentityWorkingSet(t *testing.T) {
	const edges = 8192
	exec := codeIntelReadFixture(t, 256, edges)
	runtime.GC()
	var before, blocked runtime.MemStats
	runtime.ReadMemStats(&before)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run := executeStreamed(ctx, exec, "MATCH ()-[r]->() RETURN startNode(r).id AS a,endNode(r).id AS b,type(r) AS t,r.site AS site,r.props_hash AS hash", 16)
	t.Cleanup(func() { cancel(); <-run.stream.Done() })
	require.True(t, run.stream.streamed(), "identity query must use the result stream: %v", run.err)
	// The producer is now stopped at stream demand. GC removes transient
	// decoder allocations; retained MATCH inputs remain observable.
	runtime.GC()
	runtime.ReadMemStats(&blocked)
	live := int64(blocked.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("identity query: %d retained bytes at the first fetch", live)
	var rows int
	for {
		batch, done := run.stream.Next(128)
		for _, row := range batch {
			require.Len(t, row, 5)
			a, b := row[0].(int64), row[1].(int64)
			require.GreaterOrEqual(t, a, int64(0))
			require.Less(t, b, int64(256))
			require.Equal(t, []interface{}{"CALLS", nil, "fixed"}, row[2:])
		}
		rows += len(batch)
		if done {
			break
		}
	}
	<-run.stream.Done()
	require.NoError(t, run.err)
	require.Equal(t, edges, rows)
	require.Less(t, live, int64(8<<20), "fetching 16 scalar rows must not retain all relationship bindings")
}


