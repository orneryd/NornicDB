package cypher

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// BenchmarkExpandReferences measures the reference closure of a library of
// 70,802 nodes and 96,160 relationships: 5,280 selected Items (with HAS
// relationships to their Notes) naming Topics, Notes and People by id,
// Notes naming People and other Notes, and generated, manual and other LINKs
// between Notes and People. "procedure" is the one call
// (referenceClosureCall), "rounds" the client loop it replaces
// (referenceClosureRounds), checked equal first. Each runs in its own
// explicit read transaction, as an export reads, so no result is served from
// the auto-commit result cache.
func BenchmarkExpandReferences(b *testing.B) {
	exec, ctx := newReferenceClosureBenchmark(b)
	params := referenceClosureParams()
	_, closure := runReferenceClosure(b, exec, ctx, referenceClosureCall, params)
	require.Equal(b, referenceClosureRounds(b, exec, ctx), closure)
	inTransaction := func(b *testing.B, run func()) {
		if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
			b.Fatal(err)
		}
		run()
		if _, err := exec.Execute(ctx, "ROLLBACK", nil); err != nil {
			b.Fatal(err)
		}
	}
	b.Run("procedure", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			inTransaction(b, func() {
				result, err := exec.Execute(ctx, referenceClosureCall, params)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != len(closure) {
					b.Fatalf("the procedure found %d elements, %d before", len(result.Rows), len(closure))
				}
				gdsBenchmarkResult = result
			})
		}
		b.ReportMetric(float64(len(closure)), "elements")
	})
	b.Run("rounds", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			inTransaction(b, func() {
				if parts := referenceClosureRounds(b, exec, ctx); len(parts) != len(closure) {
					b.Fatalf("rounds found %d elements, the procedure %d", len(parts), len(closure))
				}
			})
		}
	})
}

func newReferenceClosureBenchmark(b testing.TB) (*StorageExecutor, context.Context) {
	b.Helper()
	const items, notes, people, topics = 5280, 30000, 15000, 20522
	const has, links = 10560, 85600
	store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "bench")
	random := rand.New(rand.NewSource(17))
	create := func(label string, count int, prefix string, props func(int) map[string]interface{}) {
		for i := 0; i < count; i++ {
			properties := props(i)
			properties["id"] = fmt.Sprintf("%s%d", prefix, i)
			// One Person in a hundred shares its id with a Note.
			if label == "Person" && i%100 == 0 {
				properties["id"] = fmt.Sprintf("n%d", i)
			}
			_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("%s-%d", label, i)), Labels: []string{label}, Properties: properties})
			require.NoError(b, err)
		}
	}
	create("Item", items, "i", func(i int) map[string]interface{} {
		props := map[string]interface{}{"parent_id": fmt.Sprintf("s%d", random.Intn(topics))}
		if i%20 == 0 {
			props["cites"] = []interface{}{fmt.Sprintf("n%d", random.Intn(notes)), fmt.Sprintf("p%d", random.Intn(people))}
		}
		return props
	})
	create("Note", notes, "n", func(i int) map[string]interface{} {
		props := map[string]interface{}{}
		if i%2 == 0 {
			props["editor_id"] = fmt.Sprintf("p%d", random.Intn(people))
		}
		if i%3 == 0 {
			props["author_id"] = fmt.Sprintf("n%d", random.Intn(notes))
		}
		return props
	})
	create("Person", people, "p", func(int) map[string]interface{} { return map[string]interface{}{} })
	create("Topic", topics, "s", func(int) map[string]interface{} { return map[string]interface{}{} })
	edge := func(id int, kind, start, end string, props map[string]interface{}) {
		require.NoError(b, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%d", id)), Type: kind, StartNode: storage.NodeID(start), EndNode: storage.NodeID(end), Properties: props}))
	}
	for i := 0; i < has; i++ {
		edge(i, "HAS", fmt.Sprintf("Item-%d", i%items), fmt.Sprintf("Note-%d", random.Intn(notes)), map[string]interface{}{})
	}
	for i := 0; i < links; i++ {
		props := map[string]interface{}{"source": "generated"}
		switch roll := random.Intn(100); {
		case roll < 8:
			props["source"] = "manual"
		case roll < 10:
			props["source"] = "other"
		case roll < 11:
			props["reason"] = "why"
		}
		if i%4 == 0 {
			props["author_id"] = fmt.Sprintf("n%d", random.Intn(notes))
		}
		edge(has+i, "LINK", fmt.Sprintf("Note-%d", random.Intn(notes)), fmt.Sprintf("Person-%d", random.Intn(people)), props)
	}
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	for _, domain := range referenceClosureDomains {
		_, err := exec.Execute(ctx, "CREATE INDEX FOR (n:"+domain+") ON (n.id)", nil)
		require.NoError(b, err)
	}
	return exec, ctx
}
