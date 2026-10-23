package cypher

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGDSLouvainMissingSeedNames(t *testing.T) {
	for _, missing := range []interface{}{nil, int64(math.MinInt64)} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) {
			graphProjections = make(map[string]*GraphProjection)
			t.Cleanup(func() { graphProjections = make(map[string]*GraphProjection) })
			props := map[string]interface{}{}
			if missing != nil {
				props["seed"] = missing
			}
			_, run := newGDSEngineGraph(t, "missingseeds",
				gdsNode("0", "N", props), gdsNode("1", "N", map[string]interface{}{"seed": int64(2)}))
			_, err := run("MATCH (n:N) CREATE (n)-[:R]->(n)")
			require.NoError(t, err)
			_, err = run("CALL gds.graph.project('missingseeds', {N:{properties:'seed'}}, {R:{orientation:'UNDIRECTED'}})")
			require.NoError(t, err)
			result, err := run("CALL gds.louvain.stream('missingseeds', {seedProperty:'seed', concurrency:1})")
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"0", int64(2), nil}, {"1", int64(2), nil}}, result.Rows)
		})
	}
}

// BenchmarkLouvainProjected3000 exercises the shared colouring and modularity
// engine through the public procedure, including construction of its graph.
func BenchmarkLouvainProjected3000(b *testing.B) {
	for _, orientation := range []string{"NATURAL", "REVERSE", "UNDIRECTED"} {
		b.Run(orientation, func(b *testing.B) {
			engine := storage.NewNamespacedEngine(fixtureowner.Own(b, fixtureowner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "benchlouvain")
			defer engine.Close()
			exec := NewStorageExecutor(engine)
			for i := 0; i < 3000; i++ {
				if _, err := engine.CreateNode(gdsNode(fmt.Sprintf("n%04d", i), "L", nil)); err != nil {
					b.Fatal(err)
				}
			}
			for i := 0; i < 3000; i++ {
				for step := 1; step <= 4; step++ {
					target := (i/30)*30 + (i+step)%30
					if step == 4 && i%30 == 0 {
						target = (i + 30) % 3000
					}
					if err := engine.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%04d_%d", i, step)), StartNode: storage.NodeID(fmt.Sprintf("n%04d", i)), EndNode: storage.NodeID(fmt.Sprintf("n%04d", target)), Type: "R"}); err != nil {
						b.Fatal(err)
					}
				}
			}
			ctx := context.Background()
			if _, err := exec.Execute(ctx, fmt.Sprintf("CALL gds.graph.project('benchmark','L',{R:{orientation:'%s'}})", orientation), nil); err != nil {
				b.Fatal(err)
			}
			defer exec.Execute(ctx, "CALL gds.graph.drop('benchmark')", nil)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := exec.Execute(ctx, "CALL gds.louvain.stream('benchmark',{concurrency:1,consecutiveIds:true})", nil)
				if err != nil || len(result.Rows) != 3000 {
					b.Fatalf("Louvain rows: %v", err)
				}
			}
		})
	}
}
