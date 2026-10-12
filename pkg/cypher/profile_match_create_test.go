package cypher

// Profile harness for TestBenchmarkMatchCreateDelete_LargeDataset_Direct.
//
// Run with:
//
//	NORNICDB_PROFILE_MATCH_CREATE=1 go test -tags noui,nolocalllm ./pkg/cypher \
//	  -run TestProfileMatchCreateDelete -count=1 \
//	  -cpuprofile=/tmp/mcd.cpu -mutexprofile=/tmp/mcd.mutex \
//	  -blockprofile=/tmp/mcd.block -memprofile=/tmp/mcd.mem
//
// then: go tool pprof -top /tmp/mcd.cpu  (and -top the others).

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestProfileMatchCreateDelete mirrors TestBenchmarkMatchCreateDelete_LargeDataset_Direct
// with a larger iteration count so the sampling profilers have signal. Skipped
// unless NORNICDB_PROFILE_MATCH_CREATE is set.
func TestProfileMatchCreateDelete(t *testing.T) {
	if os.Getenv("NORNICDB_PROFILE_MATCH_CREATE") == "" {
		t.Skip("set NORNICDB_PROFILE_MATCH_CREATE=1 to run the profile harness")
	}
	baseEngine := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(baseEngine, "test")
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	for i := 0; i < 100; i++ {
		if _, err := executor.Execute(ctx, fmt.Sprintf("CREATE (a:Actor {name: 'Actor_%d'})", i), nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 150; i++ {
		if _, err := executor.Execute(ctx, fmt.Sprintf("CREATE (m:Movie {title: 'Movie_%d'})", i), nil); err != nil {
			t.Fatal(err)
		}
	}

	const iterations = 20_000
	start := time.Now()
	for i := 0; i < iterations; i++ {
		if _, err := executor.Execute(ctx, `
			MATCH (a:Actor), (m:Movie)
			WITH a, m LIMIT 1
			CREATE (a)-[r:TEMP_REL]->(m)
			DELETE r
		`, nil); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	t.Logf("%d iterations: %.2f ops/sec, %.3f us/op", iterations, float64(iterations)/elapsed.Seconds(), float64(elapsed.Microseconds())/iterations)
}
