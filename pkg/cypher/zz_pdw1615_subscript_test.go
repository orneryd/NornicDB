package cypher

// Scratch investigation (pd-w161-5), not for merge: a subscript of a stored
// string-list property inside a comprehension, against the same list bound
// once by WITH, at three list sizes. Deciding values are logged with the
// prefix PDW1615.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestPDW1615StoredStringListSubscript(t *testing.T) {
	ctx := context.Background()
	for _, n := range []int{4000, 8000, 16000} {
		engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		store := storage.NewNamespacedEngine(engine, "test")
		exec := NewStorageExecutor(store)
		if _, err := exec.Execute(ctx, "CREATE (:PDW1615 {id: 'one', cs: [x IN range(0, $n - 1) | toString(x)], ci: range(0, $n - 1)})", map[string]any{"n": int64(n)}); err != nil {
			t.Fatal(err)
		}
		nodes, err := store.GetNodesByLabel(ctx, "PDW1615")
		if err != nil || len(nodes) != 1 {
			t.Fatalf("nodes %d err %v", len(nodes), err)
		}
		t.Logf("PDW1615 n=%d stored cs type %T ci type %T", n, nodes[0].Properties["cs"], nodes[0].Properties["ci"])
		cases := []struct{ name, query string }{
			{"string-subscript", "MATCH (s:PDW1615 {id: 'one'}) WHERE $nonce >= 0 RETURN size([i IN range(0, size(s.cs) - 1) WHERE s.cs[i] <> '' | s.cs[i]]) AS c"},
			{"string-bound-once", "MATCH (s:PDW1615 {id: 'one'}) WHERE $nonce >= 0 WITH s.cs AS cs RETURN size([i IN range(0, size(cs) - 1) WHERE cs[i] <> '' | cs[i]]) AS c"},
			{"int-subscript", "MATCH (s:PDW1615 {id: 'one'}) WHERE $nonce >= 0 RETURN size([i IN range(0, size(s.ci) - 1) WHERE s.ci[i] >= 0 | s.ci[i]]) AS c"},
		}
		for _, c := range cases {
			var best time.Duration
			var got any
			for rep := 0; rep < 3; rep++ {
				started := time.Now()
				res, err := exec.Execute(ctx, c.query, map[string]any{"nonce": int64(n*100 + rep*10 + len(c.name))})
				took := time.Since(started)
				if err != nil {
					t.Fatalf("%s n=%d: %v", c.name, n, err)
				}
				if len(res.Rows) == 1 {
					got = res.Rows[0][0]
				}
				t.Logf("PDW1615 n=%d case=%s rep=%d took=%s", n, c.name, rep, took.Round(time.Microsecond))
				if rep == 0 || took < best {
					best = took
				}
			}
			t.Logf("PDW1615 n=%d case=%s result=%v best_of_3=%s", n, c.name, fmt.Sprint(got), best.Round(time.Microsecond))
		}
		_ = engine.Close()
	}
}
