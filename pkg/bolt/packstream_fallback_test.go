package bolt

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestPackStream_FallbackTypes_CommonCypherQueries(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := cypher.NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name:'Alice', created_at: datetime(), raw: [1,2,3]})
		CREATE (b:Person {name:'Bob', created_at: datetime()})
		CREATE (a)-[:KNOWS {since: 2020, payload: 'x'}]->(b)
	`, nil)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// Ensure some non-JSON-native types appear in results.
	_, _ = exec.Execute(ctx, "MATCH (n:Person {name:'Alice'}) SET n.bin = $b, n.t = $t, n.d = $d", map[string]any{
		"b": []byte{0x01, 0x02, 0x03},
		"t": time.Now().UTC(),
		"d": 1500 * time.Millisecond,
	})

	commonQueries := []string{
		"MATCH (n:Person) RETURN n LIMIT 1",
		"MATCH ()-[r:KNOWS]->() RETURN r LIMIT 1",
		"MATCH p = shortestPath((a:Person {name:'Alice'})-[*..3]->(b:Person {name:'Bob'})) RETURN p LIMIT 1",
		"RETURN [1,2,3] as xs, {a: 1, b: 'x'} as m, true as b, null as n",
		"MATCH (n:Person {name:'Alice'}) RETURN n.bin, n.t, n.d",
	}

	var mu sync.Mutex
	fallbackTypes := map[string]int{}

	prev := packstreamFallbackHook
	packstreamFallbackHook = func(v any) {
		typ := reflect.TypeOf(v)
		name := "<nil>"
		if typ != nil {
			name = typ.String()
			// Collapse pointer noise a bit.
			name = strings.TrimPrefix(name, "*")
		}
		mu.Lock()
		fallbackTypes[name]++
		mu.Unlock()
	}
	t.Cleanup(func() { packstreamFallbackHook = prev })

	for _, q := range commonQueries {
		q := q
		t.Run(q, func(t *testing.T) {
			res, err := exec.Execute(ctx, q, nil)
			if err != nil {
				t.Fatalf("execute failed: %v", err)
			}

			for _, row := range res.Rows {
				for _, cell := range row {
					_ = encodePackStreamValueInto(nil, cell)
				}
			}
		})
	}

	mu.Lock()
	defer mu.Unlock()
	if len(fallbackTypes) == 0 {
		return
	}

	var keys []string
	for k := range fallbackTypes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "- %s (%d)\n", k, fallbackTypes[k])
	}

	t.Fatalf("unexpected PackStream fallback types encountered:\n%s", b.String())
}
