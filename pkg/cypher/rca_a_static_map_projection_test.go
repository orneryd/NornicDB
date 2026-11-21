package cypher

// Root-cause diagnostic (campaign v1.5.2, lane A, group G-static-map-projection).
// Test-side only. The refusal names the receiver "b.k = a": the static
// operator checker's derive (static_operator_types.go:441-451) asks
// staticMapProjectionReceiver (static_operator_types.go:995-1025) for the text
// before a trailing '{...}' and takes the whole prefix "b.k = a" as the
// receiver, because the prefix ends in an operand; mapProjectionSplit then
// rejects that receiver. Logs the decisive values and the same statement
// written with the comparison in WHERE (no pattern property involved).

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestRCAStaticMapProjectionReceiver(t *testing.T) {
	for _, expr := range []string{"a{.k}", "b.k = a{.k}", "a{.k} = b.k", "(b.k = a{.k})", "x + a{.k}"} {
		receiver, projected := staticMapProjectionReceiver(expr)
		_, _, split := mapProjectionSplit(expr, mapProjectionStatic, nil)
		t.Logf("RCA_MAPPROJ expr=%q receiver=%q projected=%v mapProjectionSplit(whole)=%v", expr, receiver, projected, split)
	}
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:A {k: 1})-[:R]->(:B {k: 1})", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"MATCH (a)-->(b {k: a{.k}}) RETURN b",
		"MATCH (a)-->(b) WHERE b.k = a{.k} RETURN b",
		"MATCH (a)-->(b) WHERE a{.k} = b.k RETURN b",
		"MATCH (a)-->(b) WITH a, b, a{.k} AS p WHERE b.k = p RETURN b",
	} {
		res, err := exec.Execute(ctx, q, nil)
		rows := -1
		if res != nil {
			rows = len(res.Rows)
		}
		t.Logf("RCA_MAPPROJ query=%q rows=%d err=%v", q, rows, err)
	}
}
