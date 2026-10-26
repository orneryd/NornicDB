package fnboundary_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/cypher"
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
	"strings"
	"testing"
)

// Author: #1663 implementation lane. The compatibility population includes
// the value-bound native host as well as expression hosts. This public-only
// proof is also an overlay on release; the cases and assertions are lane design.
func TestNativeAliasExternalReleaseContract(t *testing.T) {
	calls := 0
	cypherfn.Register("id", func(ctx cypherfn.Context, args []string) (interface{}, error) {
		calls++
		first, err := ctx.Eval("madeUpName")
		if err != nil {
			return nil, err
		}
		second, err := ctx.Eval("1 / 0")
		if err != nil {
			return nil, err
		}
		return fmt.Sprintf("%s/%T/%T", args[0], first, second), nil
	})
	query := "CREATE (n:ExternalAlias {p: 1}) RETURN apoc.node.id(n) AS out"
	// Storage requires namespaced node identities (release behaviour).
	executor := cypher.NewStorageExecutor(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic"))
	result, err := executor.Execute(context.Background(), query, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows, _ := json.Marshal(result.Rows)
	if string(rows) != `[["value/*storage.Node/*storage.Node"]]` || calls == 0 {
		t.Fatalf("rows=%s calls=%d", rows, calls)
	}
	receipt, _ := json.Marshal(map[string]interface{}{"author": "#1663 implementation lane", "case": "native_value_host", "query": query, "rows": result.Rows, "error": "", "external_calls": calls})
	t.Logf("EXTERNAL_COMPAT %s", receipt)
}

// Author: #1663 implementation lane. Shell shorthand and map input reach the
// public executor and the retained original value records. Malformed nested
// values must be rejected before shell state is changed.
func TestShellOriginalValueContract(t *testing.T) {
	value := `{payload: {answer: 42}, items: [{answer: 1}]}`
	for _, command := range []string{":param x => " + value, ":params {x: " + value + "}"} {
		executor := cypher.NewStorageExecutor(storage.NewMemoryEngine())
		if _, err := executor.Execute(context.Background(), command, nil); err != nil {
			t.Fatalf("%s: %v", command, err)
		}
		result, err := executor.Execute(context.Background(), "RETURN $x.payload.answer AS answer, $x.items[0].answer AS first", nil)
		if err != nil {
			t.Fatal(err)
		}
		rows, _ := json.Marshal(result.Rows)
		if string(rows) != `[[42,1]]` {
			t.Fatalf("%s: rows=%s", command, rows)
		}
	}
	for _, command := range []string{":param x => {payload: {broken}}", ":params {x: {payload: {broken}}}"} {
		executor := cypher.NewStorageExecutor(storage.NewMemoryEngine())
		if _, err := executor.Execute(context.Background(), ":param x => 0", nil); err != nil {
			t.Fatal(err)
		}
		_, err := executor.Execute(context.Background(), command, nil)
		if err == nil || !strings.Contains(err.Error(), "map") {
			t.Fatalf("malformed original value admitted: %s: %v", command, err)
		}
		result, err := executor.Execute(context.Background(), "RETURN $x AS x", nil)
		if err != nil {
			t.Fatal(err)
		}
		rows, _ := json.Marshal(result.Rows)
		if string(rows) != `[[0]]` {
			t.Fatalf("rejected shell input changed x: %s: %s", command, rows)
		}
	}
}
