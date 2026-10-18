package bolt

import (
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRuntimeMapWireContract(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "r73wire")
	defer store.Close()
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close(ctx)
	run := func(q string, p map[string]interface{}) ([][]interface{}, error) {
		session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "r73wire"})
		defer session.Close(ctx)
		result, err := session.Run(ctx, q, p)
		if err != nil {
			return nil, err
		}
		records, err := result.Collect(ctx)
		var rows [][]interface{}
		for _, r := range records {
			rows = append(rows, r.Values)
		}
		return rows, err
	}
	for _, input := range []struct {
		name, expr string
		p          map[string]interface{}
	}{
		{"wire-null", "$p", map[string]interface{}{"p": nil}},
		{"driver-typed-nil-map", "$p", map[string]interface{}{"p": map[string]interface{}(nil)}},
		{"wire-empty-map", "$p", map[string]interface{}{"p": map[string]interface{}{}}},
		{"wire-json-null", "apoc.convert.fromJsonMap($text)", map[string]interface{}{"text": "null"}},
		{"wire-nested-null", "$row.value", map[string]interface{}{"row": map[string]interface{}{"value": nil}}},
	} {
		for _, entity := range []string{"n", "r"} {
			for _, op := range []string{"=", "+="} {
				if _, err = run("MATCH (n) DETACH DELETE n", nil); err != nil {
					t.Fatal(err)
				}
				if _, err = run("CREATE (:R73Wire {keep:1,x:7})-[:R73WireR {keep:2,x:8}]->(:R73WireOther)", nil); err != nil {
					t.Fatal(err)
				}
				_, writeErr := run("MATCH (n:R73Wire)-[r:R73WireR]->() SET "+entity+" "+op+" "+input.expr, input.p)
				state, err := run("MATCH (n:R73Wire)-[r:R73WireR]->() RETURN properties(n),properties(r)", nil)
				if err != nil {
					t.Fatal(err)
				}
				valid := input.name == "wire-empty-map" || input.name == "driver-typed-nil-map"
				if valid {
					require.NoError(t, writeErr, input.name)
				} else {
					require.Error(t, writeErr, input.name)
				}
				wantNode := map[string]interface{}{"keep": int64(1), "x": int64(7)}
				wantRel := map[string]interface{}{"keep": int64(2), "x": int64(8)}
				if valid && op == "=" {
					if entity == "n" {
						wantNode = map[string]interface{}{}
					} else {
						wantRel = map[string]interface{}{}
					}
				}
				require.Equal(t, [][]interface{}{{wantNode, wantRel}}, state, input.name)
			}
		}
	}
	// These raw markers establish the contract independently of the Go driver's typed-nil encoding.
	for _, data := range [][]byte{{0xc0}, {0xa0}} {
		value, _, err := decodePackStreamValue(data, 0)
		require.NoError(t, err)
		if data[0] == 0xc0 {
			require.Nil(t, value)
		} else {
			properties, ok := value.(map[string]interface{})
			require.True(t, ok)
			require.NotNil(t, properties)
			require.Empty(t, properties)
		}
	}
}
