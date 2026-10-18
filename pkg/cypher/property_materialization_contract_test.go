package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

// Every materializer and result state has an explicit semantic expectation;
// error rows compare durable nodes/edges, including existing property bags.
func TestPropertyMaterializationContract(t *testing.T) {
	states := []struct {
		name, expr                 string
		invalid, null, mapOrEntity bool
	}{
		{"unresolved-properties", "properties(1)", true, false, false},
		{"unresolved-nodes", "nodes(1)", true, false, false},
		{"unresolved-bound", "properties(v)", true, false, false},
		{"unresolved-list-child", "[nodes(1)]", true, false, false},
		{"recorded-error", "1 / 0", true, false, false},
		{"recorded-json", "apoc.convert.fromJsonMap('1')", true, false, false},
		{"null", "null", false, true, false},
		{"map", "{keep:7}", false, false, true},
		{"entity", "n", false, false, true},
		{"literal", "'properties(1)'", false, false, false},
		{"bound-equal-text", "v", false, false, false},
		{"dotted", "n.keep", false, false, false},
	}
	routes := []struct{ name, query, kind string }{
		{"create-node", "CREATE (:R3Made {x:%s})", "field"},
		{"create-rel", "CREATE (:R3Made)-[:R3Made {x:%s}]->(:R3Made)", "field"},
		{"create-bound-rel", "CREATE (n)-[:R3Made {x:%s}]->(m)", "field"},
		{"create-late-field", "CREATE (:R3Made {ok:1}), (:R3Made {x:%s})", "field"},
		{"merge-node", "MERGE (:R3Made {x:%s})", "identity"},
		{"merge-rel", "MERGE (n)-[:R3Made {x:%s}]->(m)", "identity"},
		{"merge-endpoint", "MERGE (:R3Made {x:%s})-[:R3Made]->(:R3Made)", "identity"},
		{"set-node", "SET n.x = %s", "field"},
		{"set-rel", "SET r.x = %s", "field"},
		{"set-node-dynamic", "SET n['x'] = %s", "field"},
		{"set-rel-dynamic", "SET r['x'] = %s", "field"},
		{"set-node-map=", "SET n = {x:%s}", "field"},
		{"set-node-map+=", "SET n += {x:%s}", "field"},
		{"set-rel-map=", "SET r = {x:%s}", "field"},
		{"set-rel-map+=", "SET r += {x:%s}", "field"},
		{"set-node-root=", "SET n = %s", "root"},
		{"set-node-root+=", "SET n += %s", "root"},
		{"set-rel-root=", "SET r = %s", "root"},
		{"set-rel-root+=", "SET r += %s", "root"},
		{"on-create", "MERGE (z:R3Made {id:1}) ON CREATE SET z.x = %s", "field"},
		{"on-match", "MERGE (z:R3Seed {id:1}) ON MATCH SET z.x = %s", "field"},
		{"foreach", "FOREACH (i IN [1,2] | CREATE (:R3Made)-[:R3Made {x:%s}]->(:R3Made))", "field"},
		{"unwind-create", "UNWIND [1,2] AS i CREATE (:R3Made)-[:R3Made {x:%s}]->(:R3Made)", "field"},
		{"unwind-merge", "UNWIND [1,2] AS i MERGE (:R3Made {id:i,x:%s})", "identity"},
		{"subquery-create", "CALL { WITH n,m,r,v CREATE (:R3Made)-[:R3Made {x:%s}]->(:R3Made) }", "field"},
	}
	for _, route := range routes {
		for _, state := range states {
			t.Run(route.name+"/"+state.name, func(t *testing.T) {
				for round := 0; round < 3; round++ {
					store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "r3_gate")
					e := NewStorageExecutor(store)
					ctx := context.Background()
					if _, err := e.Execute(ctx, "CREATE (:R3Seed {id:1,keep:7,x:9})-[:R3Seed {keep:8,x:9}]->(:R3Other)", nil); err != nil {
						t.Fatal(err)
					}
					snapshot := func() string {
						ns, err := store.AllNodes()
						if err != nil {
							t.Fatal(err)
						}
						rs, err := store.AllEdges()
						if err != nil {
							t.Fatal(err)
						}
						bags := map[string]interface{}{}
						for _, n := range ns {
							bags["n:"+string(n.ID)] = n
						}
						for _, r := range rs {
							bags["r:"+string(r.ID)] = r
						}
						b, err := json.Marshal(bags)
						if err != nil {
							t.Fatal(err)
						}
						return string(b)
					}
					before := snapshot()
					bound := "1"
					if state.name == "bound-equal-text" {
						bound = "'v'"
					}
					q := "MATCH (n:R3Seed)-[r:R3Seed]->(m) WITH n,r,m," + bound + " AS v " + fmt.Sprintf(route.query, state.expr)
					_, err := e.Execute(ctx, q, nil)
					after := snapshot()
					wantError := state.invalid || route.kind == "identity" && state.null || route.kind != "root" && state.mapOrEntity || route.kind == "root" && !state.mapOrEntity
					ok := (err != nil) == wantError && (!wantError || before == after)
					t.Logf("R73R3|%s|%s|%d|wantError=%v|err=%v|unchanged=%v|ok=%v", route.name, state.name, round, wantError, err, before == after, ok)
					if !wantError && err == nil {
						assertMaterializedValue(t, store, route.name, state.name)
					}
					if !ok {
						t.Errorf("property materializer contract: query=%s error=%v noWrite=%v", q, err, before == after)
					}
				}
			})
		}
	}
}

// Existing parser tests use this only for successful parse results. Failure
// cases assert the returned error separately, rather than accepting source text.
func parsedTestValue[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}
