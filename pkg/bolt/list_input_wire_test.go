package bolt

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/listinputs"
	"github.com/stretchr/testify/require"
)

func TestListInputBoltRepresentations(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	store := storage.NewNamespacedEngine(engine, "list_inputs")
	inputs := listinputs.TransportInputs()
	for _, input := range inputs {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(input.Name), Labels: []string{"TypedWireList"}, Properties: map[string]interface{}{"name": input.Name, "xs": input.PropertyValue()}})
		require.NoError(t, err)
	}
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(ctx)) })
	for _, explicit := range []bool{false, true} {
		for _, input := range inputs {
			t.Run(fmt.Sprintf("explicit=%t/%s", explicit, input.Name), func(t *testing.T) {
				// An expected statement error ends an explicit transaction. Give
				// each form its own transaction so every assertion executes.
				run := func(query string, params map[string]interface{}) ([]*neo4j.Record, error) {
					session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "list_inputs"})
					defer session.Close(ctx)
					execute := func(ctx context.Context, query string, params map[string]interface{}) (neo4j.ResultWithContext, error) {
						return session.Run(ctx, query, params)
					}
					var tx neo4j.ExplicitTransaction
					if explicit {
						var err error
						tx, err = session.BeginTransaction(ctx)
						if err != nil {
							return nil, err
						}
						defer tx.Close(ctx)
						execute = tx.Run
					}
					result, err := execute(ctx, query, params)
					if err != nil {
						return nil, err
					}
					records, err := result.Collect(ctx)
					if err == nil && explicit {
						err = tx.Commit(ctx)
					}
					return records, err
				}
				params := map[string]interface{}{"xs": input.Value, "name": input.Name}
				read := func(t *testing.T, query string, params map[string]interface{}) interface{} {
					t.Helper()
					records, err := run(query, params)
					require.NoError(t, err, query)
					require.Len(t, records, 1, query)
					return records[0].Values[0]
				}
				if input.CypherWritableProperty() {
					read(t, "CREATE (n:WireListInput {name:$name, xs:$xs}) RETURN n.xs AS v", params)
				}
				for _, template := range listinputs.Queries() {
					t.Run(template, func(t *testing.T) {
						forms := []struct{ name, prefix, value string }{
							{"literal", "", input.Literal},
							{"parameter", "", "$xs"}, {"with", "WITH $xs AS xs ", "xs"},
							{"written-property", "MATCH (n:WireListInput {name:$name}) ", "n.xs"},
							{"typed-property", "MATCH (n:TypedWireList {name:$name}) ", "n.xs"},
						}
						var want interface{}
						if template != "apoc.map.fromPairs(%s)" {
							want = read(t, listinputs.Query(template, input.Literal), nil)
						}
						for _, form := range forms {
							if form.name == "written-property" && !input.CypherWritableProperty() {
								continue
							}
							t.Run(form.name, func(t *testing.T) {
								query := form.prefix + listinputs.Query(template, form.value)
								formWant := want
								if template == "apoc.map.fromPairs(%s)" {
									oracle := listinputs.FromPairsExpectation(input, form.name)
									if oracle.Code != "" {
										_, err := run(query, params)
										var status *neo4j.Neo4jError
										require.ErrorAs(t, err, &status, query)
										require.Equal(t, oracle.Code, status.Code, query)
										return
									}
									require.NoError(t, json.Unmarshal([]byte(oracle.JSON), &formWant))
								}
								require.Equal(t, formWant, read(t, query, params))
							})
						}
					})
				}
				_, err := run("MATCH (n:WireListInput {name:$name}) DETACH DELETE n", params)
				require.NoError(t, err)
			})
		}
	}
}
