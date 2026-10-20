package bolt

import (
	"context"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/listinputs"
	"github.com/stretchr/testify/require"
)

func TestListInputBoltRepresentations(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "list_inputs")
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	for _, input := range listinputs.TransportInputs() {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(input.Name), Labels: []string{"TypedWireList"}, Properties: map[string]interface{}{"name": input.Name, "xs": input.PropertyValue()}})
		require.NoError(t, err)
	}
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(ctx)) })
	for _, explicit := range []bool{false, true} {
		for _, input := range listinputs.TransportInputs() {
			t.Run(fmt.Sprintf("explicit=%t/%s", explicit, input.Name), func(t *testing.T) {
				session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "list_inputs"})
				defer session.Close(ctx)
				run := func(ctx context.Context, query string, params map[string]interface{}) (neo4j.ResultWithContext, error) {
					return session.Run(ctx, query, params)
				}
				var tx neo4j.ExplicitTransaction
				if explicit {
					tx, err = session.BeginTransaction(ctx)
					require.NoError(t, err)
					defer tx.Close(ctx)
					run = tx.Run
				}
				params := map[string]interface{}{"xs": input.Value, "name": input.Name}
				read := func(t *testing.T, query string, params map[string]interface{}) interface{} {
					t.Helper()
					result, err := run(ctx, query, params)
					require.NoError(t, err, query)
					records, err := result.Collect(ctx)
					require.NoError(t, err, query)
					require.Len(t, records, 1, query)
					return records[0].Values[0]
				}
				if input.CypherWritableProperty() {
					read(t, "CREATE (n:WireListInput {xs:$xs}) RETURN n.xs AS v", params)
				}
				for _, template := range listinputs.Queries() {
					t.Run(template, func(t *testing.T) {
						want := read(t, listinputs.Query(template, input.Literal), nil)
						for _, form := range []struct{ name, prefix, value string }{
							{"parameter", "", "$xs"}, {"with", "WITH $xs AS xs ", "xs"},
							{"written-property", "MATCH (n:WireListInput) ", "n.xs"},
							{"typed-property", "MATCH (n:TypedWireList {name:$name}) ", "n.xs"},
						} {
							if form.name == "written-property" && !input.CypherWritableProperty() {
								continue
							}
							t.Run(form.name, func(t *testing.T) {
								require.Equal(t, want, read(t, form.prefix+listinputs.Query(template, form.value), params))
							})
						}
					})
				}
				result, err := run(ctx, "MATCH (n:WireListInput) DETACH DELETE n", nil)
				require.NoError(t, err)
				_, err = result.Consume(ctx)
				require.NoError(t, err)
				if explicit {
					require.NoError(t, tx.Commit(ctx))
				}
			})
		}
	}
}
