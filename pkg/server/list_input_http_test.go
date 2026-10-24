package server

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/listinputs"
	"github.com/stretchr/testify/require"
)

func TestListInputHTTPRepresentations(t *testing.T) {
	srv, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	client := newListInputHTTPClient(t, srv, token, false)
	store, err := srv.dbManager.GetStorage(srv.dbManager.DefaultDatabaseName())
	require.NoError(t, err)
	for _, input := range listinputs.TransportInputs() {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(input.Name), Labels: []string{"TypedWireList"}, Properties: map[string]interface{}{"name": input.Name, "xs": input.PropertyValue()}})
		require.NoError(t, err)
	}
	for _, explicit := range []bool{false, true} {
		for _, input := range listinputs.TransportInputs() {
			t.Run(fmt.Sprintf("explicit=%t/%s", explicit, input.Name), func(t *testing.T) {
				run := func(t *testing.T, query string, params map[string]interface{}) TransactionResponse {
					t.Helper()
					return client.run(t, explicit, query, params)
				}
				params := map[string]interface{}{"xs": input.Value, "name": input.Name}
				read := func(t *testing.T, query string, params map[string]interface{}) interface{} {
					t.Helper()
					payload := run(t, query, params)
					require.Empty(t, payload.Errors, query)
					require.Len(t, payload.Results, 1, query)
					require.Len(t, payload.Results[0].Data, 1, query)
					return payload.Results[0].Data[0].Row[0]
				}
				if input.CypherWritableProperty() {
					read(t, "CREATE (n:WireListInput {name:$name, xs:$xs}) RETURN n.xs AS v", params)
				}
				for _, template := range listinputs.Queries() {
					t.Run(template, func(t *testing.T) {
						var want interface{}
						if template != "apoc.map.fromPairs(%s)" {
							want = read(t, listinputs.Query(template, input.Literal), nil)
						}
						for _, form := range []struct{ name, prefix, value string }{
							{"literal", "", input.Literal},
							{"parameter", "", "$xs"}, {"with", "WITH $xs AS xs ", "xs"},
							{"written-property", "MATCH (n:WireListInput {name:$name}) ", "n.xs"},
							{"typed-property", "MATCH (n:TypedWireList {name:$name}) ", "n.xs"},
						} {
							if form.name == "written-property" && !input.CypherWritableProperty() {
								continue
							}
							t.Run(form.name, func(t *testing.T) {
								query := form.prefix + listinputs.Query(template, form.value)
								formWant := want
								if template == "apoc.map.fromPairs(%s)" {
									oracle := listinputs.FromPairsExpectation(input, form.name)
									if oracle.Code != "" {
										payload := run(t, query, params)
										require.Len(t, payload.Errors, 1, query)
										require.Equal(t, oracle.Code, payload.Errors[0].Code, query)
										require.Equal(t, float64(7), read(t, "RETURN 7 AS v", nil), "the next independent case survives a rejected input")
										return
									}
									require.NoError(t, json.Unmarshal([]byte(oracle.JSON), &formWant))
								}
								require.Equal(t, formWant, read(t, query, params))
							})
						}
					})
				}
				cleaned := run(t, "MATCH (n:WireListInput {name:$name}) DETACH DELETE n", params)
				require.Empty(t, cleaned.Errors)
			})
		}
	}
}
