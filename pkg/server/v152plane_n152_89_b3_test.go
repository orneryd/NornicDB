package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/testing/cypher/caseinputs"
	"github.com/stretchr/testify/require"
)

// The large executor benchmark alone cannot prove the reported HTTP path
// binds the same fulltext/filter owners. Check the exact shared statement
// through the authenticated transaction router with committed fixture writes.
func TestNOR403FulltextExistsHTTP(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			s, users := setupTestServer(t)
			token := "Bearer " + getAuthToken(t, users, "admin")
			path := "/db/" + s.dbManager.DefaultDatabaseName() + "/tx/commit"
			execute := func(t *testing.T, query string, params map[string]any) QueryResult {
				t.Helper()
				response := makeRequest(t, s, http.MethodPost, path, map[string]any{
					"statements": []map[string]any{{"statement": query, "parameters": params}},
				}, token)
				require.Equal(t, http.StatusOK, response.Code, response.Body.String())
				var decoded TransactionResponse
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded))
				require.Empty(t, decoded.Errors, query)
				require.Len(t, decoded.Results, 1)
				return decoded.Results[0]
			}
			for _, query := range []string{
				"CREATE CONSTRAINT nor403_http_doc FOR (d:PDDocument) REQUIRE d.id IS UNIQUE",
				"CREATE CONSTRAINT nor403_http_collection FOR (c:PDCollection) REQUIRE c.id IS UNIQUE",
				"CREATE CONSTRAINT nor403_http_version FOR (v:PDVersion) REQUIRE v.id IS UNIQUE",
				"CREATE INDEX nor403_http_version_document FOR (v:PDVersion) ON (v.document)",
				"CREATE FULLTEXT INDEX " + caseinputs.NOR403FulltextIndex + " FOR (n:PDSource|PDSummary) ON EACH [n.text]",
				`CREATE (:PDCollection {id:'open', archived:false}), (:PDCollection {id:'closed', archived:true})`,
				`CREATE (:PDDocument {id:'d1', current_version_id:'v1'}), (:PDDocument {id:'d2', current_version_id:'v2'})`,
				`MATCH (d:PDDocument {id:'d1'}), (c:PDCollection {id:'open'}) CREATE (d)-[:MEMBER_OF]->(c)`,
				`MATCH (d:PDDocument {id:'d2'}), (c:PDCollection {id:'closed'}) CREATE (d)-[:MEMBER_OF]->(c)`,
				`CREATE (:PDVersion {id:'v1', document:'d1', current_source_id:'s1'}), (:PDVersion {id:'v2', document:'d2', current_source_id:'s2'})`,
				`CREATE (:PDSource {id:'s1', document_id:'d1', text:'boiler alpha'}), (:PDSource {id:'s2', document_id:'d2', text:'boiler alpha'}), (:PDSource {id:'s3', text:'boiler alpha'})`,
				`CREATE (:PDSummary {id:'m1', current:true, owner_kind:'collection', owner_id:'open', source_ids:['s1'], text:'boiler alpha'}), (:PDSummary {id:'m2', current:true, owner_kind:'collection', owner_id:'closed', source_ids:['s2'], text:'boiler alpha'}), (:PDSummary {id:'m3', current:true, owner_kind:'document', owner_id:'d1', source_ids:['s1'], text:'boiler alpha'}), (:PDSummary {id:'m4', current:true, owner_kind:'document', owner_id:'d1', source_ids:['missing'], text:'boiler alpha'})`,
			} {
				execute(t, query, nil)
			}
			for _, tc := range []struct {
				name, query string
				limit       int
				want        []string
			}{
				{"raw", caseinputs.NOR403RawStatement, 20, []string{"m1", "m2", "m3", "m4", "s1", "s2", "s3"}},
				{"full-filter", caseinputs.NOR403EligibilityStatement, 20, []string{"m1", "m3", "s1", "s3"}},
				{"limit3", caseinputs.NOR403EligibilityStatement, 3, []string{"m1", "m3", "s1"}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					result := execute(t, tc.query, map[string]any{"query": "boiler", "limit": tc.limit})
					require.Equal(t, []string{"node", "score"}, result.Columns)
					ids := make([]string, 0, len(result.Data))
					for _, data := range result.Data {
						require.Len(t, data.Row, 2)
						node, ok := data.Row[0].(map[string]any)
						require.True(t, ok, "node row must be a JSON property object: %T", data.Row[0])
						id, ok := node["id"].(string)
						require.True(t, ok, "node must preserve its id property: %v", node)
						ids = append(ids, id)
						score, ok := data.Row[1].(float64)
						require.True(t, ok)
						require.Positive(t, score)
					}
					require.Equal(t, tc.want, ids)
				})
			}
		})
	}
}
