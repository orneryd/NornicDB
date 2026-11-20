package server

// NOR-170 public HTTP transport reproduction.
//
// The maintained migration scripts run over Bolt and the documented
// verification commands run over Bolt/Cypher; this test exercises the same
// node, edge and promotion statements through NornicDB's in-process HTTP
// transaction endpoint so the ordinary public read path is covered as well.
// It asserts the current transport behavior (the migrated nodes stay hidden
// and promotion reports labels_added = 0) as explicit observations;
// the documented completed-migration contract is asserted, and currently
// fails, in pkg/cypher/nor170_migration_visibility_test.go.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

const nor170HTTPNodeUpsertCypher = "UNWIND $rows AS row\n" +
	"MERGE (n:_Migrated {_neo4j_id: row.id})\n" +
	"SET n += row.props\n" +
	"SET n._neo4j_labels = row.labels\n"

const nor170HTTPEdgeCreateCypher = "UNWIND $rows AS row " +
	"MATCH (a:_Migrated {_neo4j_id: row.startId}) " +
	"MATCH (b:_Migrated {_neo4j_id: row.endId}) " +
	"CREATE (a)-[:`KNOWS` {_neo4j_id: row.id}]->(b)"

const nor170HTTPPromptPromotion = "MATCH (n:_Migrated)\n" +
	"WHERE 'Person' IN n._neo4j_labels\n" +
	"SET n:Person\n" +
	"REMOVE n:_Migrated\n" +
	"REMOVE n._neo4j_labels"

func nor170HTTPRows() ([]map[string]any, []map[string]any) {
	nodes := []map[string]any{
		{"id": "n1", "labels": []any{"Person", "Employee"}, "props": map[string]any{"name": "Alice"}},
		{"id": "n2", "labels": []any{"Person"}, "props": map[string]any{"name": "Bob"}},
	}
	edges := []map[string]any{
		{"id": "e1", "startId": "n1", "endId": "n2", "props": map[string]any{"since": int64(2020)}},
	}
	return nodes, edges
}

// TestNOR170HTTPMigratedGraphVisibility drives the documented migration
// statements through /db/<name>/tx/commit under both parser configurations and
// records what the public HTTP reads return for the completed workflow.
func TestNOR170HTTPMigratedGraphVisibility(t *testing.T) {
	for _, parser := range []string{nornicConfig.ParserTypeNornic, nornicConfig.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := nornicConfig.GetParserType()
			nornicConfig.SetParserType(parser)
			t.Cleanup(func() { nornicConfig.SetParserType(previous) })

			srv, authenticator := setupTestServer(t)
			require.Equal(t, parser, nornicConfig.GetParserType())
			token := getAuthToken(t, authenticator, "admin")
			peer := httptest.NewServer(srv.buildRouter())
			t.Cleanup(peer.Close)
			client := peer.Client()
			client.Timeout = 5 * time.Second
			database := srv.dbManager.DefaultDatabaseName()
			path := "/db/" + database + "/tx/commit"

			post := func(t *testing.T, statement string, parameters map[string]any) TransactionResponse {
				t.Helper()
				body, err := json.Marshal(map[string]any{
					"statements": []map[string]any{{
						"statement":    statement,
						"parameters":   parameters,
						"includeStats": true,
					}},
				})
				require.NoError(t, err)
				request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, peer.URL+path, bytes.NewReader(body))
				require.NoError(t, err)
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Authorization", "Bearer "+token)
				response, err := client.Do(request)
				require.NoError(t, err)
				defer response.Body.Close()
				require.Equal(t, http.StatusOK, response.StatusCode)
				var payload TransactionResponse
				require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
				require.Empty(t, payload.Errors)
				require.Len(t, payload.Results, 1)
				return payload
			}

			nodeRows, edgeRows := nor170HTTPRows()
			post(t, nor170HTTPNodeUpsertCypher, map[string]any{"rows": nodeRows})
			post(t, nor170HTTPEdgeCreateCypher, map[string]any{"rows": edgeRows})

			// The nodes exist in storage even though no HTTP read returns them.
			store, err := srv.dbManager.GetDefaultStorage()
			require.NoError(t, err)
			migrated, err := store.GetNodesByLabel(context.Background(), "_Migrated")
			require.NoError(t, err)
			require.Len(t, migrated, 2)
			edges, err := store.AllEdges(context.Background())
			require.NoError(t, err)
			require.Empty(t, edges, "the documented edge batch silently created no relationship")

			countPayload := post(t, "MATCH (n:_Migrated) RETURN count(n) AS c", nil)
			require.Len(t, countPayload.Results[0].Data, 1)
			require.Equal(t, float64(0), countPayload.Results[0].Data[0].Row[0],
				"ordinary HTTP MATCH counts none of the migrated nodes")
			idPayload := post(t, "MATCH (n:_Migrated) RETURN n._neo4j_id AS id ORDER BY id", nil)
			require.Empty(t, idPayload.Results[0].Data)

			// Run the documented promotion. It reports success with zero
			// labels added, because the MATCH matched nothing.
			promotion := post(t, nor170HTTPPromptPromotion, nil)
			require.NotNil(t, promotion.Results[0].Stats)
			require.Zero(t, promotion.Results[0].Stats.LabelsAdded)
			require.Zero(t, promotion.Results[0].Stats.LabelsRemoved)

			// After the documented "completed" workflow the ordinary public
			// read still returns no source-labelled node and no relationship.
			personPayload := post(t, "MATCH (n:Person) RETURN n.name AS name ORDER BY name", nil)
			require.Empty(t, personPayload.Results[0].Data)
			relPayload := post(t, "MATCH (a)-[r:KNOWS]->(b) RETURN a.name AS a, b.name AS b", nil)
			require.Empty(t, relPayload.Results[0].Data)

			// The unfinished state is still present in storage and still
			// labelled only with the migration bridge label.
			migrated, err = store.GetNodesByLabel(context.Background(), "_Migrated")
			require.NoError(t, err)
			require.Len(t, migrated, 2)
			person, err := store.GetNodesByLabel(context.Background(), "Person")
			require.NoError(t, err)
			require.Empty(t, person)
		})
	}
}
