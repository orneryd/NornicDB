package bolt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFunctionLexicalCommentBolt(t *testing.T) {
	raw, err := os.ReadFile("../cypher/testdata/function_lexical_oracle.json")
	require.NoError(t, err)
	var cases []struct {
		ID    string
		Query string
		Rows  json.RawMessage
	}
	require.NoError(t, json.Unmarshal(raw, &cases))
	require.NotEmpty(t, cases)
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	_, port := startBoltIntegrationServerWithExplicitTx(t, storage.NewNamespacedEngine(base, "neo4j"))
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	for _, normalize := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, tc := range cases {
				t.Run(fmt.Sprintf("normalize=%t/explicit=%t/%s", normalize, explicit, tc.ID), func(t *testing.T) {
					config.SetCypherQueryNormalizationEnabled(normalize)
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
					defer session.Close(ctx)
					var result neo4j.ResultWithContext
					var tx neo4j.ExplicitTransaction
					if explicit {
						tx, err = session.BeginTransaction(ctx)
						require.NoError(t, err)
						defer tx.Rollback(ctx)
						result, err = tx.Run(ctx, tc.Query, nil)
					} else {
						result, err = session.Run(ctx, tc.Query, nil)
					}
					rows := make([][]interface{}, 0)
					if err == nil {
						for result.Next(ctx) {
							rows = append(rows, result.Record().Values)
						}
						err = result.Err()
					}
					queryErr := err
					code, message := "", ""
					if queryErr != nil {
						message = queryErr.Error()
						var classified *neo4j.Neo4jError
						if errors.As(queryErr, &classified) {
							code = classified.Code
						}
					}
					record, err := json.Marshal(map[string]interface{}{
						"id": tc.ID, "query": tc.Query, "route": "bolt",
						"normalize": normalize, "explicit": explicit,
						"rows": rows, "code": code, "message": message,
					})
					require.NoError(t, err)
					t.Log("INNER_AGG_RESULT " + string(record))
					if assert.NoError(t, queryErr) {
						got, err := json.Marshal(rows)
						require.NoError(t, err)
						assert.JSONEq(t, string(tc.Rows), string(got))
					}
				})
			}
		}
	}
}
