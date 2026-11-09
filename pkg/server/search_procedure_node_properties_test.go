package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/search/nodecontract"
	"github.com/stretchr/testify/require"
)

// Configure the same database search service as the production Bolt adapter.
type searchNodeWireExecutor struct{ server *Server }

func (a searchNodeWireExecutor) Execute(ctx context.Context, query string, params map[string]any) (*bolt.QueryResult, error) {
	exec, err := a.server.newExecutorForDatabase(a.server.dbManager.DefaultDatabaseName())
	if err != nil {
		return nil, err
	}
	result, err := exec.Execute(ctx, query, params)
	if err != nil {
		return nil, err
	}
	return &bolt.QueryResult{Columns: result.Columns, Rows: result.Rows, Metadata: result.Metadata}, nil
}
func (a searchNodeWireExecutor) ConfigureDatabaseExecutor(exec *cypher.StorageExecutor, database string, engine storage.Engine) {
	service, err := a.server.db.GetOrCreateSearchService(database, engine)
	if err == nil {
		exec.SetSearchService(service)
	}
}
func TestSearchProcedureNodePropertiesWire(t *testing.T) {
	ctx := context.Background()
	server, authenticator := setupTestServer(t)
	database := server.dbManager.DefaultDatabaseName()
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	base := storage.UnwrapEngine(server.db.GetBaseStorageForManager()).(*storage.BadgerEngine)
	base.SetDecayEnabled(true)
	schema := base.GetSchemaForNamespace(database)
	require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "retrieval_age", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 86400 * 365, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
	require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "retrieval_age", ProfileRef: "retrieval_age", TargetLabels: []string{"RetrievalSource"}}))
	exec, err := server.newExecutorForDatabase(database)
	require.NoError(t, err)
	vector := make([]float64, 2048)
	vector[0] = 1
	params := map[string]interface{}{"vector": vector}
	for i := 1; i <= 2; i++ {
		_, err = exec.Execute(ctx, "CREATE (:RetrievalSource {id:$id, bucket:$id, title:'source', ordinary:42, embedding:$vector})", map[string]interface{}{"id": fmt.Sprintf("source-%d", i), "vector": vector})
		require.NoError(t, err)
	}
	for _, ddl := range []string{
		"CREATE VECTOR INDEX retrieval_vector FOR (n:RetrievalSource) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`:2048, `vector.similarity_function`:'cosine'}}",
		"CREATE FULLTEXT INDEX retrieval_text FOR (n:RetrievalSource) ON EACH [n.title]",
	} {
		_, err := exec.Execute(ctx, ddl, nil)
		require.NoError(t, err)
	}
	engine, err := server.dbManager.GetStorage(database)
	require.NoError(t, err)
	service, err := server.db.GetOrCreateSearchService(database, engine)
	require.NoError(t, err)
	service.EnableRelationshipSearch(nil)
	service.StartRelationshipIndex()
	require.NoError(t, service.BuildIndexes(ctx))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	peer := bolt.NewWithDatabaseManager(bolt.DefaultConfig(), searchNodeWireExecutor{server}, server.dbManager)
	served := make(chan error, 1)
	go func() { served <- peer.Serve(listener) }()
	t.Cleanup(func() { require.NoError(t, peer.Close()); require.NoError(t, <-served) })
	driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(context.Background())) })
	for _, route := range []string{"execute", "bolt", "bolt_tx", "http"} {
		t.Run(route, func(t *testing.T) {
			run := func(query string) ([][]any, error) {
				if route == "execute" {
					result, err := exec.Execute(ctx, query, params)
					if err != nil {
						return nil, err
					}
					return result.Rows, nil
				}
				if route == "http" {
					response := makeRequest(t, server, "POST", "/db/"+database+"/tx/commit", map[string]any{"statements": []map[string]any{{"statement": query, "parameters": params}}}, token)
					if response.Code != 200 {
						return nil, fmt.Errorf("HTTP %d: %s", response.Code, response.Body.String())
					}
					var decoded TransactionResponse
					if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
						return nil, err
					}
					if len(decoded.Errors) > 0 {
						return nil, fmt.Errorf("HTTP Cypher: %s", decoded.Errors[0].Message)
					}
					if len(decoded.Results) != 1 {
						return nil, fmt.Errorf("expected one result, got %d", len(decoded.Results))
					}
					var rows [][]any
					for _, data := range decoded.Results[0].Data {
						rows = append(rows, data.Row)
					}
					return rows, nil
				}
				session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
				defer session.Close(ctx)
				var result neo4j.ResultWithContext
				var err error
				if route == "bolt_tx" {
					tx, beginErr := session.BeginTransaction(ctx)
					if beginErr != nil {
						return nil, beginErr
					}
					defer tx.Close(ctx)
					result, err = tx.Run(ctx, query, params)
				} else {
					result, err = session.Run(ctx, query, params)
				}
				if err != nil {
					return nil, err
				}
				records, err := result.Collect(ctx)
				if err != nil {
					return nil, err
				}
				var rows [][]any
				for _, record := range records {
					rows = append(rows, record.Values)
				}
				return rows, nil
			}
			nodecontract.Run(t, run)
		})
	}
}
