package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Every producer feeds the same execution output budget. All cases cross
// row/byte exhaustion with both actual MCP HTTP forms and native storage.
func TestMCPNativeProducerMatrix(t *testing.T) {
	s, authenticator := setupTestServer(t)
	store, err := s.dbManager.GetStorage("nornic")
	require.NoError(t, err)
	for i := 0; i < 12; i++ {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("matrix-%d", i)), Labels: []string{"MatrixDoc"}, Properties: map[string]interface{}{"content": "matrix needle evidence", "eligible": i%2 == 0}, ChunkEmbeddings: [][]float32{{1, float32(i) / 20, 0}}})
		require.NoError(t, err)
	}
	for i := 0; i < 12; i++ {
		for _, distance := range []int{1, 2} {
			require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("matrix-edge-%d-%d", i, distance)), StartNode: storage.NodeID(fmt.Sprintf("matrix-%d", i)), EndNode: storage.NodeID(fmt.Sprintf("matrix-%d", (i+distance)%12)), Type: "MATRIX"}))
		}
	}
	executor, err := s.getExecutorForDatabase("nornic")
	require.NoError(t, err)
	service := search.NewServiceWithDimensions(store, 3)
	t.Cleanup(func() { service.Close() })
	require.NoError(t, service.BuildIndexes(context.Background()))
	executor.SetSearchService(service)
	router := s.buildRouter()
	var remoteBytes atomic.Int64
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/db/matrix_remote/") {
			w = review1601CountingWriter{ResponseWriter: w, remoteBytes: &remoteBytes}
		}
		router.ServeHTTP(w, r)
	}))
	t.Cleanup(host.Close)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	boltAuth := bolt.NewAuthenticatorAdapter(authenticator)
	boltAuth.SetGetEffectivePermissions(s.GetEffectivePermissions)
	boltServer := bolt.NewWithDatabaseManager(&bolt.Config{ReadBufferSize: 8192, WriteBufferSize: 8192, Authenticator: boltAuth, RequireAuth: true}, nil, s.dbManager)
	boltServer.SetDatabaseAccessModeResolver(s.GetDatabaseAccessModeForRoles)
	boltServer.SetResolvedAccessResolver(s.GetResolvedAccessForRoles)
	boltServer.SetProcedureGrantResolver(s.ProcedureGrantChecker)
	serveError := make(chan error, 1)
	go func() { serveError <- boltServer.Serve(listener) }()
	t.Cleanup(func() { require.NoError(t, boltServer.Close()); require.NoError(t, <-serveError) })
	require.NoError(t, s.dbManager.CreateDatabase("matrix_use"))
	require.NoError(t, s.dbManager.CreateDatabase("matrix_remote"))
	require.NoError(t, s.dbManager.CreateCompositeDatabase("matrix_cmp", []multidb.ConstituentRef{
		{Alias: "local", DatabaseName: "nornic", Type: "local", AccessMode: "read"},
		{Alias: "remote", DatabaseName: "matrix_remote", Type: "remote", AccessMode: "read", URI: host.URL, AuthMode: "oidc_forwarding"},
		{Alias: "bolt", DatabaseName: "matrix_remote", Type: "remote", AccessMode: "write", URI: "bolt://" + listener.Addr().String(), AuthMode: "oidc_forwarding"},
	}))
	token := getAuthToken(t, authenticator, "admin")
	authenticate := func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) }
	_, created := nativeReviewCall(t, host, "/mcp", map[string]interface{}{"query": "CALL gds.graph.project('mcp-producer-matrix','MatrixDoc','MATRIX')"}, authenticate)
	require.False(t, created.IsError, created.Content)
	t.Cleanup(func() {
		_, _ = executor.Execute(context.Background(), "CALL gds.graph.drop('mcp-producer-matrix')", nil)
	})
	producers := []struct{ name, database, body, projection string }{
		{"local", "nornic", "UNWIND range(1,200000) AS x", "x"},
		{"local_call", "nornic", "CALL { UNWIND range(1,200000) AS x RETURN x }", "x"},
		{"local_correlated_call", "nornic", "UNWIND range(1,200000) AS y CALL { WITH y RETURN y AS x }", "x"},
		{"local_call_union", "nornic", "CALL { UNWIND range(1,200000) AS x RETURN x UNION ALL RETURN 0 AS x }", "x"},
		{"local_union", "nornic", "", "x"},
		{"use", "nornic", "USE matrix_use UNWIND range(1,200000) AS x", "x"},
		{"identifier_rewrite", "nornic", "UNWIND range(1,200000) AS `x`", "`x`"},
		{"label_rewrite", "nornic", "MATCH (n:MatrixDoc&MatrixDoc)", "n.content"},
		{"profile", "nornic", "PROFILE UNWIND range(1,200000) AS x", "x"},
		{"transaction", "nornic", "BEGIN UNWIND range(1,200000) AS x", "x"},
		{"apoc_literal", "nornic", "CALL apoc.cypher.run('UNWIND range(1,200000) AS x RETURN x',{}) YIELD value", "value.x"},
		{"apoc_bound", "nornic", "CALL apoc.cypher.run($statement,{}) YIELD value", "value.x"},
		{"apoc_many", "nornic", "CALL apoc.cypher.runMany('UNWIND range(1,2000) AS x RETURN x',{}, {statistics:false}) YIELD row, result", "row"},
		{"retrieval", "nornic", "CALL db.retrieve($request) YIELD node, score", "score"},
		{"gds_louvain", "nornic", "CALL gds.louvain.stream('mcp-producer-matrix') YIELD nodeId, communityId", "communityId"},
		{"gds_fastrp", "nornic", "CALL gds.fastRP.stream('mcp-producer-matrix',{embeddingDimension:4,randomSeed:42}) YIELD nodeId, embedding", "embedding"},
		{"gds_similarity", "nornic", "CALL gds.nodeSimilarity.stream('mcp-producer-matrix',{topK:10,similarityCutoff:0}) YIELD node1, node2, similarity", "similarity"},
		{"gds_prediction", "nornic", "CALL gds.linkPrediction.commonNeighbors.stream('mcp-producer-matrix',{sourceNode:'matrix-0',topK:10}) YIELD node1, node2, score", "score"},
		{"fabric_local", "matrix_cmp", "CALL { USE matrix_cmp.local UNWIND range(1,200000) AS x RETURN x }", "x"},
		{"remote", "matrix_cmp", "USE matrix_cmp.remote UNWIND range(1,200000) AS x", "x"},
		{"bolt", "matrix_cmp", "USE matrix_cmp.bolt UNWIND range(1,200000) AS x", "x"},
		{"bolt_basic", "matrix_cmp", "USE matrix_cmp.bolt UNWIND range(1,200000) AS x", "x"},
		{"bolt_profile", "matrix_cmp", "PROFILE USE matrix_cmp.bolt UNWIND range(1,200000) AS x", "x"},
		{"bolt_apoc", "matrix_cmp", "USE matrix_cmp.bolt CALL apoc.cypher.run('UNWIND range(1,200000) AS x RETURN x',{}) YIELD value", "value.x"},
		{"fabric_bolt", "matrix_cmp", "CALL { USE matrix_cmp.bolt UNWIND range(1,200000) AS x RETURN x } WITH x WHERE x > 20", "x"},
		{"fabric_remote", "matrix_cmp", "CALL { USE matrix_cmp.remote UNWIND range(1,200000) AS x RETURN x }", "x"},
		{"fabric_filtered", "matrix_cmp", "CALL { USE matrix_cmp.remote UNWIND range(1,200000) AS x RETURN x } WITH x WHERE x > 20", "x"},
		{"fabric_union", "matrix_cmp", "", "x"},
	}
	parameters := map[string]interface{}{
		"statement": "UNWIND range(1,200000) AS x RETURN x",
		"request":   map[string]interface{}{"query": "matrix needle", "embedding": []float64{1, 0, 0}, "labels": []string{"MatrixDoc"}, "where": "node.eligible = $eligible", "limit": 8, "min_similarity": 0.0},
		"eligible":  true,
	}
	for _, producer := range producers {
		for _, path := range []string{"/mcp", "/mcp/tools/call"} {
			for _, kind := range []string{"rows", "bytes"} {
				t.Run(producer.name+path+"/"+kind, func(t *testing.T) {
					projection := producer.projection
					if kind == "bytes" {
						projection = "$payload"
						parameters["payload"] = strings.Repeat("x", 600000)
					} else {
						delete(parameters, "payload")
					}
					query := producer.body + " RETURN " + projection + " AS selected"
					if producer.name == "fabric_union" {
						query = "USE matrix_cmp.local UNWIND range(1,200000) AS x RETURN " + projection + " AS selected UNION ALL USE matrix_cmp.remote UNWIND range(1,200000) AS x RETURN " + projection + " AS selected"
					}
					if producer.name == "local_union" {
						query = "UNWIND range(1,200000) AS x RETURN " + projection + " AS selected UNION ALL UNWIND [0] AS x RETURN " + projection + " AS selected"
					}
					if producer.name == "transaction" {
						query += " COMMIT"
					}
					runtime.GC()
					var before, after runtime.MemStats
					runtime.ReadMemStats(&before)
					remoteBytes.Store(0)
					credential := authenticate
					if producer.name == "bolt_basic" {
						credential = func(r *http.Request) { r.SetBasicAuth("admin", "password123") }
					}
					raw, response := nativeReviewCall(t, host, path, map[string]interface{}{"database": producer.database, "query": query, "parameters": parameters, "max_rows": 1}, credential)
					runtime.ReadMemStats(&after)
					require.False(t, response.IsError, response.Content)
					require.LessOrEqual(t, len(raw), 1<<20)
					var result struct {
						Rows       [][]interface{} `json:"rows"`
						Truncation struct {
							Reasons []string `json:"reasons"`
						} `json:"truncation"`
					}
					require.NoError(t, json.Unmarshal([]byte(response.Content[0].Text), &result))
					if kind == "rows" {
						require.Len(t, result.Rows, 1)
						require.Contains(t, result.Truncation.Reasons, "max_rows")
						if producer.name == "fabric_filtered" {
							require.EqualValues(t, 21, result.Rows[0][0], "coordinator filtering must precede final output admission")
						}
					} else {
						require.Empty(t, result.Rows)
						require.Contains(t, result.Truncation.Reasons, "max_bytes")
					}
					// Includes both peers for the remote cases, not just local retention.
					ceiling := uint64(16 << 20)
					// Byte cases include encoding/decoding the 600 kB bound parameter
					// in both peers and its row/REST protocol representations.
					if kind == "bytes" {
						ceiling = 64 << 20
					}
					require.Less(t, after.TotalAlloc-before.TotalAlloc, ceiling, "producer buffered before the final execution sink")
					if kind == "rows" {
						require.Less(t, remoteBytes.Load(), int64(1<<20))
					}
					t.Logf("producer=%s bound=%s allocation=%d remote_bytes=%d", producer.name, kind, after.TotalAlloc-before.TotalAlloc, remoteBytes.Load())
				})
			}
		}
	}
	for _, path := range []string{"/mcp", "/mcp/tools/call"} {
		_, plan := nativeReviewCall(t, host, path, map[string]interface{}{"query": "EXPLAIN UNWIND range(1,200000000) AS x RETURN x", "max_rows": 1}, authenticate)
		require.False(t, plan.IsError, plan.Content)
		require.Contains(t, plan.Content[0].Text, `"rows":[]`)
		require.Contains(t, plan.Content[0].Text, "plan")
		for _, query := range []string{
			"UNWIND range(1,200000) AS x RETURN x ORDER BY x",
			"UNWIND range(1,2000000) AS x RETURN collect(x)",
			"UNWIND range(1,200000) AS x CALL { WITH x RETURN x AS y } RETURN y ORDER BY y",
			"CALL { USE matrix_cmp.remote UNWIND range(1,200000) AS x RETURN x } RETURN x ORDER BY x",
			"CALL { USE matrix_cmp.bolt UNWIND range(1,200000) AS x RETURN x } RETURN x ORDER BY x",
		} {
			database := "nornic"
			if strings.Contains(query, "matrix_cmp.") {
				database = "matrix_cmp"
			}
			_, failed := nativeReviewCall(t, host, path, map[string]interface{}{"database": database, "query": query, "max_rows": 1}, authenticate)
			require.True(t, failed.IsError, failed.Content)
			require.Contains(t, failed.Content[0].Text, "db.memory.transaction.max threshold reached")
			global, databaseUsage := cypher.TransactionMemoryPoolUsage(database)
			require.Zero(t, global)
			require.Zero(t, databaseUsage)
		}
		_, written := nativeReviewCall(t, host, path, map[string]interface{}{"database": "matrix_cmp", "query": "USE matrix_cmp.bolt UNWIND range(1,40) AS x CREATE (:BoltBudgetWrite {x:x}) RETURN x", "max_rows": 1}, authenticate)
		require.False(t, written.IsError, written.Content)
		require.Contains(t, written.Content[0].Text, "max_rows")
	}
	_, counted := nativeReviewCall(t, host, "/mcp", map[string]interface{}{"database": "matrix_remote", "query": "MATCH (n:BoltBudgetWrite) RETURN count(n) AS count"}, authenticate)
	require.False(t, counted.IsError, counted.Content)
	require.Contains(t, counted.Content[0].Text, `"rows":[[80]]`)
}

func TestMCPNativeProducerCompletionAndReplay(t *testing.T) {
	s, authenticator := setupTestServer(t)
	host := httptest.NewServer(s.buildRouter())
	t.Cleanup(host.Close)
	token := getAuthToken(t, authenticator, "admin")
	authenticate := func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) }
	for _, path := range []string{"/mcp", "/mcp/tools/call"} {
		_, replayed := nativeReviewCall(t, host, path, map[string]interface{}{
			"query": "UNWIND [1,2,3] AS x CALL apoc.cypher.run('RETURN rand() AS v',{}) YIELD value RETURN value.v AS v", "max_rows": 10,
		}, authenticate)
		require.False(t, replayed.IsError, replayed.Content)
		var result struct {
			Rows [][]interface{} `json:"rows"`
		}
		require.NoError(t, json.Unmarshal([]byte(replayed.Content[0].Text), &result))
		require.Len(t, result.Rows, 3)
		require.Equal(t, result.Rows[0], result.Rows[1])
		require.Equal(t, result.Rows[0], result.Rows[2])
		_, written := nativeReviewCall(t, host, path, map[string]interface{}{
			"query":    "UNWIND range(1,40) AS x CALL apoc.cypher.run('RETURN 1 AS y',{}) YIELD value CALL apoc.cypher.run($statement,{x:x}) YIELD value AS written RETURN written",
			"max_rows": 1, "parameters": map[string]interface{}{"statement": "CREATE (:PipelineCompletion {x:$x}) RETURN $x AS x"},
		}, authenticate)
		require.False(t, written.IsError, written.Content)
		require.Contains(t, written.Content[0].Text, "max_rows")
	}
	_, counted := nativeReviewCall(t, host, "/mcp", map[string]interface{}{"query": "MATCH (n:PipelineCompletion) RETURN count(n) AS count"}, authenticate)
	require.False(t, counted.IsError, counted.Content)
	require.Contains(t, counted.Content[0].Text, `"rows":[[80]]`)
}

func TestMCPNativeProducerBatchTransactions(t *testing.T) {
	s, authenticator := setupTestServer(t)
	host := httptest.NewServer(s.buildRouter())
	t.Cleanup(host.Close)
	token := getAuthToken(t, authenticator, "admin")
	authenticate := func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) }
	_, constraint := nativeReviewCall(t, host, "/mcp", map[string]interface{}{"query": "CREATE CONSTRAINT mcp_batch_partial_key FOR (n:BoundBatchPartial) REQUIRE n.key IS UNIQUE"}, authenticate)
	require.False(t, constraint.IsError, constraint.Content)
	for _, path := range []string{"/mcp", "/mcp/tools/call"} {
		_, read := nativeReviewCall(t, host, path, map[string]interface{}{
			"query": "UNWIND range(1,200000) AS x CALL { WITH x RETURN x AS y } IN TRANSACTIONS OF 7 ROWS RETURN y", "max_rows": 1,
		}, authenticate)
		require.False(t, read.IsError, read.Content)
		require.Contains(t, read.Content[0].Text, `"rows":[[1]]`)
		require.Contains(t, read.Content[0].Text, "max_rows")
		_, wide := nativeReviewCall(t, host, path, map[string]interface{}{
			"query": "UNWIND range(1,200000) AS x CALL { WITH x RETURN x AS y } IN TRANSACTIONS OF 1 ROWS RETURN $payload AS selected", "max_rows": 1,
			"parameters": map[string]interface{}{"payload": strings.Repeat("x", 600000)},
		}, authenticate)
		require.False(t, wide.IsError, wide.Content)
		require.Contains(t, wide.Content[0].Text, `"rows":[]`)
		require.Contains(t, wide.Content[0].Text, "max_bytes")
		_, written := nativeReviewCall(t, host, path, map[string]interface{}{
			"query": "UNWIND range(1,40) AS x CALL { WITH x CREATE (:BoundBatchWrite {x:x}) RETURN x AS y } IN TRANSACTIONS OF 7 ROWS RETURN y", "max_rows": 1,
		}, authenticate)
		require.False(t, written.IsError, written.Content)
		require.Contains(t, written.Content[0].Text, "max_rows")
		_, failed := nativeReviewCall(t, host, path, map[string]interface{}{
			"query": "UNWIND range(1,14) AS x CALL { WITH x CREATE (:BoundBatchPartial {key:$lane + toString(CASE WHEN x > 7 THEN 1 ELSE x END)}) RETURN x AS y } IN TRANSACTIONS OF 7 ROWS RETURN y", "max_rows": 1,
			"parameters": map[string]interface{}{"lane": path},
		}, authenticate)
		require.True(t, failed.IsError, failed.Content)
		require.Contains(t, failed.Content[0].Text, "BoundBatchPartial")
	}
	for _, expected := range []struct{ label, count string }{{"BoundBatchWrite", "80"}, {"BoundBatchPartial", "14"}} {
		_, result := nativeReviewCall(t, host, "/mcp", map[string]interface{}{"query": "MATCH (n:" + expected.label + ") RETURN count(n) AS count"}, authenticate)
		require.False(t, result.IsError, result.Content)
		require.Contains(t, result.Content[0].Text, `"rows":[[`+expected.count+`]]`)
	}
}
