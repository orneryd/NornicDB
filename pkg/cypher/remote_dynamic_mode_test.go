package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The HTTP transaction fixture executes real public Cypher in each pending
// remote transaction. Policies alter only the evidence returned at the genuine
// wire boundary, including older/foreign peers that report no mode or counters.
func newRemoteModeExecutor(t *testing.T, policies [2]string) (*StorageExecutor, [2]storage.Engine) {
	t.Helper()
	remote, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = remote.Close() })
	var stores [2]storage.Engine
	for i, name := range []string{"remote_one", "remote_two"} {
		require.NoError(t, remote.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: name}))
		stores[i], err = remote.GetStorage(name)
		require.NoError(t, err)
	}
	var mu sync.Mutex
	handles := make(map[string]*StorageExecutor)
	next := 0
	var endpoint *httptest.Server
	endpoint = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		reply := func(results []interface{}, err error) {
			errors := []interface{}{}
			if err != nil {
				errors = append(errors, map[string]interface{}{"code": "Neo.DatabaseError.Statement.ExecutionFailed", "message": err.Error()})
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"results": results, "errors": errors})
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) < 3 {
			http.NotFound(w, r)
			return
		}
		if len(parts) == 3 && r.Method == http.MethodPost {
			var openRequest struct {
				TransactionMetadata map[string]interface{} `json:"tx_metadata"`
			}
			if err := json.NewDecoder(r.Body).Decode(&openRequest); err != nil {
				reply(nil, err)
				return
			}
			store, err := remote.GetStorage(parts[1])
			if err != nil {
				reply(nil, err)
				return
			}
			exec := NewStorageExecutor(store)
			if _, err = exec.Execute(WithTransactionMetadata(r.Context(), openRequest.TransactionMetadata), "BEGIN", nil); err != nil {
				reply(nil, err)
				return
			}
			mu.Lock()
			next++
			id := strconv.Itoa(next)
			handles[id] = exec
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"results": []interface{}{}, "errors": []interface{}{}, "commit": endpoint.URL + "/db/" + parts[1] + "/tx/" + id + "/commit"})
			return
		}
		if len(parts) < 4 {
			http.NotFound(w, r)
			return
		}
		mu.Lock()
		exec := handles[parts[3]]
		mu.Unlock()
		if exec == nil {
			http.NotFound(w, r)
			return
		}
		if r.Method == http.MethodDelete || len(parts) == 5 && parts[4] == "commit" {
			command := "COMMIT"
			if r.Method == http.MethodDelete {
				command = "ROLLBACK"
			}
			_, err := exec.Execute(r.Context(), command, nil)
			mu.Lock()
			delete(handles, parts[3])
			mu.Unlock()
			reply([]interface{}{}, err)
			return
		}
		var request struct {
			Statements []struct {
				Statement    string                 `json:"statement"`
				Parameters   map[string]interface{} `json:"parameters"`
				IncludeStats bool                   `json:"includeStats"`
			} `json:"statements"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			reply(nil, err)
			return
		}
		results := []interface{}{}
		for _, statement := range request.Statements {
			result, err := exec.Execute(r.Context(), statement.Statement, statement.Parameters)
			if err != nil {
				reply(nil, err)
				return
			}
			data := []interface{}{}
			for _, row := range result.Rows {
				data = append(data, map[string]interface{}{"row": row})
			}
			item := map[string]interface{}{"columns": result.Columns, "data": data}
			policy := policies[0]
			if parts[1] == "remote_two" {
				policy = policies[1]
			}
			if policy != "missing" && statement.IncludeStats {
				updates := false
				if stats := result.Stats; stats != nil {
					updates = stats.NodesCreated+stats.NodesDeleted+stats.RelationshipsCreated+stats.RelationshipsDeleted+stats.PropertiesSet > 0
				}
				stats := map[string]interface{}{"contains_updates": updates}
				if policy == "owner" {
					if mode, ok := result.Metadata["resolved_query_type"]; ok {
						item["query_type"] = mode
					}
				}
				item["stats"] = stats
			}
			results = append(results, item)
		}
		reply(results, nil)
	}))
	t.Cleanup(func() {
		endpoint.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, exec := range handles {
			_, _ = exec.Execute(context.Background(), "ROLLBACK", nil)
		}
	})
	configuration := multidb.DefaultConfig()
	configuration.RemoteEngineFactory = func(ref multidb.ConstituentRef, token string) (storage.Engine, error) {
		return storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, AuthToken: token})
	}
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), configuration)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "remote_modes", []multidb.ConstituentRef{
		{Alias: "one", DatabaseName: "remote_one", Type: "remote", AccessMode: "read_write", URI: endpoint.URL},
		{Alias: "two", DatabaseName: "remote_two", Type: "remote", AccessMode: "read_write", URI: endpoint.URL},
	}))
	store, err := manager.GetStorage("remote_modes")
	require.NoError(t, err)
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	return exec, stores
}

func TestRemoteDynamicModeSummaryAdmission(t *testing.T) {
	cleanupAdmissionProcedure(t, "u5remote.noStats")
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "u5remote.noStats", Mode: ProcedureModeWrite, Returns: []ProcedureColumn{{Name: "v", Type: "INTEGER"}}}, func(ctx context.Context, exec *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
		if _, err := exec.Execute(ctx, "CREATE (:U5RemoteMode) RETURN 1 AS v", nil); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{int64(1)}}}, nil
	}))
	read := "RETURN 1 AS v"
	write := "CREATE (:U5RemoteMode) RETURN 1 AS v"
	noStats := "CALL u5remote.noStats() YIELD v RETURN v"
	for _, tc := range []struct {
		name, first, second string
		policy              [2]string
		reject              bool
		want                [2]int
	}{
		{"resolved reads", read, read, [2]string{"owner", "owner"}, false, [2]int{}},
		{"write then read", write, read, [2]string{"owner", "owner"}, false, [2]int{1, 0}},
		{"two resolved writers", write, write, [2]string{"owner", "owner"}, true, [2]int{}},
		{"periodic batches cannot escape second writer", "CALL apoc.periodic.iterate('RETURN 1 AS input','CREATE (:U5RemoteMode)',{batchSize:1}) YIELD total RETURN total", "CALL apoc.periodic.iterate('RETURN 1 AS input','CREATE (:U5RemoteMode)',{batchSize:1}) YIELD total RETURN total", [2]string{"owner", "owner"}, true, [2]int{}},
		{"standard updates", write, write, [2]string{"standard", "standard"}, true, [2]int{}},
		{"callbacks without counters", noStats, noStats, [2]string{"owner", "owner"}, true, [2]int{}},
		{"missing summaries", write, write, [2]string{"missing", "missing"}, true, [2]int{}},
		{"zero counters do not prove read", noStats, noStats, [2]string{"standard", "standard"}, true, [2]int{}},
		{"unknown then resolved read", write, read, [2]string{"missing", "owner"}, false, [2]int{1, 0}},
		{"foreign reads without resolved mode", read, read, [2]string{"standard", "standard"}, true, [2]int{}},
	} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit=%v", tc.name, explicit), func(t *testing.T) {
				exec, stores := newRemoteModeExecutor(t, tc.policy)
				ctx := context.Background()
				if explicit {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				query := "CALL { USE remote_modes.one CALL apoc.cypher.run($first,{}) YIELD value RETURN value } RETURN value UNION ALL CALL { USE remote_modes.two CALL apoc.cypher.run($second,{}) YIELD value RETURN value } RETURN value"
				result, err := exec.Execute(ctx, query, map[string]interface{}{"first": tc.first, "second": tc.second})
				if tc.reject {
					if tc.name == "periodic batches cannot escape second writer" {
						require.ErrorContains(t, err, "unsupported query type in transaction")
					} else {
						require.ErrorContains(t, err, "AccessMode")
					}
				} else {
					require.NoError(t, err)
					require.Len(t, result.Rows, 2)
				}
				if explicit {
					_, err = exec.Execute(ctx, "COMMIT", nil)
					if tc.reject {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
				}
				for i, store := range stores {
					nodes, err := store.GetNodesByLabel("U5RemoteMode")
					require.NoError(t, err)
					require.Len(t, nodes, tc.want[i])
				}
			})
		}
	}
}
