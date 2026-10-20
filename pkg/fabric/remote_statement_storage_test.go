package fabric

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoteStatementStorageSharesParticipant(t *testing.T) {
	var opened, queries, committed, rolledBack atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/db/tenant/tx":
			opened.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"results": []interface{}{}, "errors": []interface{}{}, "commit": server.URL + "/db/tenant/tx/1/commit"})
		case request.Method == http.MethodPost && request.URL.Path == "/db/tenant/tx/1":
			queries.Add(1)
			var body struct {
				Statements []struct{ Statement string } `json:"statements"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode statement: %v", err)
				http.Error(w, "invalid statement", http.StatusBadRequest)
				return
			}
			var value interface{} = map[string]interface{}{"id": "shared", "labels": []string{"N"}, "properties": map[string]interface{}{"v": 7}}
			if len(body.Statements) == 1 && strings.Contains(body.Statements[0].Statement, "RETURN count(n)") {
				value = 1
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"results": []interface{}{map[string]interface{}{
					"columns": []string{"n"},
					"data":    []interface{}{map[string]interface{}{"row": []interface{}{value}}},
					"stats":   map[string]interface{}{"query_type": "r"},
				}},
				"errors": []interface{}{},
			})
		case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/commit"):
			committed.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"results": []interface{}{}, "errors": []interface{}{}})
		case request.Method == http.MethodDelete:
			rolledBack.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"results": []interface{}{}, "errors": []interface{}{}})
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)
	executor := NewRemoteFragmentExecutor()
	t.Cleanup(func() { require.NoError(t, executor.Close()) })
	location := &LocationRemote{URI: server.URL, DBName: "tenant"}
	transaction := NewFabricTransaction("borrowed-source")
	sub, err := transaction.GetOrOpenLocation(location, true)
	require.NoError(t, err)
	statement, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ctx := WithSubTransaction(WithFabricTransaction(WithTransactionLifetime(statement, context.Background()), transaction), sub)
	view, err := executor.StatementStorage(ctx, location, "")
	require.NoError(t, err)
	node, err := view.GetNode("shared")
	require.NoError(t, err)
	require.Equal(t, "shared", string(node.ID))
	node.Properties["changed"] = true
	require.NoError(t, view.UpdateNode(node))
	_, err = executor.Execute(ctx, location, "RETURN 1", nil, "")
	require.NoError(t, err)
	require.NoError(t, view.Close(), "a statement view borrows the physical transport")
	_, err = executor.Execute(ctx, location, "RETURN 2", nil, "")
	require.NoError(t, err)
	require.Equal(t, int32(4), queries.Load())
	cancel()
	_, err = view.GetNode("shared")
	require.ErrorIs(t, err, context.Canceled, "storage calls retain the current statement context")
	require.Equal(t, int32(4), queries.Load())
	require.NoError(t, transaction.Commit(nil, nil), "participant completion retains its own lifetime")
	require.NoError(t, executor.Close())
	require.Equal(t, int32(1), opened.Load())
	require.Equal(t, int32(1), committed.Load())
	require.Zero(t, rolledBack.Load())
}
