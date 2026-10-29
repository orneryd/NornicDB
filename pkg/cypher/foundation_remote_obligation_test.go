package cypher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	remoteFixture "github.com/orneryd/nornicdb/internal/testfixture/remotequery"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A real public peer executor serves the wire. The carried descriptor is the
// peer's actual owner, not fabricated flags or a transport pretending to own it.
func FoundationIssueRemoteFixture(t *testing.T) FoundationPublicFixture {
	f := FoundationIssuePublicFixture(t, "transactionStorageWrapper", nil)
	issued := f.Engine

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Statements []struct {
				Statement  string                 `json:"statement"`
				Parameters map[string]interface{} `json:"parameters"`
			} `json:"statements"`
		}
		err := json.NewDecoder(r.Body).Decode(&request)
		results := []interface{}{}
		failures := []interface{}{}
		if err == nil {
			for _, s := range request.Statements {
				result, readErr := NewStorageExecutor(issued).Execute(r.Context(), s.Statement, s.Parameters)
				if readErr != nil {
					err = readErr
					break
				}
				data := []interface{}{}
				for _, row := range result.Rows {
					data = append(data, map[string]interface{}{"row": row})
				}
				results = append(results, map[string]interface{}{"columns": result.Columns, "data": data, "query_type": result.Metadata[resolvedStatementModeMetadata]})
			}
		}
		if err != nil {
			failures = append(failures, map[string]interface{}{"code": "Neo.DatabaseError.Statement.ExecutionFailed", "message": err.Error()})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"results": results, "errors": failures})
	}))
	t.Cleanup(server.Close)
	remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: server.URL, Database: "tenant"})
	require.NoError(t, err)
	remote = remote.WithMembership(issued.(storage.MembershipProvider))
	t.Cleanup(func() { require.NoError(t, remote.Close()) })
	f.Engine = remote
	f.Execute = func(ctx context.Context, query string) (*ExecuteResult, error) {
		columns, rows, err := remote.QueryCypher(ctx, query, nil, storage.RemoteAccessWrite)
		return &ExecuteResult{Columns: columns, Rows: rows}, err
	}
	return f
}

// Author: Lane #1644 (Codex). Reuse the actual remote peer and carried native
// owner; the observer itself must reach every public logical consumer.
func FoundationIssueRemoteObserverFixture(t *testing.T) FoundationPublicFixture {
	f := FoundationIssueRemoteFixture(t)
	observed := &remoteFixture.Observer{RemoteEngine: f.Engine.(*storage.RemoteEngine)}
	f.Engine = observed
	f.Execute = func(ctx context.Context, query string) (*ExecuteResult, error) {
		columns, rows, err := observed.QueryCypher(ctx, query, nil, storage.RemoteAccessWrite)
		require.Positive(t, observed.Calls)
		require.Equal(t, query, observed.Source)
		return &ExecuteResult{Columns: columns, Rows: rows}, err
	}
	return f
}
