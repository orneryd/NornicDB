package server

// Untracked probe helper (earlier-issues batch 1): runs a check through the
// real Bolt listener (auto-commit and explicit transaction) and the HTTP
// transaction endpoint of one test server.

import (
	"encoding/json"
	"net"
	"net/http"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/stretchr/testify/require"
)

type ei1HTTPError struct{ detail string }

func (e *ei1HTTPError) Error() string { return e.detail }

func ei1BoltConfig(s *Server) *bolt.Config {
	return &bolt.Config{ReadBufferSize: 8192, WriteBufferSize: 8192, DatabaseExecutorOptions: s.DatabaseScopedExecutorOptions}
}

func ei1Routes(t *testing.T, check func(*testing.T, func(string, map[string]any) ([][]any, error))) {
	for _, route := range []string{"bolt_auto", "bolt_explicit", "http"} {
		t.Run(route, func(t *testing.T) {
			s, authenticator := setupTestServer(t)
			if route == "http" {
				token := "Bearer " + getAuthToken(t, authenticator, "admin")
				run := func(query string, params map[string]any) ([][]any, error) {
					response := makeRequest(t, s, http.MethodPost, "/db/"+s.dbManager.DefaultDatabaseName()+"/tx/commit", map[string]any{"statements": []any{map[string]any{"statement": query, "parameters": params}}}, token)
					var payload TransactionResponse
					require.NoError(t, json.Unmarshal(response.Body.Bytes(), &payload))
					if len(payload.Errors) != 0 {
						b, _ := json.Marshal(payload.Errors)
						return nil, &ei1HTTPError{string(b)}
					}
					var rows [][]any
					for _, result := range payload.Results {
						for _, data := range result.Data {
							rows = append(rows, data.Row)
						}
					}
					return rows, nil
				}
				check(t, run)
				return
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			peer := bolt.NewWithDatabaseManager(ei1BoltConfig(s), nil, s.dbManager)
			done := make(chan error, 1)
			go func() { done <- peer.Serve(listener) }()
			t.Cleanup(func() { _ = peer.Close(); <-done })
			driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
			require.NoError(t, err)
			defer driver.Close(t.Context())
			session := driver.NewSession(t.Context(), neo4j.SessionConfig{DatabaseName: s.dbManager.DefaultDatabaseName()})
			defer session.Close(t.Context())
			run := func(query string, params map[string]any) ([][]any, error) {
				var result neo4j.ResultWithContext
				var err error
				var tx neo4j.ExplicitTransaction
				if route == "bolt_explicit" {
					tx, err = session.BeginTransaction(t.Context())
					if err != nil {
						return nil, err
					}
					defer tx.Rollback(t.Context())
				}
				if tx != nil {
					result, err = tx.Run(t.Context(), query, params)
				} else {
					result, err = session.Run(t.Context(), query, params)
				}
				if err != nil {
					return nil, err
				}
				records, err := result.Collect(t.Context())
				if err != nil {
					return nil, err
				}
				rows := make([][]any, len(records))
				for i, record := range records {
					rows[i] = record.Values
				}
				if tx != nil {
					err = tx.Commit(t.Context())
				}
				return rows, err
			}
			check(t, run)
		})
	}
}
