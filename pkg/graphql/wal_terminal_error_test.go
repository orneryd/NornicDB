// Author: Codex, NOR-180 GraphQL presenter error-flow regression, 2026-10-27.
package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	gql "github.com/99designs/gqlgen/graphql"
	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

var walGraphQLProcedureID atomic.Uint64

func TestGraphQLWALTerminalContainedPrimary(t *testing.T) {
	for _, projected := range []bool{false, true} {
		for _, operation := range []string{"query", "mutation"} {
			for _, closed := range []bool{false, true} {
				t.Run(fmt.Sprintf("projected=%v/%s/closed=%v", projected, operation, closed), func(t *testing.T) {
					engine := walterminal.New(t)
					manager, err := multidb.NewDatabaseManager(context.Background(), engine, nil)
					require.NoError(t, err)
					h := newHandler(nil, manager, nil)
					primaryCause := errors.New("NOR-180 GraphQL primary cause")
					primary := &gqlerror.Error{Err: primaryCause, Message: "NOR-180 GraphQL primary", Extensions: map[string]interface{}{"code": "NOR180.Primary", "detail": "keep metadata"}}
					var returnedPrimary error = primary
					if projected {
						returnedPrimary = localization.ProjectError(primary, "NOR-180 projected GraphQL primary", nil, nil)
					}
					name := fmt.Sprintf("nor180.graphql%d", walGraphQLProcedureID.Add(1))
					require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: name, Signature: name + "() :: (value :: INTEGER)", Mode: cypher.ProcedureModeWrite, Returns: []cypher.ProcedureColumn{{Name: "value", Type: "INTEGER"}}}, func(ctx context.Context, exec *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
						if _, err := exec.Execute(ctx, "CREATE (:WalWire)", nil); err != nil {
							return nil, err
						}
						if closed {
							if err := engine.WAL.Close(); err != nil {
								return nil, err
							}
						}
						return nil, returnedPrimary
					}))
					var presented *gqlerror.Error
					h.server.AroundResponses(func(ctx context.Context, next gql.ResponseHandler) *gql.Response {
						response := next(ctx)
						if len(response.Errors) > 0 {
							presented = response.Errors[0]
						}
						return response
					})
					field := "cypher"
					if operation == "mutation" {
						field = "executeCypher"
					}
					statement := "CALL " + name + "() YIELD value RETURN value"
					query := fmt.Sprintf("%s { %s(input: {statement: %q}) { columns rowCount } }", operation, field, statement)
					payload, err := json.Marshal(map[string]string{"query": query})
					require.NoError(t, err)
					request := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(payload))
					request.Header.Set("Content-Type", "application/json")
					response := httptest.NewRecorder()
					h.ServeHTTP(response, request)
					require.Equal(t, http.StatusOK, response.Code)
					var wire struct {
						Errors []struct {
							Message    string                 `json:"message"`
							Extensions map[string]interface{} `json:"extensions"`
						} `json:"errors"`
					}
					require.NoError(t, json.Unmarshal(response.Body.Bytes(), &wire))
					require.Len(t, wire.Errors, 1)
					primaryMessage := primary.Message
					if projected && closed {
						primaryMessage = "NOR-180 projected GraphQL primary"
					}
					require.Contains(t, wire.Errors[0].Message, primaryMessage)
					require.Equal(t, primary.Extensions, wire.Errors[0].Extensions)
					require.ErrorIs(t, presented, primaryCause)
					if closed {
						require.Contains(t, wire.Errors[0].Message, storage.ErrWALClosed.Error())
						require.Equal(t, 1, strings.Count(wire.Errors[0].Message, "failed to write WAL tx abort"))
						require.ErrorIs(t, presented, storage.ErrWALClosed)
						var diagnostic interface{ StatusDiagnostic() localization.Message }
						require.ErrorAs(t, presented, &diagnostic)
					} else {
						require.NotContains(t, wire.Errors[0].Message, "WAL tx abort")
					}
					require.Zero(t, walterminal.Count(t, engine))
				})
			}
		}
	}
}

// The other presenter branch already contains the complete joined text. Public
// timeout/memory failures must keep it once, rather than append a second copy.
func TestGraphQLWALTerminalPrimaryMessages(t *testing.T) {
	for _, kind := range []string{"timeout", "memory"} {
		for _, closed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/closed=%v", kind, closed), func(t *testing.T) {
				engine := walterminal.New(t)
				manager, err := multidb.NewDatabaseManager(context.Background(), engine, nil)
				require.NoError(t, err)
				h := newHandler(nil, manager, nil)
				primary := walterminal.Primary(kind)
				name := fmt.Sprintf("nor180.graphql%d", walGraphQLProcedureID.Add(1))
				require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: name, Signature: name + "() :: (value :: INTEGER)", Mode: cypher.ProcedureModeWrite, Returns: []cypher.ProcedureColumn{{Name: "value", Type: "INTEGER"}}}, func(ctx context.Context, exec *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
					if _, err := exec.Execute(ctx, "CREATE (:WalWire)", nil); err != nil {
						return nil, err
					}
					if closed {
						if err := engine.WAL.Close(); err != nil {
							return nil, err
						}
					}
					return nil, primary
				}))
				var presented *gqlerror.Error
				h.server.AroundResponses(func(ctx context.Context, next gql.ResponseHandler) *gql.Response {
					response := next(ctx)
					if len(response.Errors) > 0 {
						presented = response.Errors[0]
					}
					return response
				})
				query := fmt.Sprintf("mutation { executeCypher(input: {statement: %q}) { columns rowCount } }", "CALL "+name+"() YIELD value RETURN value")
				encoded, err := json.Marshal(map[string]string{"query": query})
				require.NoError(t, err)
				payload := string(encoded)
				request := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(payload))
				request.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				h.ServeHTTP(response, request)
				require.Equal(t, http.StatusOK, response.Code)
				var wire struct {
					Errors []struct {
						Message string `json:"message"`
					} `json:"errors"`
				}
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &wire))
				require.Len(t, wire.Errors, 1)
				require.Contains(t, wire.Errors[0].Message, primary.Error())
				if kind == "timeout" {
					require.ErrorIs(t, presented, context.DeadlineExceeded)
				} else {
					require.ErrorIs(t, presented, memory.ErrLimitExceeded)
				}
				if closed {
					require.ErrorIs(t, presented, storage.ErrWALClosed)
					require.Equal(t, 1, strings.Count(wire.Errors[0].Message, "failed to write WAL tx abort"))
				} else {
					require.NotContains(t, wire.Errors[0].Message, "WAL tx abort")
				}
				require.Zero(t, walterminal.Count(t, engine))
			})
		}
	}
}
