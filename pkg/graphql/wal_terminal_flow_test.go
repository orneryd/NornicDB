// Author: Codex, NOR-180 producer-outward GraphQL regressions, 2026-10-27.
package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	gql "github.com/99designs/gqlgen/graphql"
	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func TestGraphQLWALTerminalProducerFlows(t *testing.T) {
	for _, family := range walterminal.FlowFamilies {
		for _, outcome := range walterminal.FlowOutcomes {
			for _, closed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/closed=%v", family, outcome, closed), func(t *testing.T) {
					engine := walterminal.New(t)
					flow := walterminal.NewOutcomeFlow(t, engine, family, outcome, closed)
					manager, err := multidb.NewDatabaseManager(context.Background(), engine, nil)
					require.NoError(t, err)
					h := newHandler(nil, manager, nil)
					var presented *gqlerror.Error
					h.server.AroundResponses(func(ctx context.Context, next gql.ResponseHandler) *gql.Response {
						response := next(ctx)
						if len(response.Errors) > 0 {
							presented = response.Errors[0]
						}
						return response
					})
					query := fmt.Sprintf("mutation { executeCypher(input: {statement: %q}) { columns rowCount } }", flow.Query)
					payload, err := json.Marshal(map[string]string{"query": query})
					require.NoError(t, err)
					request := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(payload))
					request.Header.Set("Content-Type", "application/json")
					response := httptest.NewRecorder()
					h.ServeHTTP(response, request)
					require.Equal(t, http.StatusOK, response.Code)
					var wire struct {
						Errors []struct{ Message string } `json:"errors"`
					}
					require.NoError(t, json.Unmarshal(response.Body.Bytes(), &wire))
					require.Equal(t, flow.CommittedNodes, walterminal.Count(t, engine))
					if !flow.WantError {
						require.Empty(t, wire.Errors)
						return
					}
					require.NotEmpty(t, wire.Errors, "failed logical terminal cannot become a GraphQL success/count response")
					if flow.Primary != nil {
						require.ErrorIs(t, presented, flow.Primary)
						require.Contains(t, wire.Errors[0].Message, flow.Primary.Error())
					}
					if closed {
						require.ErrorIs(t, presented, storage.ErrWALClosed)
						require.Contains(t, wire.Errors[0].Message, storage.ErrWALClosed.Error())
						if outcome == "commit" {
							walterminal.RequireCommitMarkerFailure(t, wire.Errors[0].Message)
						}
					}
				})
			}
		}
	}
}

func TestGraphQLWALTerminalProducerMultipleOutcomes(t *testing.T) {
	outer := walterminal.New(t)
	engines := []*walterminal.Engine{walterminal.New(t), walterminal.New(t)}
	flow := walterminal.NewMultipleOutcomeFlow(t, engines)
	manager, err := multidb.NewDatabaseManager(context.Background(), outer, nil)
	require.NoError(t, err)
	h := newHandler(nil, manager, nil)
	var presented *gqlerror.Error
	h.server.AroundResponses(func(ctx context.Context, next gql.ResponseHandler) *gql.Response {
		response := next(ctx)
		if len(response.Errors) > 0 {
			presented = response.Errors[0]
		}
		return response
	})
	query := fmt.Sprintf("mutation { executeCypher(input: {statement: %q}) { columns rowCount } }", flow.Query)
	payload, err := json.Marshal(map[string]string{"query": query})
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	h.ServeHTTP(response, request)
	var wire struct {
		Errors []struct{ Message string } `json:"errors"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &wire))
	require.NotEmpty(t, wire.Errors)
	require.Equal(t, 2, strings.Count(wire.Errors[0].Message, "failed to write WAL tx abort"))
	require.ErrorIs(t, presented, context.DeadlineExceeded)
	require.ErrorIs(t, presented, memory.ErrLimitExceeded)
	require.ErrorIs(t, presented, storage.ErrWALClosed)
	for _, engine := range engines {
		require.Zero(t, walterminal.Count(t, engine))
	}
}
