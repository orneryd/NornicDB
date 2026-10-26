// Author: Codex,NOR-180 GraphQL outcome contract,29October2026.
package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGraphQLWALTerminalOutcomeValueViews(t *testing.T) {
	for _, profile := range walterminal.ContractProfiles {
		for _, parallel := range []bool{false, true} {
			for _, closed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/parallel=%v/closed=%v", profile.Name, parallel, closed), func(t *testing.T) {
					outer := walterminal.New(t)
					c := walterminal.NewOutcomeContract(t, profile, closed, parallel)
					manager, err := multidb.NewDatabaseManager(context.Background(), outer, nil)
					require.NoError(t, err)
					h := newHandler(nil, manager, nil)
					query := fmt.Sprintf("mutation { executeCypher(input: {statement: %q}) { columns rowCount } }", c.Flow.Query)
					payload, err := json.Marshal(map[string]string{"query": query})
					require.NoError(t, err)
					request := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(payload))
					request.Header.Set("Content-Type", "application/json")
					response := httptest.NewRecorder()
					h.ServeHTTP(response, request)
					require.Equal(t, http.StatusOK, response.Code)
					c.RequireNative(t)
					var wire struct {
						Errors []struct{ Message string } `json:"errors"`
					}
					require.NoError(t, json.Unmarshal(response.Body.Bytes(), &wire))
					if !closed {
						require.Empty(t, wire.Errors)
						return
					}
					require.NotEmpty(t, wire.Errors)
					c.RequireView(t, "", wire.Errors[0].Message)
				})
			}
		}
	}
}
