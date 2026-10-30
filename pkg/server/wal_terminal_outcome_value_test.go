// Author: Codex,NOR-180 HTTP outcome contract,29October2026.
// The generated composition population (per-child WAL) by Claude, 30 October 2026.
package server

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
)

func TestHTTPWALTerminalOutcomeValueViews(t *testing.T) {
	for _, profile := range walterminal.ContractProfiles {
		for _, parallel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/parallel=%v", profile.Name, parallel), func(t *testing.T) {
				_, _, request := walHTTPFixture(t)
				c := walterminal.NewOutcomeContract(t, profile, parallel)
				status, response := request(http.MethodPost, "/db/nornic/tx/commit", walHTTPStatements(c.Flow.Query))
				c.RequireNative(t)
				require.Equal(t, http.StatusOK, status)
				if !c.Flow.WantError {
					require.Empty(t, response.Errors)
					return
				}
				require.NotEmpty(t, response.Errors)
				c.RequireView(t, response.Errors[0].Code, walHTTPMessages(response))
			})
		}
	}
}
