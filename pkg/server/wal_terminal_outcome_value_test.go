// Author: Codex,NOR-180 HTTP outcome contract,29October2026.
// The generated composition population (per-child WAL) and one shared server
// with per-cell independence checks by Claude, 30 October 2026.
package server

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
)

func TestHTTPWALTerminalOutcomeValueViews(t *testing.T) {
	// One server for the population: building it (and its login) per cell
	// dominated the run. Each cell has its own child engines and WALs; the
	// shared outer store only runs the cell's count-only procedure call.
	_, outer, request := walHTTPServer(t)
	for _, profile := range walterminal.ContractProfiles {
		for _, parallel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/parallel=%v", profile.Name, parallel), func(t *testing.T) {
				// Independence: no earlier cell left stored work, a session or
				// a retained outcome. A healthy statement succeeds first, and
				// the outer store holds nothing before and after the cell.
				status, healthy := request(t, http.MethodPost, "/db/nornic/tx/commit", walHTTPStatements("RETURN 1 AS ready"))
				require.Equal(t, http.StatusOK, status)
				require.Empty(t, healthy.Errors, "no earlier cell's outcome or WAL state is visible")
				require.Equal(t, int64(0), walterminal.Count(t, outer))
				c := walterminal.NewOutcomeContract(t, profile, parallel)
				status, response := request(t, http.MethodPost, "/db/nornic/tx/commit", walHTTPStatements(c.Flow.Query))
				require.Equal(t, int64(0), walterminal.Count(t, outer), "the cell's work stays in its own child engines")
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
