// Author: Codex, NOR-180 producer-outward HTTP regressions, 2026-10-27.
package server

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestHTTPWALTerminalProducerFlows(t *testing.T) {
	for _, family := range walterminal.FlowFamilies {
		for _, outcome := range walterminal.FlowOutcomes {
			for _, closed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/closed=%v", family, outcome, closed), func(t *testing.T) {
					_, engine, request := walHTTPFixture(t)
					flow := walterminal.NewOutcomeFlow(t, engine, family, outcome, closed)
					status, response := request(http.MethodPost, "/db/nornic/tx/commit", walHTTPStatements(flow.Query))
					require.Equal(t, http.StatusOK, status)
					require.Equal(t, flow.CommittedNodes, walterminal.Count(t, engine))
					if !flow.WantError {
						require.Empty(t, response.Errors)
						return
					}
					require.NotEmpty(t, response.Errors, "failed logical terminal cannot be an HTTP success/count response")
					if flow.Primary != nil {
						code, message := nornicerrors.Neo4jStatus(flow.Primary)
						require.Equal(t, code, response.Errors[0].Code)
						require.Contains(t, walHTTPMessages(response), message)
					}
					if closed {
						require.Contains(t, walHTTPMessages(response), storage.ErrWALClosed.Error())
						if outcome == "commit" {
							walterminal.RequireCommitMarkerFailure(t, walHTTPMessages(response))
						}
					}
				})
			}
		}
	}
}

func TestHTTPWALTerminalProducerMultipleOutcomes(t *testing.T) {
	_, _, request := walHTTPFixture(t)
	engines := []*walterminal.Engine{walterminal.New(t), walterminal.New(t)}
	flow := walterminal.NewMultipleOutcomeFlow(t, engines)
	status, response := request(http.MethodPost, "/db/nornic/tx/commit", walHTTPStatements(flow.Query))
	require.Equal(t, http.StatusOK, status)
	for _, engine := range engines {
		require.Zero(t, walterminal.Count(t, engine))
	}
	require.NotEmpty(t, response.Errors)
	code, _ := nornicerrors.Neo4jStatus(flow.Primary)
	require.Equal(t, code, response.Errors[0].Code)
	require.Equal(t, 2, strings.Count(walHTTPMessages(response), "failed to write WAL tx abort"))
}
