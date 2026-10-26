// Author: Codex, NOR-180 producer-outward Bolt regressions, 2026-10-27.
package bolt

import (
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBoltWALTerminalProducerFlows(t *testing.T) {
	for _, family := range walterminal.FlowFamilies {
		for _, outcome := range walterminal.FlowOutcomes {
			for _, closed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/closed=%v", family, outcome, closed), func(t *testing.T) {
					engine := walterminal.New(t)
					flow := walterminal.NewOutcomeFlow(t, engine, family, outcome, closed)
					conn := openBoltTestConn(t, startTransactionLifecycleServerWithStore(t, storage.NewNamespacedEngine(engine, "nornic")))
					require.NoError(t, SendRun(t, conn, flow.Query, nil, nil))
					if !flow.WantError {
						require.NoError(t, ReadSuccess(t, conn))
						require.NoError(t, SendPull(t, conn, nil))
						for {
							kind, _, err := ReadMessage(conn)
							require.NoError(t, err)
							if kind == MsgSuccess {
								break
							}
							require.Equal(t, byte(MsgRecord), kind)
						}
					} else {
						code, message, err := AssertFailure(t, conn)
						require.NoError(t, err, "failed logical terminal cannot become a Bolt SUCCESS/count response")
						if flow.Primary != nil {
							wantCode, wantMessage := nornicerrors.Neo4jStatus(flow.Primary)
							require.Equal(t, wantCode, code)
							require.Contains(t, message, wantMessage)
						}
						if closed {
							require.Contains(t, message, storage.ErrWALClosed.Error())
							if outcome == "commit" {
								walterminal.RequireCommitMarkerFailure(t, message)
							}
						}
					}
					require.Equal(t, flow.CommittedNodes, walterminal.Count(t, engine))
				})
			}
		}
	}
}

func TestBoltWALTerminalProducerMultipleOutcomes(t *testing.T) {
	outer := walterminal.New(t)
	engines := []*walterminal.Engine{walterminal.New(t), walterminal.New(t)}
	flow := walterminal.NewMultipleOutcomeFlow(t, engines)
	conn := openBoltTestConn(t, startTransactionLifecycleServerWithStore(t, storage.NewNamespacedEngine(outer, "nornic")))
	require.NoError(t, SendRun(t, conn, flow.Query, nil, nil))
	code, message, err := AssertFailure(t, conn)
	require.NoError(t, err)
	wantCode, _ := nornicerrors.Neo4jStatus(flow.Primary)
	require.Equal(t, wantCode, code)
	require.Equal(t, 2, strings.Count(message, "failed to write WAL tx abort"))
	for _, engine := range engines {
		require.Zero(t, walterminal.Count(t, engine))
	}
}
