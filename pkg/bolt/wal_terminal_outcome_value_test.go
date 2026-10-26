// Author: Codex,NOR-180 immediate Bolt outcome contract,29October2026.
package bolt

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBoltWALTerminalOutcomeValueViews(t *testing.T) {
	for _, profile := range walterminal.ContractProfiles {
		for _, parallel := range []bool{false, true} {
			for _, closed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/parallel=%v/closed=%v", profile.Name, parallel, closed), func(t *testing.T) {
					outer := walterminal.New(t)
					c := walterminal.NewOutcomeContract(t, profile, closed, parallel)
					conn := openBoltTestConn(t, startTransactionLifecycleServerWithStore(t, storage.NewNamespacedEngine(outer, "nornic")))
					require.NoError(t, SendRun(t, conn, c.Flow.Query, nil, nil))
					if !closed {
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
						require.NoError(t, err)
						c.RequireView(t, code, message)
					}
					c.RequireNative(t)
				})
			}
		}
	}
}
