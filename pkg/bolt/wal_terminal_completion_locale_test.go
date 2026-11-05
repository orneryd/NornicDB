// Author: Codex, NOR-180 localized public Bolt completion boundary, 2026-11-01.
package bolt

import (
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestBoltWALTerminalCompletionLocales(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	for _, tag := range manager.SupportedTags() {
		for _, operation := range []string{"commit", "rollback", "reset"} {
			t.Run(tag.String()+"/"+operation, func(t *testing.T) {
				engine := walterminal.New(t)
				store := storage.NewNamespacedEngine(engine, "nornic")
				mgr := &mockDBManager{stores: map[string]storage.Engine{"nornic": store}, defaultDB: "nornic"}
				server := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 8, ReadBufferSize: 8192, WriteBufferSize: 8192, Localizer: manager}, &mockExecutor{}, mgr)
				port := startBoltTestServer(t, server)
				conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
				require.NoError(t, err)
				t.Cleanup(func() { _ = conn.Close() })
				require.NoError(t, PerformHandshakeWithTesting(t, conn))
				require.NoError(t, SendHello(t, conn, map[string]string{"locale": tag.String()}))
				require.NoError(t, ReadSuccess(t, conn))
				beginExplicitTransaction(t, conn, nil)
				require.NoError(t, SendRun(t, conn, "CREATE (:WalWire)", nil, map[string]any{"locale": tag.String()}))
				require.NoError(t, ReadSuccess(t, conn))
				require.NoError(t, SendPull(t, conn, nil))
				require.NoError(t, ReadSuccess(t, conn))
				require.NoError(t, engine.WAL.Close())
				descriptor := localization.CypherTransactionsWALAbortFailed("TRANSACTION_ID", storage.ErrWALClosed)
				switch operation {
				case "commit":
					descriptor = localization.CypherTransactionsWALCommitFailed("TRANSACTION_ID", storage.ErrWALClosed)
					require.NoError(t, SendCommit(t, conn))
				case "rollback":
					require.NoError(t, SendRollback(t, conn))
				case "reset":
					require.NoError(t, SendReset(t, conn))
				}
				code, message, err := AssertFailure(t, conn)
				require.NoError(t, err)
				expected, _, err := manager.Render(localization.WithPreferences(t.Context(), tag), localization.MessageRequest(descriptor))
				require.NoError(t, err)
				for _, part := range strings.Split(expected, "TRANSACTION_ID") {
					require.Contains(t, message, part)
				}
				if operation == "commit" {
					require.Equal(t, "Neo.DatabaseError.Transaction.TransactionCommitFailed", code)
					require.Equal(t, int64(1), walterminal.Count(t, engine))
				} else {
					require.Equal(t, "Neo.DatabaseError.Transaction.TransactionRollbackFailed", code)
					require.Zero(t, walterminal.Count(t, engine))
				}
			})
		}
	}
}
