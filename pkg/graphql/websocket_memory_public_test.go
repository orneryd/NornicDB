package graphql

// Author of fixture cleanup changes: L4 implementation lane (Claude and Codex), 1 November 2026.

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/orneryd/nornicdb/internal/testutil"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

func TestGraphQLWebsocketApplicationAdmissionBeforeDecode(t *testing.T) {
	config := nornicdb.DefaultConfig()
	config.Memory.ApplicationSoftMax = 1 // physical pressure on every lab worker
	db, err := nornicdb.Open("", config)
	var closeDB func()
	if db != nil {
		_, closeDB = fixtureowner.OwnWithClose(t, db)
	}
	require.NoError(t, err)
	manager := testutil.DatabaseManager(t, db, closeDB)
	server := httptest.NewServer(NewHandler(db, manager))
	t.Cleanup(server.Close)
	dialer := websocket.Dialer{Subprotocols: []string{"graphql-transport-ws"}}
	conn, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	_ = conn.WriteMessage(websocket.TextMessage, []byte("{invalid client JSON"))
	_, _, err = conn.ReadMessage()
	var closeError *websocket.CloseError
	require.ErrorAs(t, err, &closeError)
	require.Equal(t, websocket.CloseTryAgainLater, closeError.Code,
		"pressure must close before gqlgen's malformed-JSON decoder runs")
	require.Contains(t, closeError.Text, "Neo.TransientError.General.MemoryPoolOutOfMemoryError")
	require.Eventually(t, func() bool { return db.MemoryPolicy().Reserved() == 0 }, time.Second, time.Millisecond)
}
