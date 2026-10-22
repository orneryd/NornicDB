package graphql

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/orneryd/nornicdb/internal/testutil"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/stretchr/testify/require"
)

func TestGraphQLWebsocketFrameCapacityAndReservationLifetime(t *testing.T) {
	db := testDB(t)
	manager := testutil.DatabaseManager(t, db)
	policy := memory.NewPolicy(1<<20, 0, time.Minute, func() (memory.Sample, error) {
		return memory.Sample{Used: 1}, nil
	}, time.Now)
	require.NoError(t, policy.Refresh())
	server := httptest.NewServer(newHandler(db, manager, policy))
	t.Cleanup(server.Close)
	dialer := websocket.Dialer{Subprotocols: []string{"graphql-transport-ws"}}
	conn, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	require.NoError(t, conn.WriteJSON(map[string]any{"type": "connection_init"}))
	var reply map[string]any
	require.NoError(t, conn.ReadJSON(&reply))
	require.Equal(t, "connection_ack", reply["type"])
	require.Eventually(t, func() bool { return policy.Reserved() == 0 }, time.Second, time.Millisecond,
		"an idle upgraded connection must not retain a reservation")
	require.NoError(t, conn.WriteJSON(map[string]any{"id": "one", "type": "subscribe", "payload": map[string]any{"query": "{ __typename }"}}))
	require.NoError(t, conn.ReadJSON(&reply))
	require.Equal(t, "next", reply["type"])
	require.NoError(t, conn.ReadJSON(&reply))
	require.Equal(t, "complete", reply["type"])
	require.Eventually(t, func() bool { return policy.Reserved() == 0 }, time.Second, time.Millisecond)
	// Below gqlgen's unchanged 1 MiB limit, above the available four-times-wire
	// forecast. Application admission must stop buffering before JSON decoding.
	_ = conn.WriteJSON(map[string]any{"id": "two", "type": "subscribe", "payload": map[string]any{
		"query": "{ __typename }", "variables": map[string]any{"bulk": strings.Repeat("x", 300<<10)},
	}})
	_, _, err = conn.ReadMessage()
	var closeError *websocket.CloseError
	require.ErrorAs(t, err, &closeError)
	require.Equal(t, websocket.CloseTryAgainLater, closeError.Code)
	require.Contains(t, closeError.Text, websocketMemoryErrorCode)
	require.Eventually(t, func() bool { return policy.Reserved() == 0 }, time.Second, time.Millisecond)
}
