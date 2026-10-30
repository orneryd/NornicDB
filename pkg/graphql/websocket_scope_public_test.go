package graphql

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/gorilla/websocket"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/stretchr/testify/require"
)

// A transport close must not return credit while its synchronous consumer is
// still decoding/setup. The real socket supplies the frame; only the consumer
// is paused, after it has obtained bytes from the admitted reader.
func TestGraphQLWebsocketTransportCloseRetainsConsumerReservation(t *testing.T) {
	policy := memory.NewPolicy(1<<20, 0, time.Minute, func() (memory.Sample, error) {
		return memory.Sample{Used: 1}, nil
	}, time.Now)
	require.NoError(t, policy.Refresh())
	decoding, resume := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	release := func() { resumeOnce.Do(func() { close(resume) }) }
	connections := make(chan transport.WebsocketConn, 1)
	returned := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wire := memory.NewWireRequest(policy)
		returned <- wire.Run(func() error {
			owner := &websocketRequestOwner{wire: wire}
			defer owner.close()
			conn, err := (policyWebsocketImplementation{}).Accept(w, withWebsocketRequestOwner(r, owner), transport.WebsocketAcceptOptions{Subprotocols: []string{"graphql-transport-ws"}})
			if err != nil {
				return err
			}
			connections <- conn
			_, reader, err := conn.NextReader()
			if err != nil {
				return err
			}
			var payload map[string]any
			return json.NewDecoder(&pausedWireConsumer{Reader: reader, started: decoding, resume: resume}).Decode(&payload)
		})
	}))
	defer server.Close()
	defer release()
	dialer := websocket.Dialer{Subprotocols: []string{"graphql-transport-ws"}}
	client, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	defer client.Close()
	conn := <-connections
	require.NoError(t, client.WriteMessage(websocket.TextMessage, []byte(`{"type":"connection_init"}`)))
	select {
	case <-decoding:
	case <-time.After(time.Second):
		t.Fatal("consumer never received the frame")
	}
	require.NoError(t, conn.Close())
	require.Positive(t, policy.Reserved(), "transport close must retain credit until the consumer stops")
	release()
	select {
	case err = <-returned:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("consumer did not finish")
	}
	require.Zero(t, policy.Reserved())
}

type pausedWireConsumer struct {
	io.Reader
	started chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (r *pausedWireConsumer) Read(data []byte) (int, error) {
	n, err := r.Reader.Read(data)
	if n > 0 {
		r.once.Do(func() { close(r.started); <-r.resume })
	}
	return n, err
}
