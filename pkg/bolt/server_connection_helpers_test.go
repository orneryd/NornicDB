package bolt

import (
	"crypto/tls"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "timeout" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return false }

func TestConnectionHelperClassifiers(t *testing.T) {
	t.Run("isDeadlineErr handles nil, os, and net timeout errors", func(t *testing.T) {
		assert.False(t, isDeadlineErr(nil))
		assert.True(t, isDeadlineErr(os.ErrDeadlineExceeded))
		assert.True(t, isDeadlineErr(timeoutNetError{}))
		assert.False(t, isDeadlineErr(errors.New("ordinary error")))
	})

	t.Run("classifySniffError maps known reasons and defaults", func(t *testing.T) {
		assert.Equal(t, "unrecognized_prefix", classifySniffError(nil))
		assert.Equal(t, "requires_tls", classifySniffError(ErrUnencryptedRequired))
		assert.Equal(t, "sniff_timeout", classifySniffError(errors.New("transport sniff timeout: read tcp timeout")))
		assert.Equal(t, "tls_handshake", classifySniffError(errors.New("tls handshake: bad certificate")))
		assert.Equal(t, "unrecognized_prefix", classifySniffError(errors.New("something else")))
	})
}

func TestTransportLabelAndUnwrapHelpers(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	tlsConn := tls.Client(clientConn, &tls.Config{InsecureSkipVerify: true})
	unwrapped, encrypted := unwrapTLS(tlsConn)
	require.True(t, encrypted)
	assert.Equal(t, clientConn, unwrapped)
	assert.Equal(t, "tcp_tls", transportLabelFor(transportRaw, tlsConn))
	assert.Equal(t, "ws_tls", transportLabelFor(transportWebSocket, &wsConn{encrypted: true}))
	assert.Equal(t, "ws", transportLabelFor(transportWebSocket, &wsConn{encrypted: false}))
	assert.Equal(t, "tcp", transportLabelFor(transportRaw, serverConn))

	plainUnwrapped, plainEncrypted := unwrapTLS(serverConn)
	assert.False(t, plainEncrypted)
	assert.Equal(t, serverConn, plainUnwrapped)
}

func TestHandleConnection_MaxConnectionsRejectsEarly(t *testing.T) {
	reg := prometheus.NewRegistry()
	bag := observability.NewBoltMetrics(reg)

	server := New(DefaultConfig(), &mockExecutor{})
	server.config.MaxConnections = 1
	server.activeConnections.Store(1)
	server.SetBoltMetrics(bag)

	conn := &mockConn{}
	server.handleConnection(conn)

	assert.True(t, conn.closed, "max-connections rejection should close the incoming conn")
	assert.Equal(t, int64(1), server.activeConnections.Load(), "rejected connection must not leak active connection count")
	assert.Equal(t, 1.0, sumCounterWithLabelValue(t, reg, "nornicdb_bolt_connections_rejected_total", "reason", "max_connections"))
}

func sumCounterWithLabelValue(t *testing.T, reg *prometheus.Registry, name, labelName, labelValue string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		var total float64
		for _, m := range mf.Metric {
			for _, lp := range m.Label {
				if lp.GetName() == labelName && lp.GetValue() == labelValue {
					total += m.GetCounter().GetValue()
				}
			}
		}
		return total
	}
	t.Fatalf("counter %q not found in registry", name)
	return 0
}

func TestHandleConnection_MaxConnectionsRejectsImmediatelyForPipeConn(t *testing.T) {
	server := New(DefaultConfig(), &mockExecutor{})
	server.config.MaxConnections = 1
	server.activeConnections.Store(1)

	clientConn, serverConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		server.handleConnection(serverConn)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("handleConnection did not return promptly on max-connections rejection")
	}

	buf := make([]byte, 1)
	_, err := clientConn.Read(buf)
	assert.Error(t, err, "client side should observe closed connection after rejection")
	assert.Equal(t, int64(1), server.activeConnections.Load())
	_ = clientConn.Close()
}
