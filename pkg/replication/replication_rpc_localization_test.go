package replication

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

type replicationRPCFailingConn struct {
	writeErr error
}

func (c *replicationRPCFailingConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *replicationRPCFailingConn) Write([]byte) (int, error)        { return 0, c.writeErr }
func (c *replicationRPCFailingConn) Close() error                     { return nil }
func (c *replicationRPCFailingConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *replicationRPCFailingConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *replicationRPCFailingConn) SetDeadline(time.Time) error      { return nil }
func (c *replicationRPCFailingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *replicationRPCFailingConn) SetWriteDeadline(time.Time) error { return nil }

func requireReplicationRPCError(t *testing.T, err error, id localization.MessageID, english string) *localization.LocalizedError {
	t.Helper()

	require.EqualError(t, err, english)
	var localizedErr *localization.LocalizedError
	require.ErrorAs(t, err, &localizedErr)
	require.Equal(t, id, localizedErr.Message.ID)
	require.Equal(t, string(id), localizedErr.Code)
	return localizedErr
}

func TestReplicationRPCReturnedErrorsAreLocalized(t *testing.T) {
	t.Run("handler decode preserves cause", func(t *testing.T) {
		transport := NewClusterTransport(nil)
		RegisterClusterHandlers(transport, &coverageReplicator{})

		_, err := transport.handlers[ClusterMsgHeartbeat](context.Background(), "peer", &ClusterMessage{Payload: []byte("bad")})
		localizedErr := requireReplicationRPCError(t, err, "replication.rpc.decode_failed", "decode heartbeat: invalid character 'b' looking for beginning of value")
		require.ErrorIs(t, err, localizedErr.Cause)
	})

	t.Run("not connected", func(t *testing.T) {
		connection := &ClusterConnection{}
		_, err := connection.sendRPC(context.Background(), &ClusterMessage{})
		requireReplicationRPCError(t, err, "replication.rpc.not_connected", "not connected")
	})

	t.Run("write preserves cause", func(t *testing.T) {
		cause := errors.New("forced write failure")
		netConn := &replicationRPCFailingConn{writeErr: cause}
		connection := &ClusterConnection{
			conn:        netConn,
			writer:      bufio.NewWriter(netConn),
			closeCh:     make(chan struct{}),
			pendingRPCs: make(map[uint64]chan *ClusterMessage),
		}
		connection.connected.Store(true)

		_, err := connection.sendRPC(context.Background(), &ClusterMessage{})
		requireReplicationRPCError(t, err, "replication.rpc.write_failed", "write: forced write failure")
		require.ErrorIs(t, err, cause)
	})

	t.Run("authentication", func(t *testing.T) {
		connection := &ClusterConnection{authSecret: []byte("secret")}
		err := connection.verifyMessage(&ClusterMessage{})
		requireReplicationRPCError(t, err, "replication.rpc.authentication_fields_missing", "missing authentication fields")
	})

	t.Run("read validation", func(t *testing.T) {
		var wire bytes.Buffer
		require.NoError(t, binary.Write(&wire, binary.BigEndian, uint32(65)))
		_, err := readClusterMessage(bufio.NewReader(&wire), 64)
		requireReplicationRPCError(t, err, "replication.rpc.message_too_large", "message too large: 65 > 64")
	})
}

func TestReplicationRPCCatalogRendering(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	message := localization.Message{
		ID:       "replication.rpc.decode_failed",
		Fallback: "decode heartbeat: forced decode failure",
		Data: map[string]any{
			"Operation": "heartbeat",
			"Cause":     "forced decode failure",
		},
	}

	spanish, tag, err := manager.Render(localization.WithPreferences(context.Background(), language.EuropeanSpanish), message)
	require.NoError(t, err)
	require.Equal(t, language.EuropeanSpanish, tag)
	require.Equal(t, "no se pudo decodificar heartbeat: forced decode failure", spanish)

	pseudoTag := language.MustParse("en-XA")
	pseudo, tag, err := manager.Render(localization.WithPreferences(context.Background(), pseudoTag), message)
	require.NoError(t, err)
	require.Equal(t, pseudoTag, tag)
	require.Equal(t, "[!! decode heartbeat: forced decode failure !!]", pseudo)
}

func TestReplicationRPCPrivateCodecGuardsRemainInternal(t *testing.T) {
	_, nodeErr := encodeNodePayload(nil)
	require.EqualError(t, nodeErr, "nil node")
	var nodeLocalizedErr *localization.LocalizedError
	require.False(t, errors.As(nodeErr, &nodeLocalizedErr))

	_, edgeErr := encodeEdgePayload(nil)
	require.EqualError(t, edgeErr, "nil edge")
	var edgeLocalizedErr *localization.LocalizedError
	require.False(t, errors.As(edgeErr, &edgeLocalizedErr))
}
