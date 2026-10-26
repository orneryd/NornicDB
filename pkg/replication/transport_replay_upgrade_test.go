package replication

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"github.com/orneryd/nornicdb/pkg/storage"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This fixture is the pre-nonce wire format. Keep its signer in tests only.
type preNonceMessage struct {
	Type      ClusterMessageType
	NodeID    string
	Timestamp int64
	Signature string
	Payload   []byte
}

func preNonceFrame(t *testing.T, msgType ClusterMessageType) []byte {
	t.Helper()
	msg := preNonceMessage{Type: msgType, NodeID: "old-node", Timestamp: time.Now().UnixNano(), Payload: []byte("old payload")}
	mac := hmac.New(sha256.New, []byte(replayTestSecret))
	_, _ = mac.Write([]byte{byte(msg.Type)})
	writeStringWithLength(mac, msg.NodeID)
	writeInt64(mac, msg.Timestamp)
	writeBytesWithLength(mac, msg.Payload)
	msg.Signature = hex.EncodeToString(mac.Sum(nil))
	data, err := storage.EncodeCommandPayload(&msg)
	require.NoError(t, err)
	return data
}

func TestClusterTransport_OldFormatRejectedByDefault(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NodeID = "new-node"
	cfg.ReplicationSecret = replayTestSecret
	transport, err := NewDefaultTransportFromConfig(cfg)
	require.NoError(t, err)
	receiver := transport.(*ClusterTransport)
	defer receiver.Close()
	var handled atomic.Int32
	for msgType := ClusterMsgVoteRequest; msgType <= ClusterMsgForwardApplyResponse; msgType++ {
		receiver.RegisterHandler(msgType, func(context.Context, string, *ClusterMessage) (*ClusterMessage, error) {
			handled.Add(1)
			return nil, nil
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr := tlsShutdownTestAddr(t)
	go func() { _ = receiver.Listen(ctx, addr, nil) }()
	waitTCPListening(t, addr)

	for msgType := ClusterMsgVoteRequest; msgType <= ClusterMsgForwardApplyResponse; msgType++ {
		conn, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		data := preNonceFrame(t, msgType)
		w := bufio.NewWriter(conn)
		require.NoError(t, binary.Write(w, binary.BigEndian, uint32(len(data))))
		_, err = w.Write(data)
		require.NoError(t, err)
		require.NoError(t, w.Flush())
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		_, err = conn.Read(make([]byte, 1))
		_ = conn.Close()
		requireClosedByPeer(t, err, "old-format messages must close the connection")
	}
	require.Zero(t, handled.Load(), "old-format messages reached a handler")
}

func TestSignMessage_AlwaysUsesFreshNonceSignature(t *testing.T) {
	sender := replayTestTransport()
	defer sender.Close()
	receiver := replayTestTransport()
	defer receiver.Close()
	conn := sender.createConnection("sender", nil)
	msg := &ClusterMessage{Type: ClusterMsgHeartbeat}
	var previous []byte
	for i := 0; i < 2; i++ {
		conn.signMessage(msg)
		require.Len(t, msg.Nonce, messageNonceSize)
		require.NotEqual(t, previous, msg.Nonce)
		require.NotEmpty(t, msg.SignatureV2)
		data, err := storage.EncodeCommandPayload(msg)
		require.NoError(t, err)
		var old preNonceMessage
		require.NoError(t, storage.DecodeCommandPayload(data, &old))
		require.Empty(t, old.Signature, "the v1 signature must never be sent")
		require.NoError(t, receiver.createConnection("receiver", nil).verifyMessage(msg))
		require.Error(t, receiver.createConnection("replay", nil).verifyMessage(msg))
		previous = append([]byte(nil), msg.Nonce...)
	}
}

func TestVerifyMessage_RejectsMissingNonceOrSignatureV2(t *testing.T) {
	sender := replayTestTransport()
	defer sender.Close()
	receiver := replayTestTransport()
	defer receiver.Close()
	for _, field := range []string{"nonce", "short nonce", "signature"} {
		t.Run(field, func(t *testing.T) {
			msg := replayTestSigned(t, sender, ClusterMsgWALBatch)
			switch field {
			case "nonce":
				msg.Nonce = nil
			case "short nonce":
				msg.Nonce = msg.Nonce[:messageNonceSize-1]
			case "signature":
				msg.SignatureV2 = ""
			}
			require.Error(t, receiver.createConnection("receiver", nil).verifyMessage(msg))
		})
	}
}

// Exercise all ten request/response pairs over real TCP among three signed
// nodes, with two peers admitting messages to the same cache concurrently.
func TestClusterTransport_SignedMessageTypesRoundTrip(t *testing.T) {
	server := replayTestTransport()
	server.nodeID = "server"
	defer server.Close()
	for msgType := ClusterMsgVoteRequest; msgType < ClusterMsgForwardApplyResponse; msgType += 2 {
		server.RegisterHandler(msgType, func(_ context.Context, nodeID string, msg *ClusterMessage) (*ClusterMessage, error) {
			if (nodeID != "client-a" && nodeID != "client-b") || len(msg.Nonce) != messageNonceSize || msg.SignatureV2 == "" {
				return nil, net.ErrClosed
			}
			return &ClusterMessage{Type: msg.Type + 1, Payload: msg.Payload}, nil
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	addr := tlsShutdownTestAddr(t)
	go func() { _ = server.Listen(ctx, addr, nil) }()
	waitTCPListening(t, addr)
	t.Run("peers", func(t *testing.T) {
		for _, nodeID := range []string{"client-a", "client-b"} {
			t.Run(nodeID, func(t *testing.T) {
				t.Parallel()
				client := replayTestTransport()
				client.nodeID = nodeID
				defer client.Close()
				peer, err := client.Connect(ctx, addr)
				require.NoError(t, err)
				require.True(t, waitForConnected(peer, time.Second))
				for msgType := ClusterMsgVoteRequest; msgType < ClusterMsgForwardApplyResponse; msgType += 2 {
					resp, err := peer.(*ClusterConnection).sendRPC(ctx, &ClusterMessage{Type: msgType, Payload: []byte{byte(msgType)}})
					require.NoError(t, err, "message type %d", msgType)
					require.Equal(t, msgType+1, resp.Type)
					require.Equal(t, "server", resp.NodeID)
					require.Equal(t, []byte{byte(msgType)}, resp.Payload)
					require.Len(t, resp.Nonce, messageNonceSize)
					require.NotEmpty(t, resp.SignatureV2)
				}
				require.Equal(t, 10, client.replay.size(), "every response must enter the replay cache")
			})
		}
	})
	require.Equal(t, 20, server.replay.size(), "every request from both peers must enter the shared replay cache")
}
