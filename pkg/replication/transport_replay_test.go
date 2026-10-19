package replication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const replayTestSecret = "replication-secret-of-forty-characters!!"

func replayTestTransport() *ClusterTransport {
	cfg := DefaultClusterTransportConfig()
	cfg.NodeID = "node-a"
	cfg.AuthSecret = []byte(replayTestSecret)
	return NewClusterTransport(cfg)
}

func replayTestSigned(t *testing.T, sender *ClusterTransport, msgType ClusterMessageType) *ClusterMessage {
	t.Helper()
	conn := sender.createConnection("test", nil)
	msg := &ClusterMessage{Type: msgType, Payload: []byte("payload")}
	conn.signMessage(msg)
	return msg
}

func replayTestCopy(msg *ClusterMessage) *ClusterMessage {
	c := *msg
	return &c
}

// Replay protection covers every message type, requests and responses alike.
func TestVerifyMessage_ReplayRejectedForEveryMessageType(t *testing.T) {
	sender := replayTestTransport()
	receiver := replayTestTransport()
	defer sender.Close()
	defer receiver.Close()
	for msgType := ClusterMessageType(0); msgType <= ClusterMsgForwardApplyResponse; msgType++ {
		msg := replayTestSigned(t, sender, msgType)
		require.NoError(t, receiver.createConnection("a", nil).verifyMessage(replayTestCopy(msg)), "type %d", msgType)
		require.Error(t, receiver.createConnection("b", nil).verifyMessage(replayTestCopy(msg)), "type %d replay accepted", msgType)
	}
}

// Changing the nonce invalidates the signature, so a replay can't be
// disguised as a new message.
func TestVerifyMessage_NonceIsSigned(t *testing.T) {
	sender := replayTestTransport()
	receiver := replayTestTransport()
	defer sender.Close()
	defer receiver.Close()
	msg := replayTestSigned(t, sender, ClusterMsgFence)
	forged := replayTestCopy(msg)
	forged.Nonce = append([]byte(nil), msg.Nonce...)
	forged.Nonce[0] ^= 0xff
	require.Error(t, receiver.createConnection("a", nil).verifyMessage(forged))
}

// The cache forgets nonces whose timestamp has left the window, and refuses
// new messages when full instead of forgetting live ones.
func TestReplayCache_BoundedByWindowAndLimit(t *testing.T) {
	cache := newReplayCache()
	cache.limit = 2
	now := time.Now().UnixNano()
	n1, n2, n3 := newMessageNonce(), newMessageNonce(), newMessageNonce()

	require.NoError(t, cache.admit("a", n1, now+int64(time.Second), now))
	require.NoError(t, cache.admit("a", n2, now+int64(time.Minute), now))
	require.Error(t, cache.admit("a", n3, now+int64(time.Minute), now), "a full cache must refuse")
	require.Error(t, cache.admit("a", n2, now+int64(time.Minute), now), "a recorded nonce must be refused")

	// Two seconds on, n1 has expired and is swept, which frees room.
	later := now + 2*int64(time.Second)
	require.NoError(t, cache.admit("a", n3, later+int64(time.Minute), later))
	require.Len(t, cache.seen, 2)
	// The cache is full again; a new sender gets no room either.
	require.Error(t, cache.admit("b", n1, later+int64(time.Minute), later))
}
