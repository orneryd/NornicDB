package replication

import (
	"bytes"
	"encoding/gob"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const replayTestSecret = "replication-secret-of-forty-characters!!"

// legacyClusterMessage is ClusterMessage as nodes from before replay
// protection encode and decode it.
type legacyClusterMessage struct {
	Type      ClusterMessageType
	NodeID    string
	Timestamp int64
	Signature string
	Payload   []byte
}

func replayTestTransport(require bool) *ClusterTransport {
	cfg := DefaultClusterTransportConfig()
	cfg.NodeID = "node-a"
	cfg.AuthSecret = []byte(replayTestSecret)
	cfg.RequireReplayProtection = require
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
	sender := replayTestTransport(false)
	receiver := replayTestTransport(false)
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
	sender := replayTestTransport(false)
	receiver := replayTestTransport(false)
	defer sender.Close()
	defer receiver.Close()
	msg := replayTestSigned(t, sender, ClusterMsgFence)
	forged := replayTestCopy(msg)
	forged.Nonce = append([]byte(nil), msg.Nonce...)
	forged.Nonce[0] ^= 0xff
	require.Error(t, receiver.createConnection("a", nil).verifyMessage(forged))
}

// Rolling upgrade: a node from before replay protection accepts a message
// from an upgraded node, and an upgraded node in compat mode accepts one from
// an old node. In require mode the old format is refused.
func TestVerifyMessage_RollingUpgradeCompatibility(t *testing.T) {
	sender := replayTestTransport(false)
	defer sender.Close()

	// New sender, old receiver: decode into the old struct and check the
	// original signature.
	msg := replayTestSigned(t, sender, ClusterMsgWALBatch)
	data, err := encodeGob(msg)
	require.NoError(t, err)
	var old legacyClusterMessage
	require.NoError(t, gob.NewDecoder(bytes.NewReader(data)).Decode(&old))
	asNew := &ClusterMessage{Type: old.Type, NodeID: old.NodeID, Timestamp: old.Timestamp, Payload: old.Payload}
	require.Equal(t, computeMessageSignature([]byte(replayTestSecret), asNew), old.Signature)

	// Old sender, new receiver.
	legacy := legacyClusterMessage{Type: ClusterMsgWALBatch, NodeID: "old-node", Timestamp: time.Now().UnixNano(), Payload: []byte("batch")}
	legacy.Signature = computeMessageSignature([]byte(replayTestSecret), &ClusterMessage{Type: legacy.Type, NodeID: legacy.NodeID, Timestamp: legacy.Timestamp, Payload: legacy.Payload})
	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(legacy))
	var decoded ClusterMessage
	require.NoError(t, decodeGob(buf.Bytes(), &decoded))

	compat := replayTestTransport(false)
	defer compat.Close()
	require.NoError(t, compat.createConnection("a", nil).verifyMessage(replayTestCopy(&decoded)))

	strict := replayTestTransport(true)
	defer strict.Close()
	require.Error(t, strict.createConnection("a", nil).verifyMessage(replayTestCopy(&decoded)))
}

// In require mode a sender sends only the nonce signature, and a receiver
// in either mode accepts it.
func TestSignMessage_RequireModeSendsOnlyNonceSignature(t *testing.T) {
	sender := replayTestTransport(true)
	defer sender.Close()
	msg := replayTestSigned(t, sender, ClusterMsgHeartbeat)
	require.Empty(t, msg.Signature)
	require.NotEmpty(t, msg.SignatureV2)
	require.Len(t, msg.Nonce, messageNonceSize)

	for _, mode := range []bool{false, true} {
		receiver := replayTestTransport(mode)
		require.NoError(t, receiver.createConnection("a", nil).verifyMessage(replayTestCopy(msg)))
		receiver.Close()
	}
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

// The replay protection mode is validated.
func TestConfig_Validate_ReplayProtection(t *testing.T) {
	base := func() *Config {
		cfg := DefaultConfig()
		cfg.Mode = ModeHAStandby
		cfg.NodeID = "node-1"
		cfg.HAStandby.Role = "primary"
		cfg.HAStandby.PeerAddr = "standby:7000"
		cfg.ReplicationSecret = replayTestSecret
		return cfg
	}
	for _, mode := range []string{"", ReplayProtectionCompat, ReplayProtectionRequire} {
		cfg := base()
		cfg.ReplayProtection = mode
		require.NoError(t, cfg.Validate(), mode)
	}
	cfg := base()
	cfg.ReplayProtection = "strict"
	require.ErrorContains(t, cfg.Validate(), "NORNICDB_CLUSTER_REPLAY_PROTECTION")

	cfg = base()
	cfg.ReplayProtection = ReplayProtectionRequire
	cfg.ReplicationSecret = ""
	require.ErrorContains(t, cfg.Validate(), "NORNICDB_CLUSTER_REPLICATION_SECRET")

	t.Setenv("NORNICDB_CLUSTER_REPLAY_PROTECTION", " Require ")
	require.Equal(t, ReplayProtectionRequire, LoadFromEnv().ReplayProtection)
}
