package replication

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// Replay protection for signed cluster messages.
//
// A signed message carries a random Nonce and SignatureV2, an HMAC over the
// type, sender, timestamp, nonce and payload. The receiver accepts a nonce
// once per sender: it remembers (sender, nonce) until the message's timestamp
// leaves the skew window, after which the timestamp check rejects the message
// anyway. So the cache holds at most one window of messages.
//
// Signature, the original HMAC without the nonce, is still sent in compat
// mode so nodes from before this change accept the message during a rolling
// upgrade. A message with only Signature is accepted in compat mode, unless
// its sender has already sent a SignatureV2 message to this transport (a
// stripped SignatureV2 doesn't downgrade an upgraded peer). Require mode
// sends and accepts SignatureV2 only.

const (
	// ReplayProtectionCompat signs both ways and accepts legacy messages
	// from peers not yet upgraded. Default, for rolling upgrades.
	ReplayProtectionCompat = "compat"
	// ReplayProtectionRequire accepts only nonce-carrying messages.
	ReplayProtectionRequire = "require"

	messageNonceSize = 16

	// replayCacheLimit bounds the cache. Only messages with a valid
	// signature are recorded, so only a secret holder can fill it; when
	// full, new messages are refused rather than old nonces forgotten.
	replayCacheLimit = 1 << 20

	defaultAuthMaxSkew = 30 * time.Second
)

type replayKey struct {
	nodeID string
	nonce  [messageNonceSize]byte
}

// replayCache records the nonces seen per sender within the skew window.
type replayCache struct {
	mu        sync.Mutex
	seen      map[replayKey]int64 // expiry, unix nanos
	upgraded  map[string]struct{} // senders seen with SignatureV2
	nextSweep int64
	limit     int
}

func newReplayCache() *replayCache {
	return &replayCache{
		seen:     make(map[replayKey]int64),
		upgraded: make(map[string]struct{}),
		limit:    replayCacheLimit,
	}
}

// admit records (nodeID, nonce) until expiry and reports an error if it was
// already recorded or the cache is full.
func (c *replayCache) admit(nodeID string, nonce []byte, expiry, now int64) error {
	key := replayKey{nodeID: nodeID}
	copy(key.nonce[:], nonce)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.upgraded[nodeID] = struct{}{}
	if now >= c.nextSweep || len(c.seen) >= c.limit {
		for k, exp := range c.seen {
			if exp < now {
				delete(c.seen, k)
			}
		}
		c.nextSweep = now + int64(time.Second)
	}
	if exp, ok := c.seen[key]; ok && exp >= now {
		return localizedError(localization.ReplicationRPCReplayedMessage(), nil)
	}
	if len(c.seen) >= c.limit {
		return localizedError(localization.ReplicationRPCReplayCacheFull(c.limit), nil)
	}
	c.seen[key] = expiry
	return nil
}

// isUpgraded reports whether nodeID has sent a SignatureV2 message.
func (c *replayCache) isUpgraded(nodeID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.upgraded[nodeID]
	return ok
}

func newMessageNonce() []byte {
	nonce := make([]byte, messageNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		// crypto/rand does not fail on supported platforms.
		panic(err)
	}
	return nonce
}

// computeMessageSignatureV2 is the HMAC that covers the nonce. The leading
// version byte keeps it distinct from computeMessageSignature.
func computeMessageSignatureV2(secret []byte, msg *ClusterMessage) string {
	mac := hmac.New(sha256.New, secret)

	_, _ = mac.Write([]byte{2, byte(msg.Type)})
	writeStringWithLength(mac, msg.NodeID)
	writeInt64(mac, msg.Timestamp)
	writeBytesWithLength(mac, msg.Nonce)
	writeBytesWithLength(mac, msg.Payload)

	return hex.EncodeToString(mac.Sum(nil))
}
