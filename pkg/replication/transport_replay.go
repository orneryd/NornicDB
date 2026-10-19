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
// Every signed message must use this format. Upgrade every cluster node
// before resuming replication; messages from pre-nonce nodes are rejected.

const (
	messageNonceSize = 16

	// replayCacheLimit bounds the cache, which is all the per-sender state
	// replay protection keeps. Entries have a fixed size whatever the
	// sender ID. Only messages with a valid signature are recorded, so only
	// a secret holder can fill it; when full, new messages are refused
	// rather than live nonces forgotten.
	replayCacheLimit = 1 << 20

	defaultAuthMaxSkew = 30 * time.Second
)

// replayKey identifies (sender, nonce) by a fixed-size digest, so a long
// sender ID costs the cache no more than a short one.
type replayKey [sha256.Size]byte

func newReplayKey(nodeID string, nonce []byte) replayKey {
	h := sha256.New()
	writeStringWithLength(h, nodeID)
	writeBytesWithLength(h, nonce)
	var key replayKey
	h.Sum(key[:0])
	return key
}

// replayCache records the nonces seen per sender within the skew window.
type replayCache struct {
	mu        sync.Mutex
	seen      map[replayKey]int64 // expiry, unix nanos
	nextSweep int64
	limit     int
}

func newReplayCache() *replayCache {
	return &replayCache{
		seen:  make(map[replayKey]int64),
		limit: replayCacheLimit,
	}
}

// admit records (nodeID, nonce) until expiry and reports an error if it was
// already recorded or the cache is full. Expired entries are swept at most
// once a second, so a full cache costs a refusal, not a scan per message.
func (c *replayCache) admit(nodeID string, nonce []byte, expiry, now int64) error {
	key := newReplayKey(nodeID, nonce)

	c.mu.Lock()
	defer c.mu.Unlock()
	if now >= c.nextSweep {
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

// size reports how many (sender, nonce) entries the cache holds.
func (c *replayCache) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.seen)
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
// version byte identifies the nonce-signed protocol.
func computeMessageSignatureV2(secret []byte, msg *ClusterMessage) string {
	mac := hmac.New(sha256.New, secret)

	_, _ = mac.Write([]byte{2, byte(msg.Type)})
	writeStringWithLength(mac, msg.NodeID)
	writeInt64(mac, msg.Timestamp)
	writeBytesWithLength(mac, msg.Nonce)
	writeBytesWithLength(mac, msg.Payload)

	return hex.EncodeToString(mac.Sum(nil))
}
