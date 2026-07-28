package security

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

var processLocalDigestKey = mustRandomDigestKey()

func mustRandomDigestKey() []byte {
	key := make([]byte, sha256.Size)
	if _, err := rand.Read(key); err != nil {
		panic("security: failed to initialize digest key: " + err.Error())
	}
	return key
}

// KeyedDigest returns a deterministic process-local HMAC-SHA256 digest for the
// supplied namespace and parts. The namespace provides domain separation so the
// same value used in different contexts does not yield the same digest.
func KeyedDigest(namespace string, parts ...string) [32]byte {
	mac := hmac.New(sha256.New, processLocalDigestKey)
	writeDigestPart(mac, namespace)
	for _, part := range parts {
		writeDigestPart(mac, part)
	}

	var out [32]byte
	copy(out[:], mac.Sum(nil))
	return out
}

// KeyedDigestHex is the hex-encoded form of KeyedDigest.
func KeyedDigestHex(namespace string, parts ...string) string {
	digest := KeyedDigest(namespace, parts...)
	return hex.EncodeToString(digest[:])
}

func writeDigestPart(mac hashWriter, value string) {
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(value)))
	_, _ = mac.Write(lenBuf[:])
	_, _ = mac.Write([]byte(value))
}

type hashWriter interface {
	Write(p []byte) (n int, err error)
}
