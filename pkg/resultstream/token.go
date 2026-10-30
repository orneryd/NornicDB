package resultstream

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
)

const (
	tokenVersion   = byte(2)
	tokenBodyBytes = 1 + 8 + 16 + 8 + 8 + 32
	tokenBytes     = tokenBodyBytes + sha256.Size
	// Bound the self-contained database binding before decoding client input.
	maxTokenDatabaseBytes = 64 << 10
)

type decodedToken struct {
	streamID  [16]byte
	position  uint64
	expires   int64
	ownerHash [32]byte
	database  string
}

func encodeToken(secret [32]byte, instance [8]byte, streamID [16]byte, position uint64, expires int64, ownerHash [32]byte, database string) string {
	raw := make([]byte, tokenBytes+len(database))
	raw[0] = tokenVersion
	copy(raw[1:9], instance[:])
	copy(raw[9:25], streamID[:])
	binary.BigEndian.PutUint64(raw[25:33], position)
	binary.BigEndian.PutUint64(raw[33:41], uint64(expires))
	copy(raw[41:73], ownerHash[:])
	copy(raw[tokenBodyBytes:], database)
	bodyEnd := len(raw) - sha256.Size
	mac := tokenMAC(secret, raw[:bodyEnd])
	copy(raw[bodyEnd:], mac[:])
	return base64.RawURLEncoding.EncodeToString(raw[:])
}

func decodeToken(secret [32]byte, instance [8]byte, qid string) (decodedToken, error) {
	var token decodedToken
	if len(qid) < base64.RawURLEncoding.EncodedLen(tokenBytes+1) ||
		len(qid) > base64.RawURLEncoding.EncodedLen(tokenBytes+maxTokenDatabaseBytes) {
		return token, ErrInvalidQID
	}
	raw := make([]byte, base64.RawURLEncoding.DecodedLen(len(qid)))
	decoded, err := base64.RawURLEncoding.Strict().Decode(raw[:], []byte(qid))
	if err != nil || decoded != len(raw) || raw[0] != tokenVersion || !hmac.Equal(raw[1:9], instance[:]) {
		return token, ErrInvalidQID
	}
	bodyEnd := len(raw) - sha256.Size
	mac := tokenMAC(secret, raw[:bodyEnd])
	if !hmac.Equal(raw[bodyEnd:], mac[:]) {
		return token, ErrInvalidQID
	}
	copy(token.streamID[:], raw[9:25])
	token.position = binary.BigEndian.Uint64(raw[25:33])
	token.expires = int64(binary.BigEndian.Uint64(raw[33:41]))
	copy(token.ownerHash[:], raw[41:73])
	token.database = string(raw[tokenBodyBytes:bodyEnd])
	return token, nil
}

func tokenMAC(secret [32]byte, body []byte) [sha256.Size]byte {
	var innerPad, outerPad [sha256.BlockSize]byte
	for i := range innerPad {
		innerPad[i], outerPad[i] = 0x36, 0x5c
	}
	for i := range secret {
		innerPad[i] ^= secret[i]
		outerPad[i] ^= secret[i]
	}
	inner := sha256.New()
	_, _ = inner.Write(innerPad[:])
	_, _ = inner.Write(body)
	var sum [sha256.Size]byte
	inner.Sum(sum[:0])
	outer := sha256.New()
	_, _ = outer.Write(outerPad[:])
	_, _ = outer.Write(sum[:])
	outer.Sum(sum[:0])
	return sum
}
