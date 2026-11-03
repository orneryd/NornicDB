package security

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
)

var processLocalDigestKey = mustRandomDigestKey()

// keyedDigestChunkSize bounds how many bytes of a digest part are converted to
// bytes and hashed at a time, so a large part is never converted in one piece.
// It also bounds the single scratch buffer a digest reuses for every chunk of
// every part. The HMAC framing is independent of this value: chunked writes
// produce the same digest as hashing the whole part at once.
const keyedDigestChunkSize = 32 << 10

func mustRandomDigestKey() []byte {
	key := make([]byte, sha256.Size)
	if _, err := rand.Read(key); err != nil {
		panic("security: failed to initialize digest key: " + err.Error())
	}
	return key
}

// KeyedDigest returns a deterministic process-local HMAC-SHA256 digest for the
// supplied namespace and parts. The namespace provides domain separation so the
// same value used in different contexts does not yield the same digest. Every
// namespace and part is length-prefixed with its 8-byte big-endian length, so
// different part boundaries never produce the same digest.
//
// Parts are hashed in bounded chunks. ctx is checked before the digest starts,
// before each part's length prefix, and before each chunk is converted to bytes
// and hashed. When ctx is cancelled or its deadline passes, KeyedDigest stops
// and returns ctx.Err() with the zero digest.
//
// A digest reuses one bounded chunk buffer for all of its chunks and parts, and
// allocates it only when a part is long enough to need chunking, so a large
// input is never converted to bytes in one piece and a small one never
// allocates the buffer.
//
// The digest is only meaningful within the current process: the key is random
// per process. KeyedDigest requires a non-nil ctx, as usual for Go APIs that
// accept a context.
//
// Example:
//
//	digest, err := security.KeyedDigest(ctx, "auth.basic.header", r.Header.Get("Authorization"))
//	if err != nil {
//		return err
//	}
//	if digest == deniedDigest {
//		return ErrDenied
//	}
func KeyedDigest(ctx context.Context, namespace string, parts ...string) ([32]byte, error) {
	var out [32]byte
	if err := ctx.Err(); err != nil {
		return out, err
	}

	mac := hmac.New(sha256.New, processLocalDigestKey)
	// scratch carries the one bounded chunk buffer this digest reuses across
	// every chunk of every part, including shorter trailing chunks.
	var scratch []byte
	// hash.Hash embeds io.Writer: Write cannot retain its input. Reuse the
	// fixed framing buffer after each synchronous write instead of allocating
	// one escaping length array for every namespace and tuple part.
	var length [8]byte
	var err error
	if scratch, err = writeDigestPart(ctx, mac, scratch, &length, namespace); err != nil {
		return out, err
	}
	for _, part := range parts {
		if scratch, err = writeDigestPart(ctx, mac, scratch, &length, part); err != nil {
			return out, err
		}
	}

	// out is returned by value and nothing else retains it. Summing into
	// out[:0] instead of copying measures the same allocations, because passing
	// out to Sum moves out to the heap, which costs the allocation the in-place
	// sum would save; copying keeps out on the stack.
	sum := mac.Sum(nil)
	// Publish only after observing cancellation that arrived during the final
	// bounded chunk or digest finalization; callers never receive a partial or
	// canceled key as a successful cache/constraint identity.
	if err := ctx.Err(); err != nil {
		return out, err
	}
	copy(out[:], sum)
	return out, nil
}

// KeyedDigestHex is the hex-encoded form of KeyedDigest. It returns an empty
// string and ctx.Err() when the context is cancelled or its deadline passes.
func KeyedDigestHex(ctx context.Context, namespace string, parts ...string) (string, error) {
	digest, err := KeyedDigest(ctx, namespace, parts...)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(digest[:]), nil
}

// writeDigestPart writes one length-prefixed part to mac. The 8-byte big-endian
// length prefix is the original framing, so digests stay identical to hashing
// every part in one write. The body is written in bounded chunks, and ctx is
// checked before each chunk is converted to bytes, so a cancelled context stops
// work before the next chunk is consumed.
//
// scratch is the digest's one bounded buffer. It is returned so the next part
// reuses it, and it is allocated only when a part is longer than one chunk. A
// part that fits in one chunk is written from its own bytes, so a digest of
// short parts never allocates the buffer.
func writeDigestPart(ctx context.Context, mac hash.Hash, scratch []byte, length *[8]byte, value string) ([]byte, error) {
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	if err := ctx.Err(); err != nil {
		return scratch, err
	}
	if _, err := mac.Write(length[:]); err != nil {
		return scratch, fmt.Errorf("security: write digest part length: %w", err)
	}

	// Decide once per part whether its body streams through the scratch buffer,
	// and keep streaming for the whole part so a shorter trailing chunk reuses
	// the same buffer instead of converting its own bytes.
	stream := len(value) > keyedDigestChunkSize || scratch != nil
	for len(value) > 0 {
		chunk := value
		if len(chunk) > keyedDigestChunkSize {
			chunk = value[:keyedDigestChunkSize]
		}
		value = value[len(chunk):]
		if err := ctx.Err(); err != nil {
			return scratch, err
		}

		var body []byte
		if stream {
			// Allocated lazily: after the context check above, immediately
			// before the copy into it, so a cancelled digest never allocates it.
			if scratch == nil {
				scratch = make([]byte, keyedDigestChunkSize)
			}
			body = scratch[:copy(scratch, chunk)]
		} else {
			body = []byte(chunk)
		}
		if _, err := mac.Write(body); err != nil {
			return scratch, fmt.Errorf("security: write digest part body: %w", err)
		}
	}
	return scratch, nil
}
