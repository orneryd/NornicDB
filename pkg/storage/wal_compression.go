package storage

import (
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// WAL buffered-commit payload compression. Every acknowledged write-behind
// commit serializes its full payload (operations, raw KV writes/deletes,
// property-key tokens) into the WAL for kill -9 recovery; that payload is
// JSON with repetitive structure and previously blew the WAL up to several
// times the Badger data size. Lossless zstd shrinks it by ~5-10× with no
// durability impact: the decompressed bytes are identical to what was
// acknowledged, so replay is byte-for-byte equivalent.

var (
	walZstdEncOnce sync.Once
	walZstdEnc     *zstd.Encoder
	walZstdDecOnce sync.Once
	walZstdDec     *zstd.Decoder
)

func walZstdEncoder() *zstd.Encoder {
	walZstdEncOnce.Do(func() {
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err == nil {
			walZstdEnc = enc
		}
	})
	return walZstdEnc
}

func walZstdDecoder() *zstd.Decoder {
	walZstdDecOnce.Do(func() {
		dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err == nil {
			walZstdDec = dec
		}
	})
	return walZstdDec
}

// compressWALBufferedCommitPayload zstd-compresses a buffered-commit payload.
// Small payloads and compression failures fall back to the raw bytes, so the
// WAL remains readable either way.
func compressWALBufferedCommitPayload(data []byte) []byte {
	if len(data) < 512 {
		return data
	}
	enc := walZstdEncoder()
	if enc == nil {
		return data
	}
	return enc.EncodeAll(data, nil)
}

// decompressWALBufferedCommitPayload decompresses a buffered-commit payload
// that was compressed on write (detected by the zstd frame magic). Raw
// payloads pass through unchanged.
func decompressWALBufferedCommitPayload(data []byte) ([]byte, error) {
	if len(data) < 4 || data[0] != 0x28 || data[1] != 0xB5 || data[2] != 0x2F || data[3] != 0xFD {
		return data, nil
	}
	dec := walZstdDecoder()
	if dec == nil {
		return nil, fmt.Errorf("wal: zstd decoder unavailable")
	}
	out, err := dec.DecodeAll(data, nil)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to decompress buffered commit: %w", err)
	}
	return out, nil
}
