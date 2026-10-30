package storage

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// WAL buffered-commit payload compression. Every acknowledged write-behind
// commit serializes its full payload (operations, raw KV writes/deletes,
// property-key tokens) into the WAL for kill -9 recovery. The typed payload
// preserves binary keys and is compressed without changing its contents.

var (
	walZstdEncOnce sync.Once
	walZstdEnc     *zstd.Encoder
)

// A buffered commit is a single recovery unit. Its decoded payload must stay
// bounded even when a tiny zstd frame expands dramatically during restart.
const maxBufferedWALPayloadBytes = 64 << 20

func walZstdEncoder() *zstd.Encoder {
	walZstdEncOnce.Do(func() {
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err == nil {
			walZstdEnc = enc
		}
	})
	return walZstdEnc
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
		if len(data) > maxBufferedWALPayloadBytes {
			return nil, fmt.Errorf("wal: buffered commit exceeds decoded payload limit")
		}
		return data, nil
	}
	dec, err := zstd.NewReader(bytes.NewReader(data), zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(maxBufferedWALPayloadBytes))
	if err != nil {
		return nil, fmt.Errorf("wal: zstd decoder unavailable: %w", err)
	}
	defer dec.Close()
	out, err := io.ReadAll(io.LimitReader(dec, maxBufferedWALPayloadBytes+1))
	if err != nil {
		return nil, fmt.Errorf("wal: failed to decompress buffered commit: %w", err)
	}
	if len(out) > maxBufferedWALPayloadBytes {
		return nil, fmt.Errorf("wal: buffered commit exceeds decoded payload limit")
	}
	return out, nil
}
