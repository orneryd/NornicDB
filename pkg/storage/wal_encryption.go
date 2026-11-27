package storage

import (
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/chacha20poly1305"
)

// Encrypted WAL frames carry a 192-bit random nonce followed by an
// authenticated ciphertext. The distinct frame versions prevent old readers
// from interpreting ciphertext as a damaged plaintext record.
const (
	walFormatVersionEncrypted        uint8 = 8
	walFormatVersionEncryptedSpanned uint8 = 9
	walEncryptedFrameOverhead              = chacha20poly1305.NonceSizeX + chacha20poly1305.Overhead
)

var ErrWALKeyRequired = errors.New("wal: encryption key required")

type walFrameCipher struct {
	aead cipher.AEAD
}

func newWALFrameCipher(master []byte) (*walFrameCipher, error) {
	if len(master) != 32 {
		return nil, fmt.Errorf("wal: expected 32-byte encryption key, got %d", len(master))
	}
	mac := hmac.New(sha256.New, master)
	_, _ = mac.Write([]byte("nornicdb/wal/frame-encryption/v1"))
	derived := mac.Sum(nil)
	aead, err := chacha20poly1305.NewX(derived)
	if err != nil {
		return nil, err
	}
	return &walFrameCipher{aead: aead}, nil
}

func walFrameEncrypted(version uint8) bool {
	return version == walFormatVersionEncrypted || version == walFormatVersionEncryptedSpanned
}

func walPlaintextFrameVersion(version uint8) uint8 {
	switch version {
	case walFormatVersionEncrypted:
		return walFormatVersionCurrent
	case walFormatVersionEncryptedSpanned:
		return walFormatVersionSpanned
	default:
		return version
	}
}

func walEncryptedFrameVersion(version uint8) (uint8, error) {
	switch version {
	case walFormatVersionCurrent:
		return walFormatVersionEncrypted, nil
	case walFormatVersionSpanned:
		return walFormatVersionEncryptedSpanned, nil
	default:
		return 0, unsupportedWALVersion(version)
	}
}

func walFramePayloadLimit(version uint8) uint32 {
	if walFrameEncrypted(version) {
		return walMaxEntrySize + walEncryptedFrameOverhead
	}
	return walMaxEntrySize
}

func (c *walFrameCipher) seal(plaintext []byte, version uint8) ([]byte, uint8, error) {
	if c == nil {
		return nil, 0, ErrWALKeyRequired
	}
	if len(plaintext) > int(walMaxEntrySize) {
		return nil, 0, fmt.Errorf("wal: frame plaintext exceeds maximum")
	}
	newVersion, err := walEncryptedFrameVersion(version)
	if err != nil {
		return nil, 0, err
	}
	nonce := make([]byte, chacha20poly1305.NonceSizeX)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, 0, fmt.Errorf("wal: nonce generation failed: %w", err)
	}
	payloadLen := uint32(len(plaintext) + walEncryptedFrameOverhead)
	var header [9]byte
	binary.LittleEndian.PutUint32(header[:4], walMagic)
	header[4] = newVersion
	binary.LittleEndian.PutUint32(header[5:], payloadLen)
	sealed := c.aead.Seal(nonce, nonce, plaintext, header[:])
	return sealed, newVersion, nil
}

func (c *walFrameCipher) open(payload []byte, version uint8) ([]byte, error) {
	if c == nil {
		return nil, ErrWALKeyRequired
	}
	if !walFrameEncrypted(version) || len(payload) < walEncryptedFrameOverhead || len(payload) > int(walFramePayloadLimit(version)) {
		return nil, ErrWALCorrupted
	}
	var header [9]byte
	binary.LittleEndian.PutUint32(header[:4], walMagic)
	header[4] = version
	binary.LittleEndian.PutUint32(header[5:], uint32(len(payload)))
	plaintext, err := c.aead.Open(nil, payload[:chacha20poly1305.NonceSizeX], payload[chacha20poly1305.NonceSizeX:], header[:])
	if err != nil {
		return nil, fmt.Errorf("%w: frame authentication failed", ErrWALCorrupted)
	}
	return plaintext, nil
}
