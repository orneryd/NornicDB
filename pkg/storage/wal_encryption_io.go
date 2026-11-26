package storage

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
)

// This file is the one integration point between the encrypted frame codec
// (wal_encryption.go) and the WAL's framing owners. NewWAL builds one
// walFrameReader and one frame writer from the configured key; every frame the
// journal writes, appends, rewrites or reads passes through them, so no path
// can mix a plaintext frame into an encrypted journal or read ciphertext
// without the key.
//
// The frame envelope is unchanged: magic, version, payload length, CRC32 of the
// stored payload, trailer and alignment stay where they were. Only the payload
// and the version byte differ: an encrypted frame holds
// [nonce:24][ciphertext+tag] from walFrameCipher.seal and carries version 8 (or
// 9 for a spanned part). The CRC and trailer therefore still describe the bytes
// on disk, while the AEAD authenticates the plaintext.

const (
	// WALEncryptionKeySize is the size of the XChaCha20-Poly1305 key
	// WALConfig.EncryptionKey accepts. A non-empty key of any other size is
	// refused before a journal is opened or converted.
	WALEncryptionKeySize = 32
)

var (
	// ErrWALEncryptionKeyInvalid reports a WAL key that is not exactly
	// WALEncryptionKeySize bytes.
	ErrWALEncryptionKeyInvalid = errors.New("wal: expected 32-byte encryption key")
	// ErrWALPlaintextFrames reports a plaintext frame in a journal opened,
	// read or converted with a key. Such a journal must be converted first
	// (EncryptWALDirectory); it is never rewritten by the attempt.
	ErrWALPlaintextFrames = errors.New("wal: plaintext frames in an encrypted WAL")
)

// walFrameReader is how one frame-reading path handles the frame formats it
// meets. The zero value is the plaintext reader every existing path uses. A
// reader built from a configured key reads encrypted frames and refuses
// plaintext, so one journal is read in exactly one format.
type walFrameReader struct {
	cipher *walFrameCipher
}

// walConfigCipher returns the frame cipher a configured key selects. A nil or
// empty key keeps the journal's plaintext format.
func walConfigCipher(key []byte) (*walFrameCipher, error) {
	if len(key) == 0 {
		return nil, nil
	}
	if len(key) != WALEncryptionKeySize {
		return nil, fmt.Errorf("%w, got %d", ErrWALEncryptionKeyInvalid, len(key))
	}
	return newWALFrameCipher(key)
}

func walFrameReaderForKey(key []byte) (walFrameReader, error) {
	cipher, err := walConfigCipher(key)
	if err != nil {
		return walFrameReader{}, err
	}
	return walFrameReader{cipher: cipher}, nil
}

// version refuses a frame version this reader cannot read. A reader without a
// key refuses ciphertext, and a reader with a key refuses plaintext: it is
// never read as a record of an operation it was not written as.
func (r walFrameReader) version(version uint8) error {
	if walFrameEncrypted(version) {
		if r.cipher == nil {
			return fmt.Errorf("%w: frame version %d is encrypted", ErrWALKeyRequired, version)
		}
		return nil
	}
	if !walFrameVersionReadable(version) {
		return unsupportedWALVersion(version)
	}
	if r.cipher != nil {
		return fmt.Errorf("%w: frame version %d is plaintext", ErrWALPlaintextFrames, version)
	}
	return nil
}

// plaintext returns the entry payload a complete stored frame holds and the
// version that payload is decoded with. The payload returned for an encrypted
// frame is owned by the caller; a plaintext payload is the stored slice.
func (r walFrameReader) plaintext(payload []byte, version uint8) ([]byte, uint8, error) {
	if err := r.version(version); err != nil {
		return nil, 0, err
	}
	if !walFrameEncrypted(version) {
		return payload, version, nil
	}
	opened, err := r.cipher.open(payload, version)
	if err != nil {
		return nil, 0, err
	}
	return opened, walPlaintextFrameVersion(version), nil
}

// walFrameOversized reports whether a stored frame payload is over the bound
// its version admits. An encrypted frame carries one nonce and tag more than
// the 16 MiB plaintext bound; only an over-one-frame plaintext audit record has
// the reader's historical exception.
func walFrameOversized(payloadLen uint32, version uint8) bool {
	return payloadLen > walFramePayloadLimit(version)
}

// walFrameAlignedLen is the aligned length the frame of one payload is stored
// as when cipher writes it: a non-nil cipher seals the payload, so the stored
// frame carries one nonce and tag more. Maintenance that rewrites a frame in
// place uses it to prove the replacement has the length it replaced.
func walFrameAlignedLen(cipher *walFrameCipher, payloadLen int, version uint8) int64 {
	stored := int64(payloadLen)
	if cipher != nil || walFrameEncrypted(version) {
		stored += walEncryptedFrameOverhead
	}
	return alignUp(9 + stored + 4 + 8)
}

// writeWALFrame writes one frame through the cipher a journal was opened with.
// A nil cipher writes the plaintext frame the journal always wrote; a
// configured cipher seals the payload and writes it under the encrypted frame
// version. No caller decides the envelope: this is its one owner.
func writeWALFrame(cipher *walFrameCipher, writer io.Writer, payload []byte, version uint8) (int64, error) {
	if cipher == nil {
		if walFrameEncrypted(version) {
			return 0, fmt.Errorf("%w: cannot write encrypted frame %d", ErrWALKeyRequired, version)
		}
		return writeAtomicRecordBufio(writer, payload, version)
	}
	sealed, sealedVersion, err := cipher.seal(payload, version)
	if err != nil {
		return 0, err
	}
	return writeAtomicRecordBufio(writer, sealed, sealedVersion)
}

// writeFrame is this WAL instance's one frame writer: every record it appends,
// every property-authority control it writes and every file it rewrites for
// truncation passes here, so a configured key covers all of them.
func (w *WAL) writeFrame(writer io.Writer, payload []byte, version uint8) (int64, error) {
	return writeWALFrame(w.cipher, writer, payload, version)
}

// frameReader is the reader matching the format this WAL writes.
func (w *WAL) frameReader() walFrameReader { return walFrameReader{cipher: w.cipher} }

// VisitEntriesAfter visits retained entries through this WAL's configured
// frame reader, including encrypted frames, without exposing its key.
func (w *WAL) VisitEntriesAfter(ctx context.Context, afterSeq uint64, visit func(WALEntry) error) error {
	return visitWALEntriesAfterFromDir(ctx, w.config.Dir, afterSeq, w.frameReader(), visit)
}

// ValidateWALFormatWithKey is ValidateWALFormat for a journal written with key.
// An empty key validates a plaintext journal. The format check authenticates
// every complete encrypted frame, so a wrong key is refused here instead of at
// the first replay.
func ValidateWALFormatWithKey(dir string, mode WALFormatValidation, key []byte) error {
	reader, err := walFrameReaderForKey(key)
	if err != nil {
		return err
	}
	return validateWALFormat(dir, mode, nil, reader)
}

// ReadWALEntriesWithKey is ReadWALEntries for a journal written with key.
func ReadWALEntriesWithKey(ctx context.Context, walPath string, logger *slog.Logger, key []byte) ([]WALEntry, error) {
	reader, err := walFrameReaderForKey(key)
	if err != nil {
		return nil, err
	}
	return readWALEntries(ctx, walPath, logger, reader)
}

// ReadWALEntriesFromDirWithKey is ReadWALEntriesFromDir for retained segments
// and an active file written with key.
func ReadWALEntriesFromDirWithKey(ctx context.Context, walDir string, key []byte) ([]WALEntry, error) {
	reader, err := walFrameReaderForKey(key)
	if err != nil {
		return nil, err
	}
	return readWALEntriesFromDir(ctx, walDir, reader)
}

// VisitWALEntriesAfterFromDirWithKey is VisitWALEntriesAfterFromDir for a
// journal written with key.
func VisitWALEntriesAfterFromDirWithKey(ctx context.Context, walDir string, afterSeq uint64, key []byte, visit func(WALEntry) error) error {
	reader, err := walFrameReaderForKey(key)
	if err != nil {
		return err
	}
	return visitWALEntriesAfterFromDir(ctx, walDir, afterSeq, reader, visit)
}

// ReadWALEntriesAfterWithKey is ReadWALEntriesAfter for a journal written with
// key.
func ReadWALEntriesAfterWithKey(ctx context.Context, path string, afterSeq uint64, scope WALReadScope, key []byte) ([]WALEntry, error) {
	reader, err := walFrameReaderForKey(key)
	if err != nil {
		return nil, err
	}
	return readWALEntriesAfter(ctx, path, afterSeq, scope, reader)
}

// CheckWALIntegrityWithKey is CheckWALIntegrity for a journal written with key.
// A frame that does not authenticate under the key is reported as the error it
// is, never as a damaged record the check may count and continue past.
func CheckWALIntegrityWithKey(walPath string, key []byte) (*WALIntegrityReport, error) {
	reader, err := walFrameReaderForKey(key)
	if err != nil {
		return nil, err
	}
	return checkWALIntegrity(walPath, reader)
}

// walFileEncrypted reports whether the journal file at path stores encrypted
// frames, from its first frame. An empty file is a plaintext journal with no
// records; a file whose header is not a WAL frame is refused exactly as its
// readers refuse it.
func walFileEncrypted(path string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()
	var header [5]byte
	n, readErr := io.ReadFull(bufio.NewReader(file), header[:])
	if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
		return false, readErr
	}
	if n == 0 {
		return false, nil
	}
	if n < 4 || binary.LittleEndian.Uint32(header[:4]) != walMagic {
		return false, fmt.Errorf("%w: unrecognized WAL header in %s", ErrWALUnsupportedVersion, path)
	}
	return n >= 5 && walFrameEncrypted(header[4]), nil
}
