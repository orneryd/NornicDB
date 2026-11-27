package storage

// Snapshot-at-rest encryption for NDS2 streaming snapshots. The envelope is a
// chunked AES-256-GCM stream written directly to the snapshot's temporary
// file: the plaintext NDS2 frames pass through this writer as they are
// produced, so no plaintext snapshot file exists on disk at any point.
//
// Envelope layout (all integers little endian):
//
//	magic "NDSE" (4) | version (1) | random nonce prefix (8) | chunk size (4)
//	frame*            data chunks and one final completion chunk
//
// A frame is a uint32 ciphertext length followed by that many ciphertext bytes.
// The GCM tag is 16 bytes, so a data frame carries 1..chunkSize plaintext bytes
// and the completion frame carries an empty plaintext. Every frame's nonce is
// the per-file random 8-byte prefix followed by the 4-byte big-endian chunk
// counter, and every frame authenticates the header, its counter and its kind
// as additional data. A wrong key, a tampered header or chunk, a reordered,
// replayed, dropped or truncated chunk, and a stream without the final
// completion frame all fail the read instead of returning partial data. A
// caller that supplies a key never reads an older plaintext snapshot: the
// envelope magic is absent, so the key-aware readers refuse it.

import (
	"bufio"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
)

const (
	// SnapshotEncryptionKeySize is the size of the AES-256 key SnapshotOptions
	// accepts. A non-empty key of any other size is refused before a snapshot
	// is written or read.
	SnapshotEncryptionKeySize = 32

	encryptedSnapshotMagic          = "NDSE"
	encryptedSnapshotVersion        = byte(1)
	snapshotNoncePrefixSize         = 8
	snapshotNonceSize               = 12
	snapshotChunkKindData           = byte(0)
	snapshotChunkKindFinal          = byte(1)
	snapshotEncryptionChunkSize     = 64 << 10
	maxSnapshotEncryptionChunkSize  = 1 << 20
	encryptedSnapshotHeaderSize     = len(encryptedSnapshotMagic) + 1 + snapshotNoncePrefixSize + 4
	encryptedSnapshotAADSize        = encryptedSnapshotHeaderSize + 4 + 1
	encryptedSnapshotChunkSizeField = encryptedSnapshotHeaderSize - 4
	encryptedSnapshotPrefixField    = len(encryptedSnapshotMagic) + 1
)

var (
	// ErrSnapshotEncryptionKeyInvalid reports a key that is not exactly
	// SnapshotEncryptionKeySize bytes.
	ErrSnapshotEncryptionKeyInvalid = errors.New("snapshot encryption key must be 32 bytes")
	// ErrSnapshotEncryptionRequired reports an encrypted snapshot read without
	// a key.
	ErrSnapshotEncryptionRequired = errors.New("encrypted snapshot requires a snapshot encryption key")
	// ErrSnapshotEncryptionNotEncrypted reports a plaintext snapshot presented
	// to a key-aware reader (or an envelope without its magic).
	ErrSnapshotEncryptionNotEncrypted = errors.New("snapshot is not an encrypted NDS2 stream")
	// ErrSnapshotEncryptionFormat reports an envelope that cannot be framed:
	// a short or invalid header, an invalid chunk length, a missing completion
	// frame or trailing data.
	ErrSnapshotEncryptionFormat = errors.New("invalid encrypted snapshot envelope")
	// ErrSnapshotEncryptionAuthentication reports a chunk that does not
	// authenticate under the supplied key and its header, counter and kind.
	ErrSnapshotEncryptionAuthentication = errors.New("encrypted snapshot authentication failed")

	errSnapshotNonceExhausted = errors.New("encrypted snapshot nonce space exhausted")
)

// IsEncryptedSnapshot reports whether the snapshot file at path carries the
// encrypted envelope. A missing file returns the open error, like
// IsStreamingSnapshot; a file too short to hold the magic is not encrypted.
func IsEncryptedSnapshot(path string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()
	header := make([]byte, len(encryptedSnapshotMagic))
	if _, err := io.ReadFull(file, header); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return false, nil
		}
		return false, err
	}
	return string(header) == encryptedSnapshotMagic, nil
}

// NewSnapshotDecryptingReader authenticates and decrypts a chunked snapshot
// envelope, returning the plaintext NDS2 stream. The reader fails closed: the
// key must be SnapshotEncryptionKeySize bytes, the envelope header must be
// present, every chunk must authenticate in order, and the final completion
// frame must be present before io.EOF is returned. A plaintext snapshot is
// refused rather than read, and the first chunk is authenticated before the
// reader is returned so a wrong key is reported as such.
func NewSnapshotDecryptingReader(reader io.Reader, key []byte) (io.Reader, error) {
	if len(key) == 0 {
		return nil, ErrSnapshotEncryptionRequired
	}
	return newSnapshotEnvelopeReader(reader, key)
}

// ReadSnapshotWithKey reads a framed snapshot with the snapshot-at-rest key
// contract. With no key it reads the plaintext NDS2 stream exactly as
// ReadSnapshot does. With a key it reads only an encrypted envelope and
// refuses a plaintext snapshot, so switching a database to encryption never
// silently recovers an older plaintext snapshot.
func ReadSnapshotWithKey(ctx context.Context, reader io.Reader, visitor SnapshotVisitor, key []byte) (SnapshotMetadata, error) {
	buffered := bufio.NewReader(reader)
	magic, peekErr := buffered.Peek(len(encryptedSnapshotMagic))
	if peekErr != nil && !errors.Is(peekErr, io.EOF) {
		return SnapshotMetadata{}, peekErr
	}
	encrypted := peekErr == nil && string(magic) == encryptedSnapshotMagic
	if len(key) == 0 {
		if encrypted {
			return SnapshotMetadata{}, ErrSnapshotEncryptionRequired
		}
		return readSnapshot(ctx, buffered, visitor, nil, materializedSnapshotRecordSize)
	}
	if len(key) != SnapshotEncryptionKeySize {
		return SnapshotMetadata{}, ErrSnapshotEncryptionKeyInvalid
	}
	if !encrypted {
		return SnapshotMetadata{}, fmt.Errorf("%w: refusing to read a plaintext snapshot with an encryption key", ErrSnapshotEncryptionNotEncrypted)
	}
	decrypted, err := newSnapshotEnvelopeReader(buffered, key)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	return readSnapshot(ctx, decrypted, visitor, nil, materializedSnapshotRecordSize)
}

func snapshotEnvelopeGCM(key []byte) (cipher.AEAD, error) {
	if len(key) != SnapshotEncryptionKeySize {
		return nil, ErrSnapshotEncryptionKeyInvalid
	}
	// Badger and WAL use the same installation secret. A separate snapshot
	// subkey keeps their nonce domains independent of this envelope.
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte("nornicdb/snapshot/encryption/v1"))
	block, err := aes.NewCipher(mac.Sum(nil))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSnapshotEncryptionKeyInvalid, err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSnapshotEncryptionKeyInvalid, err)
	}
	return gcm, nil
}

// snapshotEnvelopeWriter encrypts one plaintext snapshot stream. It buffers at
// most one chunk of plaintext and seals at most one chunk of ciphertext at a
// time, so memory use is bounded independently of the snapshot size.
type snapshotEnvelopeWriter struct {
	dst       io.Writer
	gcm       cipher.AEAD
	header    [encryptedSnapshotHeaderSize]byte
	aad       [encryptedSnapshotAADSize]byte
	nonce     [snapshotNonceSize]byte
	chunkSize int
	plaintext []byte
	sealed    []byte
	counter   uint32
	finished  bool
	err       error
}

func newSnapshotEnvelopeWriter(dst io.Writer, key []byte) (*snapshotEnvelopeWriter, error) {
	gcm, err := snapshotEnvelopeGCM(key)
	if err != nil {
		return nil, err
	}
	writer := &snapshotEnvelopeWriter{
		dst:       dst,
		gcm:       gcm,
		chunkSize: snapshotEncryptionChunkSize,
		plaintext: make([]byte, 0, snapshotEncryptionChunkSize),
		sealed:    make([]byte, 0, snapshotEncryptionChunkSize+gcm.Overhead()),
	}
	copy(writer.header[:], encryptedSnapshotMagic)
	writer.header[len(encryptedSnapshotMagic)] = encryptedSnapshotVersion
	if _, err := rand.Read(writer.header[encryptedSnapshotPrefixField : encryptedSnapshotPrefixField+snapshotNoncePrefixSize]); err != nil {
		return nil, fmt.Errorf("encrypted snapshot nonce prefix: %w", err)
	}
	binary.LittleEndian.PutUint32(writer.header[encryptedSnapshotChunkSizeField:], uint32(writer.chunkSize))
	copy(writer.nonce[:snapshotNoncePrefixSize], writer.header[encryptedSnapshotPrefixField:])
	copy(writer.aad[:encryptedSnapshotHeaderSize], writer.header[:])
	if _, err := writer.dst.Write(writer.header[:]); err != nil {
		return nil, fmt.Errorf("encrypted snapshot header: %w", err)
	}
	return writer, nil
}

func (w *snapshotEnvelopeWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	written := 0
	for len(p) > 0 {
		if len(w.plaintext) == w.chunkSize {
			if err := w.flushData(); err != nil {
				w.err = err
				return written, err
			}
			continue
		}
		n := copy(w.plaintext[len(w.plaintext):w.chunkSize], p)
		w.plaintext = w.plaintext[:len(w.plaintext)+n]
		p = p[n:]
		written += n
	}
	return written, nil
}

func (w *snapshotEnvelopeWriter) flushData() error {
	if len(w.plaintext) == 0 {
		return nil
	}
	if err := w.writeChunk(w.plaintext, snapshotChunkKindData); err != nil {
		return err
	}
	w.plaintext = w.plaintext[:0]
	return nil
}

// Finish writes the remaining plaintext chunk and the authenticated completion
// frame. It is the snapshot stream's end-of-file; a reader that does not see it
// fails instead of returning a truncated snapshot.
func (w *snapshotEnvelopeWriter) Finish() error {
	if w.err != nil {
		return w.err
	}
	if w.finished {
		return nil
	}
	if err := w.flushData(); err != nil {
		w.err = err
		return err
	}
	if err := w.writeChunk(nil, snapshotChunkKindFinal); err != nil {
		w.err = err
		return err
	}
	w.finished = true
	return nil
}

func (w *snapshotEnvelopeWriter) writeChunk(plaintext []byte, kind byte) error {
	if w.counter == math.MaxUint32 {
		return errSnapshotNonceExhausted
	}
	binary.BigEndian.PutUint32(w.nonce[snapshotNoncePrefixSize:], w.counter)
	binary.BigEndian.PutUint32(w.aad[encryptedSnapshotHeaderSize:], w.counter)
	w.aad[encryptedSnapshotHeaderSize+4] = kind
	sealed := w.gcm.Seal(w.sealed[:0], w.nonce[:], plaintext, w.aad[:])
	w.sealed = sealed
	var length [4]byte
	binary.LittleEndian.PutUint32(length[:], uint32(len(sealed)))
	if _, err := w.dst.Write(length[:]); err != nil {
		return err
	}
	if _, err := w.dst.Write(sealed); err != nil {
		return err
	}
	w.counter++
	return nil
}

// snapshotEnvelopeReader decrypts one envelope with at most one frame of
// ciphertext and one frame of plaintext resident.
type snapshotEnvelopeReader struct {
	src       *bufio.Reader
	gcm       cipher.AEAD
	header    [encryptedSnapshotHeaderSize]byte
	aad       [encryptedSnapshotAADSize]byte
	nonce     [snapshotNonceSize]byte
	chunkSize int
	plaintext []byte
	frame     []byte
	counter   uint32
	offset    int
	partial   bool
	done      bool
}

func newSnapshotEnvelopeReader(reader io.Reader, key []byte) (*snapshotEnvelopeReader, error) {
	gcm, err := snapshotEnvelopeGCM(key)
	if err != nil {
		return nil, err
	}
	decrypting := &snapshotEnvelopeReader{src: bufio.NewReader(reader), gcm: gcm}
	if _, err := io.ReadFull(decrypting.src, decrypting.header[:]); err != nil {
		return nil, fmt.Errorf("%w: incomplete header: %v", ErrSnapshotEncryptionFormat, err)
	}
	if string(decrypting.header[:len(encryptedSnapshotMagic)]) != encryptedSnapshotMagic {
		return nil, fmt.Errorf("%w: envelope magic is absent", ErrSnapshotEncryptionNotEncrypted)
	}
	if version := decrypting.header[len(encryptedSnapshotMagic)]; version != encryptedSnapshotVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrSnapshotEncryptionFormat, version)
	}
	chunkSize := int(binary.LittleEndian.Uint32(decrypting.header[encryptedSnapshotChunkSizeField:]))
	if chunkSize <= 0 || chunkSize > maxSnapshotEncryptionChunkSize {
		return nil, fmt.Errorf("%w: invalid chunk size %d", ErrSnapshotEncryptionFormat, chunkSize)
	}
	decrypting.chunkSize = chunkSize
	copy(decrypting.nonce[:snapshotNoncePrefixSize], decrypting.header[encryptedSnapshotPrefixField:])
	copy(decrypting.aad[:encryptedSnapshotHeaderSize], decrypting.header[:])
	// Authenticate the first chunk here, before a caller can read any plaintext:
	// a wrong key or a malformed first frame is reported as an envelope failure
	// instead of a generic NDS2 header error.
	if err := decrypting.readChunk(); err != nil {
		return nil, err
	}
	return decrypting, nil
}

func (r *snapshotEnvelopeReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if r.offset < len(r.plaintext) {
			n := copy(p, r.plaintext[r.offset:])
			r.offset += n
			return n, nil
		}
		if r.done {
			return 0, io.EOF
		}
		if err := r.readChunk(); err != nil {
			return 0, err
		}
	}
}

func (r *snapshotEnvelopeReader) readChunk() error {
	var lengthBytes [4]byte
	if _, err := io.ReadFull(r.src, lengthBytes[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("%w: stream ended before the completion frame", ErrSnapshotEncryptionFormat)
		}
		return fmt.Errorf("%w: chunk length: %v", ErrSnapshotEncryptionFormat, err)
	}
	length := int(binary.LittleEndian.Uint32(lengthBytes[:]))
	overhead := r.gcm.Overhead()
	if length < overhead || length > r.chunkSize+overhead {
		return fmt.Errorf("%w: invalid chunk length %d", ErrSnapshotEncryptionFormat, length)
	}
	if r.counter == math.MaxUint32 {
		return errSnapshotNonceExhausted
	}
	if cap(r.frame) < length {
		r.frame = make([]byte, length)
	}
	r.frame = r.frame[:length]
	if _, err := io.ReadFull(r.src, r.frame); err != nil {
		return fmt.Errorf("%w: truncated chunk: %v", ErrSnapshotEncryptionFormat, err)
	}
	kind := snapshotChunkKindData
	if length == overhead {
		kind = snapshotChunkKindFinal
	}
	binary.BigEndian.PutUint32(r.nonce[snapshotNoncePrefixSize:], r.counter)
	binary.BigEndian.PutUint32(r.aad[encryptedSnapshotHeaderSize:], r.counter)
	r.aad[encryptedSnapshotHeaderSize+4] = kind
	plaintext, err := r.gcm.Open(r.plaintext[:0], r.nonce[:], r.frame, r.aad[:])
	if err != nil {
		return fmt.Errorf("%w: %w", ErrSnapshotEncryptionAuthentication, err)
	}
	r.counter++
	r.offset = 0
	r.plaintext = plaintext
	if kind == snapshotChunkKindFinal {
		if len(plaintext) != 0 {
			return fmt.Errorf("%w: completion frame is not empty", ErrSnapshotEncryptionFormat)
		}
		if _, err := r.src.Peek(1); err != io.EOF {
			if err != nil {
				return fmt.Errorf("%w: after completion frame: %v", ErrSnapshotEncryptionFormat, err)
			}
			return fmt.Errorf("%w: trailing data after completion frame", ErrSnapshotEncryptionFormat)
		}
		r.done = true
		return nil
	}
	if len(plaintext) == 0 {
		return fmt.Errorf("%w: empty data chunk", ErrSnapshotEncryptionFormat)
	}
	if r.partial {
		return fmt.Errorf("%w: data chunk after a short chunk", ErrSnapshotEncryptionFormat)
	}
	if len(plaintext) < r.chunkSize {
		r.partial = true
	}
	return nil
}
