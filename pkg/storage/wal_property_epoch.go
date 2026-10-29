package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/orneryd/nornicdb/internal/jsontext"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/orneryd/nornicdb/pkg/config"
)

// Property authority controls use the same current framing as data records.
// A clean stop seals their byte and sequence boundaries without downgrading.
const walFormatVersionPropertyEpoch uint8 = walFormatVersionCurrent

var walPropertyEpochPrefix = []byte(`{"nornic_property_epoch":1,`)

type walPropertyEpochFrame struct {
	Format        int               `json:"nornic_property_epoch"`
	Epoch         *WALPropertyEpoch `json:"authority"`
	Through       string            `json:"through"`
	Last          string            `json:"last"`
	through, last uint64
}

func isWALPropertyEpoch(entry WALEntry) bool {
	return entry.Operation == OpCheckpoint && bytes.HasPrefix(entry.Data, walPropertyEpochPrefix)
}

func decodeWALPropertyEpochFrame(entry WALEntry) (walPropertyEpochFrame, error) {
	var frame walPropertyEpochFrame
	if !isWALPropertyEpoch(entry) {
		return frame, fmt.Errorf("wal: invalid property epoch control")
	}
	if err := jsontext.Unmarshal(entry.Data, &frame); err != nil {
		return frame, err
	}
	if frame.Format != 1 {
		return frame, fmt.Errorf("wal: invalid property epoch format")
	}
	var err error
	frame.through, err = strconv.ParseUint(frame.Through, 16, 64)
	if err != nil {
		return frame, fmt.Errorf("wal: invalid property epoch byte boundary: %w", err)
	}
	frame.last, err = strconv.ParseUint(frame.Last, 16, 64)
	if err != nil {
		return frame, fmt.Errorf("wal: invalid property epoch sequence boundary: %w", err)
	}
	if entry.framingVersion != walFormatVersionPropertyEpoch && (frame.through == ^uint64(0) || frame.last == ^uint64(0)) {
		return frame, fmt.Errorf("wal: unsealed property epoch requires current framing version %d", walFormatVersionCurrent)
	}
	return frame, nil
}

// This is the base buffered wire layout. Recovery's compatibility/provenance
// fields are deliberately absent, including from JSON reflection and the
// per-record allocation. Only AppendBufferedCommit constructs this payload.
type walBufferedCommitPayload struct {
	LogicalTxID     string            `json:"logical_tx_id,omitempty"`
	TxID            string            `json:"tx_id"`
	Namespace       string            `json:"namespace,omitempty"`
	Ops             []Operation       `json:"ops"`
	Writes          map[string][]byte `json:"writes,omitempty"`
	Deletes         []string          `json:"deletes,omitempty"`
	Version         MVCCVersion       `json:"version"`
	NodeMax         uint64            `json:"node_max,omitempty"`
	EdgeMax         uint64            `json:"edge_max,omitempty"`
	HasCounters     bool              `json:"has_counters,omitempty"`
	NumericBindings []byte            `json:"numeric_bindings,omitempty"`
	PropKeys        []WALPropKeyEntry `json:"prop_keys,omitempty"`
	PropKeyCounters map[string]uint64 `json:"prop_key_counters,omitempty"`
}

// WALPropertyEpoch transports the explicit dictionary authority attached by a
// WAL reader. Its immutable state is private: JSON decoding replaces the state
// rather than mutating a plan already in use. Ordinary buffered record bytes
// contain no epoch field; file readers obtain it from the preceding header.
type WALPropertyEpoch struct {
	state *walPropertyEpochState
}

type walPropertyEpochState struct {
	Version      int    `json:"version"`
	ID           string `json:"epoch"`
	DictionaryID string `json:"dictionary_id"`
	Bindings     []byte `json:"bindings"`
}

func (e *WALPropertyEpoch) MarshalJSON() ([]byte, error) { return jsontext.Marshal(e.state) }

func (e *WALPropertyEpoch) UnmarshalJSON(data []byte) error {
	var state walPropertyEpochState
	if err := jsontext.Unmarshal(data, &state); err != nil {
		return err
	}
	if state.Version != 1 || state.ID == "" || state.DictionaryID == "" || len(state.Bindings) == 0 {
		return fmt.Errorf("wal: invalid property dictionary epoch")
	}
	e.state = &state
	return nil
}

// This is the one reader of file-level property authority, shared by atomic
// and salvage readers. Every file starts unbound. Sealed and
// active segments carry their own complete header, so retention cannot make a
// segment depend on an earlier file. Headers do not consume operation sequences.
func withWALPropertyEpochs(visit func(WALEntry) error) func(WALEntry) error {
	var epoch *WALPropertyEpoch
	var through, last uint64
	return func(entry WALEntry) error {
		if entry.propertyScopeReset {
			epoch = nil
			return nil
		}
		if isWALPropertyEpoch(entry) {
			frame, err := decodeWALPropertyEpochFrame(entry)
			if err != nil {
				return err
			}
			through, last = frame.through, frame.last
			epoch = frame.Epoch
			return nil
		}
		if epoch != nil && (entry.Sequence > last || entry.recordEnd > through) {
			epoch = nil
		}
		if entry.Operation == OpBufferedCommit && entry.PropertyEpoch == nil {
			entry.PropertyEpoch = epoch
		}
		return visit(entry)
	}
}

// Capture runs only on attachment, dictionary change or snapshot boundaries.
// No property body is inspected and no names are encoded on steady append.
func (d *propertyKeyDictionary) captureWALEpoch() (*WALPropertyEpoch, uint64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	identity := d.walIdentity.Load()
	if identity == nil {
		return nil, 0, fmt.Errorf("wal: property dictionary identity is uninitialized")
	}
	bindings := []byte{1}
	for namespace, reverse := range d.reverse {
		for id, name := range reverse {
			bindings = appendWALPropertyBinding(bindings, namespace, id, name)
		}
	}
	compressed := compressWALBufferedCommitPayload(bindings)
	if len(compressed) < len(bindings) {
		bindings = append([]byte{2}, compressed...)
	}
	return &WALPropertyEpoch{state: &walPropertyEpochState{Version: 1, ID: uuid.NewString(), DictionaryID: *identity, Bindings: bindings}}, d.walEpochRevision.Load(), nil
}

// A caller already owns w.mu. Headers are written before their first body and
// are flushed by the same forced flush that precedes acknowledgement.
func encodeWALPropertyEpochFrame(frame walPropertyEpochFrame, sequence uint64, timestamp time.Time) ([]byte, error) {
	data, err := jsontext.Marshal(frame)
	if err != nil {
		return nil, err
	}
	stamp, err := timestamp.MarshalJSON()
	if err != nil {
		return nil, err
	}
	// Fixed width numeric fields keep every metadata frame's byte length
	// unchanged when sealing its sequence/checksum. JSON permits this whitespace.
	header := []byte(fmt.Sprintf(`{"seq":%20d,"ts":%s,"op":"checkpoint","data":null,"checksum":%10d}`, sequence, stamp, crc32Checksum(data)))
	payload := make([]byte, 8+len(header)+len(data))
	binary.LittleEndian.PutUint32(payload, uint32(len(header)))
	copy(payload[4:], header)
	binary.LittleEndian.PutUint32(payload[4+len(header):], uint32(len(data)))
	copy(payload[8+len(header):], data)
	if len(payload) > int(walMaxEntrySize) {
		return nil, fmt.Errorf("wal: property dictionary epoch exceeds maximum frame size")
	}
	return payload, nil
}

func writeWALPropertyEpoch(writer *bufio.Writer, epoch *WALPropertyEpoch, sequence uint64) (int64, error) {
	frame := walPropertyEpochFrame{Format: 1, Epoch: epoch, Through: "ffffffffffffffff", Last: "ffffffffffffffff"}
	payload, err := encodeWALPropertyEpochFrame(frame, sequence, time.Now())
	if err != nil {
		return 0, err
	}
	return writeAtomicRecordBufio(writer, payload, walFormatVersionPropertyEpoch)
}

// walPropertyEpochUnsealedName marks a log that may hold v4 controls. It is
// durable before the first such frame and removed only after every file has
// been converted, so an ordinary synchronous log is never scanned at close.
const walPropertyEpochUnsealedName = "property-epoch.unsealed"

func walPropertyEpochUnsealedPath(dir string) string {
	return filepath.Join(dir, walPropertyEpochUnsealedName)
}

// Caller owns w.mu. Every v4 frame, appended or retained by compaction, is
// written here, after the marker is durable.
func (w *WAL) writePropertyEpochFrameLocked(writer *bufio.Writer, epoch *WALPropertyEpoch, sequence uint64) (int64, error) {
	if !w.propertyEpochUnsealed {
		file, err := os.OpenFile(walPropertyEpochUnsealedPath(w.config.Dir), os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return 0, err
		}
		if err := file.Close(); err != nil {
			return 0, err
		}
		if err := syncDir(w.config.Dir); err != nil {
			return 0, err
		}
		w.propertyEpochUnsealed = true
	}
	return writeWALPropertyEpoch(writer, epoch, sequence)
}

// Caller owns w.mu and has converted every file.
func (w *WAL) clearPropertyEpochUnsealedLocked() error {
	if !w.propertyEpochUnsealed {
		return nil
	}
	if err := os.Remove(walPropertyEpochUnsealedPath(w.config.Dir)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := syncDir(w.config.Dir); err != nil {
		return err
	}
	w.propertyEpochUnsealed = false
	return nil
}

func (w *WAL) writePropertyEpochLocked(epoch *WALPropertyEpoch) error {
	n, err := w.writePropertyEpochFrameLocked(w.writer, epoch, w.sequence.Load())
	if err != nil {
		return err
	}
	w.bytes.Add(n)
	w.segmentBytes += n
	w.propertyEpoch = epoch
	w.propertyEpochKnown = true
	return nil
}

func (w *WAL) ensurePropertyEpochLocked(dictionary *propertyKeyDictionary, boundary bool) error {
	if w.propertyEpochError != nil {
		return w.propertyEpochError
	}
	if w.closed.Load() {
		return ErrWALClosed
	}
	if dictionary == nil {
		return fmt.Errorf("wal: property dictionary is required for buffered append")
	}
	if !boundary && w.propertyEpochKnown && w.propertyEpoch != nil && w.propertyDictionary == dictionary && w.propertyEpochRevision == dictionary.walEpochRevision.Load() {
		return nil
	}
	if err := w.prepareEpochFormatLocked(); err != nil {
		return err
	}
	epoch, revision, err := dictionary.captureWALEpoch()
	if err != nil {
		return err
	}
	if err := w.writePropertyEpochLocked(epoch); err != nil {
		return err
	}
	w.propertyDictionary, w.propertyEpochRevision = dictionary, revision
	return nil
}

// SetBufferedCommitWAL has no error result. Attach the initial header here and
// retain an I/O failure for the next append to return before acknowledgement.
// The engine caller holds its existing lifetime admission while attaching.
func (w *WAL) bindPropertyDictionary(dictionary *propertyKeyDictionary) {
	if w == nil || !config.IsWALEnabled() {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.ensurePropertyEpochLocked(dictionary, true); err != nil {
		w.propertyEpochError = err
		return
	}
	w.propertyEpochError = w.writer.Flush()
}

// Invalidate a previous clean-stop boundary before replacing the native
// dictionary or accepting a new foreign source. Even if the next body never
// arrives, an old binary must not reopen an unclean import against old v3 logs.
func (w *WAL) invalidateCleanPropertyEpoch() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed.Load() {
		return ErrWALClosed
	}
	if err := w.prepareEpochFormatLocked(); err != nil {
		return err
	}
	if err := w.writePropertyEpochLocked(nil); err != nil {
		return err
	}
	return w.syncLocked()
}

// Generic writers carry named operations or historical payloads, not bodies
// allocated by the attached dictionary. They must not inherit its authority.
func (w *WAL) prepareUnboundWALRecordLocked(op OperationType) error {
	if err := w.prepareEpochFormatLocked(); err != nil {
		return err
	}
	if op == OpBufferedCommit && (!w.propertyEpochKnown || w.propertyEpoch != nil) {
		return w.writePropertyEpochLocked(nil)
	}
	if op == OpCheckpoint && w.propertyDictionary != nil {
		return w.ensurePropertyEpochLocked(w.propertyDictionary, true)
	}
	return nil
}

// Compaction transports the authority together with retained entries. This
// shares the header writer with append/rotation instead of embedding a table
// back into every retained physical body.
func (w *WAL) writeRetainedPropertyEpochLocked(writer *bufio.Writer, previous **WALPropertyEpoch, entry WALEntry) (int64, error) {
	if entry.Operation != OpBufferedCommit || entry.PropertyEpoch == *previous {
		return 0, nil
	}
	n, err := w.writePropertyEpochFrameLocked(writer, entry.PropertyEpoch, entry.Sequence)
	if err == nil {
		*previous = entry.PropertyEpoch
	}
	return n, err
}

// Check the active file's format once per open before appending. Recognizing
// a nonempty atomic log does not establish its last dictionary epoch.
func (w *WAL) prepareEpochFormatLocked() error {
	if w.propertyEpochFormatChecked || w.file == nil {
		return nil
	}
	if err := validateWALFileVersion(walActivePath(w.config.Dir), false); err != nil {
		return err
	}
	info, err := os.Stat(walActivePath(w.config.Dir))
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		w.propertyEpochKnown = true
	}
	w.propertyEpochFormatChecked = true
	return nil
}
