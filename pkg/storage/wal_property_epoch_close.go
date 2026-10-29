package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// walPropertyEpochSealPlan is one validated file and its open control frames.
type walPropertyEpochSealPlan struct {
	path     string
	end      int64
	last     uint64
	controls []walPropertyEpochSealControl
}

type walPropertyEpochSealControl struct {
	offset, length int64
	frame          walPropertyEpochFrame
	timestamp      time.Time
}

// A sealed checkpoint retains authority for later foreign snapshot recovery,
// but cannot lend it to an older writer's subsequent appends. Both its byte
// span and its last sequence are closed. Ordinary frames are copied verbatim.
// An old reader understands the checkpoint and ignores its private payload.
func sealWALPropertyEpochFile(path string) error {
	plan, err := scanWALPropertyEpochFile(path, nil)
	if err != nil {
		return err
	}
	return plan.apply()
}

// scanWALPropertyEpochFile validates one file and records its controls. The
// optional visit sees every decoded entry in the same read, so close-time
// checks need no second pass over the log.
func scanWALPropertyEpochFile(path string, visit func(WALEntry) error) (plan walPropertyEpochSealPlan, err error) {
	plan.path = path
	source, err := os.Open(path)
	if err != nil {
		return plan, err
	}
	defer source.Close()
	var magic [4]byte
	if n, readErr := source.ReadAt(magic[:], 0); readErr == io.EOF && n == 0 {
		return plan, nil
	} else if readErr != nil {
		return plan, readErr
	}
	if binary.LittleEndian.Uint32(magic[:]) != walMagic {
		return plan, fmt.Errorf("%w: unrecognized WAL header in %s", ErrWALUnsupportedVersion, path)
	}
	reader := bufio.NewReader(source)
	var header [9]byte
	var record []byte
	// Scan headers and checksums, never decode physical bodies or name maps.
	for {
		_, err := io.ReadFull(reader, header[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return plan, err
		}
		version := header[4]
		size := binary.LittleEndian.Uint32(header[5:])
		if version != walFormatVersionCurrent {
			return plan, unsupportedWALVersion(version)
		}
		if binary.LittleEndian.Uint32(header[:]) != walMagic || size > walMaxEntrySize {
			return plan, ErrWALCorrupted
		}
		length := int64(9 + size + 4)
		if version >= 2 {
			length = alignUp(length + 8)
		}
		if cap(record) < int(length) {
			record = make([]byte, length)
		} else {
			record = record[:length]
		}
		copy(record, header[:])
		if _, err := io.ReadFull(reader, record[9:]); err != nil {
			return plan, err
		}
		payload := record[9 : 9+size]
		if binary.LittleEndian.Uint32(record[9+size:]) != crc32Checksum(payload) {
			return plan, ErrWALChecksumFailed
		}
		if version >= 2 && binary.LittleEndian.Uint64(record[13+size:]) != walTrailer {
			return plan, ErrWALCorrupted
		}
		entry, err := WALEntry{}, error(nil)
		_, err = processWALEntryPayload(walPayloadDecode, nil, &entry, payload, version)
		if err != nil {
			return plan, err
		}
		if entry.Checksum != crc32Checksum(entry.Data) {
			return plan, ErrWALChecksumFailed
		}
		if entry.Sequence > plan.last {
			plan.last = entry.Sequence
		}
		if isWALPropertyEpoch(entry) {
			frame, err := decodeWALPropertyEpochFrame(entry)
			if err != nil {
				return plan, err
			}
			plan.controls = append(plan.controls, walPropertyEpochSealControl{plan.end, length, frame, entry.Timestamp})
		}
		if visit != nil {
			if err := visitWALEntryWithSequence(visit)(entry); err != nil {
				return plan, err
			}
		}
		plan.end += length
	}
	return plan, nil
}

// The directory visitor skips sequence zero; close-time checks keep that rule.
func visitWALEntryWithSequence(visit func(WALEntry) error) func(WALEntry) error {
	return func(entry WALEntry) error {
		if entry.Sequence == 0 {
			return nil
		}
		return visit(entry)
	}
}

func (plan walPropertyEpochSealPlan) apply() error {
	if len(plan.controls) == 0 {
		return nil
	}
	source, err := os.Open(plan.path)
	if err != nil {
		return err
	}
	defer source.Close()
	// The caller holds w.mu from scan to replacement; a size change means the
	// recorded control offsets no longer describe this file.
	if info, err := source.Stat(); err != nil {
		return err
	} else if info.Size() != plan.end {
		return fmt.Errorf("wal: property epoch file changed between scan and sealing")
	}
	// Same-directory replacement is atomic. A failed copy leaves the original
	// v4 file intact, so an older reader still refuses instead of misreading it.
	temp, err := os.CreateTemp(filepath.Dir(plan.path), ".wal-epoch-*.tmp")
	if err != nil {
		return err
	}
	defer func() { _ = temp.Close(); _ = os.Remove(temp.Name()) }()
	if _, err := io.Copy(temp, source); err != nil {
		return err
	}
	for _, c := range plan.controls {
		c.frame.Through, c.frame.Last = fmt.Sprintf("%016x", plan.end), fmt.Sprintf("%016x", plan.last)
		payload, err := encodeWALPropertyEpochFrame(c.frame, plan.last, c.timestamp)
		if err != nil {
			return err
		}
		if alignUp(int64(9+len(payload)+4+8)) != c.length {
			return fmt.Errorf("wal: property epoch sealing changed frame length")
		}
		if _, err := temp.Seek(c.offset, io.SeekStart); err != nil {
			return err
		}
		writer := bufio.NewWriter(temp)
		if _, err := writeAtomicRecordBufio(writer, payload, walFormatVersionCurrent); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}
	}
	if err := temp.Sync(); err != nil {
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := source.Close(); err != nil {
		return err
	}
	if err := os.Rename(temp.Name(), plan.path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(plan.path))
}

// Caller owns w.mu and has established a durable native store or snapshot.
// A log that never received a v4 control has nothing to convert.
func (w *WAL) sealPropertyEpochsLocked() error {
	if !w.propertyEpochUnsealed {
		return nil
	}
	plans, err := w.planPropertyEpochSealLocked(nil)
	if err != nil {
		return err
	}
	return w.applyPropertyEpochSealLocked(plans)
}

// planPropertyEpochSealLocked reads every file once, the active file last.
// Nothing is rewritten here, so a visitor can veto the whole conversion.
func (w *WAL) planPropertyEpochSealLocked(visit func(WALEntry) error) ([]walPropertyEpochSealPlan, error) {
	if !w.closed.Load() {
		if err := w.syncLocked(); err != nil {
			return nil, err
		}
	}
	manifest, err := loadWALManifest(w.config.Dir)
	if err != nil {
		return nil, err
	}
	plans := make([]walPropertyEpochSealPlan, 0, len(manifest.Segments)+1)
	for _, segment := range manifest.Segments {
		path, err := resolveWALSegmentPath(w.config.Dir, segment.Path)
		if err != nil {
			return nil, err
		}
		plan, err := scanWALPropertyEpochFile(path, visit)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	plan, err := scanWALPropertyEpochFile(walActivePath(w.config.Dir), visit)
	if err != nil {
		return nil, err
	}
	return append(plans, plan), nil
}

// Sealed files go first. Until all succeed, the active v4 file prevents an
// older binary from opening a partially converted directory.
func (w *WAL) applyPropertyEpochSealLocked(plans []walPropertyEpochSealPlan) error {
	active := plans[len(plans)-1]
	for _, plan := range plans[:len(plans)-1] {
		if err := plan.apply(); err != nil {
			return err
		}
	}
	// Release the append handle before replacement (also required on Windows).
	if !w.closed.Load() {
		if err := w.file.Close(); err != nil {
			return err
		}
	}
	err := active.apply()
	if err == nil {
		// Before reopening: a reopened write-behind log publishes a new epoch,
		// which sets the marker again.
		err = w.clearPropertyEpochUnsealedLocked()
	}
	if !w.closed.Load() {
		if reopenErr := w.reopenWAL(); reopenErr != nil {
			return reopenErr
		}
	}
	w.propertyEpoch, w.propertyEpochKnown = nil, false
	return err
}

// Close's lifetime owner has stopped admissions and waited for txnWG before
// calling this with its captured handle. No ensureOpen-then-use or nested
// write-barrier acquisition is involved. Foreign provenance is never erased:
// a separate proof permits old-writer appends only after a complete clean stop.
func (b *BadgerEngine) finishPropertyWALOnClose(db *managedBadgerDB) error {
	w := b.bufferedCommitWAL
	if db == nil || w == nil {
		return nil
	}
	// Native restore invalidates dictionary authority whenever a WAL is
	// attached, including synchronous engines. All attached logs therefore
	// share this durability check and clean-downgrade sealing owner.
	b.walPropertySource.Lock()
	defer b.walPropertySource.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	// Without an unsealed control or an owed clean proof there is nothing to
	// convert or publish: ordinary synchronous shutdown does no WAL work.
	s := &b.walPropertySource
	if !w.propertyEpochUnsealed && (!s.foreign || s.clean) {
		return nil
	}
	// A caller can attach a log without replaying it. A clean-close proof must
	// never bless those unapplied records as locally written legacy bytes.
	// The same read validates every file and records its controls for sealing.
	var plans []walPropertyEpochSealPlan
	if err := db.View(func(tx *badger.Txn) error {
		var err error
		plans, err = w.planPropertyEpochSealLocked(func(entry WALEntry) error {
			if entry.Operation != OpBufferedCommit {
				return nil
			}
			payload, err := decompressWALBufferedCommitPayload(entry.Data)
			if err != nil {
				return err
			}
			// Decode only the transaction identity, not bodies/binding tables.
			// Buffered records can use the typed recovery codec or legacy JSON.
			var data struct {
				TxID        string `json:"tx_id"`
				Namespace   string `json:"namespace"`
				LogicalTxID string `json:"logical_tx_id"`
			}
			if err := decodeRecoveryPayload(payload, &data); err != nil {
				return err
			}
			if data.TxID == "" {
				return fmt.Errorf("wal: cannot seal buffered record without transaction identity")
			}
			if _, err := tx.Get(bufferedCommitMarkerKey(data.Namespace, data.TxID, data.LogicalTxID)); err != nil {
				return fmt.Errorf("wal: cannot seal unapplied buffered commit %q: %w", data.TxID, err)
			}
			return nil
		})
		return err
	}); err != nil {
		return err
	}
	// Establish the native durability boundary before publishing a proof.
	// A failure here must not leave a new proof that could authorize legacy
	// replay after a restart. Close owns the captured handle throughout.
	if err := db.Sync(); err != nil {
		return err
	}
	if s.foreign && !s.clean {
		if err := db.Update(func(tx *badger.Txn) error { return tx.Set(walPropertyCleanKey, []byte(s.identity)) }); err != nil {
			return err
		}
		if err := db.Sync(); err != nil {
			return err
		}
		s.clean = true
	}
	return w.applyPropertyEpochSealLocked(plans)
}
