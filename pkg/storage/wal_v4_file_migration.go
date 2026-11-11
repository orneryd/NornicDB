package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

// The original headerless writer used this rolling checksum before atomic
// framing introduced CRC32-C. It is only accepted at the V4 upgrade boundary.
func historicalHeaderlessChecksum(data []byte) uint32 {
	var sum uint32
	for _, b := range data {
		sum = (sum >> 8) ^ uint32(b)
		sum ^= sum << 16
	}
	return sum
}

// scanV4MigrationWAL accepts historical framing only at the one-time storage
// migration boundary. Normal WAL readers continue to accept current frames.
// The callback gets each record's original byte range and, for a control,
// its decoded property epoch. carryOversized admits a complete version-3
// record over one frame whole (a store journal); without it such a record is
// refused, as before (the replication journal).
func scanV4MigrationWAL(path string, allowTornTail, carryOversized bool, visit func(WALEntry, *walPropertyEpochFrame, uint64, uint64) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	reader := bufio.NewReaderSize(file, 64*1024)
	prefix, err := reader.Peek(4)
	if err != nil && err != io.EOF {
		return err
	}
	if len(prefix) == 0 {
		return nil
	}
	if len(prefix) < 4 && bytes.Equal(prefix, []byte("WALE")[:len(prefix)]) {
		if allowTornTail {
			return nil
		}
		return fmt.Errorf("wal: incomplete historical frame header")
	}
	if len(prefix) < 4 || binary.LittleEndian.Uint32(prefix) != walMagic {
		// Historical headerless writers emitted one JSON object per line.
		// Limit a single record even when the file contains many records.
		scanner := bufio.NewScanner(reader)
		scanner.Buffer(make([]byte, 4096), maxBufferedWALPayloadBytes)
		var offset uint64
		for scanner.Scan() {
			line := bytes.TrimSpace(scanner.Bytes())
			end := offset + uint64(len(scanner.Bytes())) + 1
			if len(line) == 0 {
				offset = end
				continue
			}
			var entry WALEntry
			if err := json.Unmarshal(line, &entry); err != nil {
				return fmt.Errorf("wal: invalid historical JSON record: %w", err)
			}
			checksum := crc32Checksum(entry.Data)
			if entry.Checksum != checksum && entry.Checksum != historicalHeaderlessChecksum(entry.Data) {
				return fmt.Errorf("wal: historical entry %d: %w", entry.Sequence, ErrWALChecksumFailed)
			}
			// Current frames always carry CRC32-C, irrespective of which
			// algorithm authenticated the source headerless entry.
			entry.Checksum = checksum
			// Epoch controls were introduced in framed WALs, so there is no
			// supported headerless byte-boundary representation to remap.
			if isWALPropertyEpoch(entry) {
				return fmt.Errorf("wal: property epoch in headerless historical WAL")
			}
			if err := visit(entry, nil, offset, end); err != nil {
				return err
			}
			offset = end
		}
		return scanner.Err()
	}

	var offset uint64
	for {
		var header [9]byte
		n, err := io.ReadFull(reader, header[:])
		if err == io.EOF && n == 0 {
			return nil
		}
		if err != nil {
			magicBytes := min(n, 4)
			validPrefix := bytes.Equal(header[:magicBytes], []byte("WALE")[:magicBytes])
			knownVersion := n < 5 || header[4] >= 1 && header[4] <= walFormatVersionSpanned
			if allowTornTail && err == io.ErrUnexpectedEOF && validPrefix && knownVersion {
				return nil
			}
			return fmt.Errorf("wal: incomplete historical frame at %d: %w", offset, err)
		}
		if binary.LittleEndian.Uint32(header[:4]) != walMagic {
			return fmt.Errorf("wal: historical frame at %d: %w", offset, ErrWALCorrupted)
		}
		version := header[4]
		if version < 1 || version > walFormatVersionSpanned {
			return unsupportedWALVersion(version)
		}
		size := binary.LittleEndian.Uint32(header[5:])
		if size > walMaxEntrySize {
			entry, length, err := readOversizedFrame(reader, size, version, carryOversized)
			if err != nil {
				return fmt.Errorf("wal: historical frame at %d: %w", offset, err)
			}
			entry.framingVersion = version
			end := offset + uint64(length)
			if err := visit(entry, nil, offset, end); err != nil {
				return err
			}
			offset = end
			continue
		}
		payload := make([]byte, int(size))
		if _, err := io.ReadFull(reader, payload); err != nil {
			if allowTornTail && (err == io.EOF || err == io.ErrUnexpectedEOF) {
				return nil
			}
			return fmt.Errorf("wal: incomplete historical payload at %d: %w", offset, err)
		}
		var crc [4]byte
		if _, err := io.ReadFull(reader, crc[:]); err != nil {
			if allowTornTail && (err == io.EOF || err == io.ErrUnexpectedEOF) {
				return nil
			}
			return fmt.Errorf("wal: incomplete historical checksum at %d: %w", offset, err)
		}
		if binary.LittleEndian.Uint32(crc[:]) != crc32Checksum(payload) {
			return fmt.Errorf("wal: historical frame at %d: %w", offset, ErrWALChecksumFailed)
		}
		length := int64(9) + int64(size) + 4
		if version >= 2 {
			var trailer [8]byte
			if _, err := io.ReadFull(reader, trailer[:]); err != nil {
				if allowTornTail && (err == io.EOF || err == io.ErrUnexpectedEOF) {
					return nil
				}
				return fmt.Errorf("wal: incomplete historical trailer at %d: %w", offset, err)
			}
			if binary.LittleEndian.Uint64(trailer[:]) != walTrailer {
				return fmt.Errorf("wal: invalid historical trailer at %d: %w", offset, ErrWALCorrupted)
			}
			rawLength := length + 8
			length = alignUp(rawLength)
			var padding [7]byte
			if _, err := io.ReadFull(reader, padding[:int(length-rawLength)]); err != nil {
				if allowTornTail && (err == io.EOF || err == io.ErrUnexpectedEOF) {
					return nil
				}
				return fmt.Errorf("wal: incomplete historical padding at %d: %w", offset, err)
			}
		}
		var entry WALEntry
		if version < 3 {
			err = json.Unmarshal(payload, &entry)
		} else {
			// Framed versions from 3 onward use the same raw-Data envelope.
			// The outer frame and semantic payload may differ.
			_, err = processWALEntryPayload(walPayloadDecode, nil, &entry, payload, walFormatVersionCurrent)
		}
		if err != nil {
			return fmt.Errorf("wal: invalid historical payload at %d: %w", offset, err)
		}
		entry.framingVersion = walFormatVersionCurrent
		if version == 4 && !isWALPropertyEpoch(entry) {
			return fmt.Errorf("wal: historical version 4 frame is not a property epoch")
		}
		if entry.Checksum != crc32Checksum(entry.Data) {
			return fmt.Errorf("wal: historical entry %d: %w", entry.Sequence, ErrWALChecksumFailed)
		}
		var epoch *walPropertyEpochFrame
		if isWALPropertyEpoch(entry) {
			frame, err := decodeWALPropertyEpochFrame(entry)
			if err != nil {
				return err
			}
			if version < 4 && (frame.through == ^uint64(0) || frame.last == ^uint64(0)) {
				return fmt.Errorf("wal: unsealed property epoch is invalid in historical version %d", version)
			}
			epoch = &frame
		}
		// Preserve the source version for semantic conversion after property
		// controls have been validated using current-frame rules above.
		entry.framingVersion = version
		end := offset + uint64(length)
		if err := visit(entry, epoch, offset, end); err != nil {
			return err
		}
		offset = end
	}
}

// v4UpgradeRecords is how the V4 upgrade converts the records of one journal:
// the journal's record policy and its payload transform. Validation and
// conversion take the same value and convert each record other than a property-epoch
// control with the same convert, so whether it is converted, spans frames or is refused
// is decided by the record encoder under that policy, in validation and again in conversion
// (NOR-412). nil is a store journal without a payload transform.
type v4UpgradeRecords struct {
	policy    WALRecordPolicy
	transform func(uint8, OperationType, []byte) ([]byte, error)
}

// carries reports whether the journal takes records over one frame. The
// scanner then reads a complete version-3 frame over one frame whole; without
// it such a frame is refused as it is read.
func (r *v4UpgradeRecords) carries() bool {
	return r == nil || r.policy == WALRecordsLarge
}

// convert returns the entry converted to current framing and the frames it is
// written as. The record encoder decides from the converted record whether it
// fits one frame; under the standard policy a record that does not is refused
// with the encoder's frame error, under the large policy it spans frames.
func (r *v4UpgradeRecords) convert(buf *bytes.Buffer, entry WALEntry) (WALEntry, walRecordFrames, error) {
	if entry.Operation == OpEntityCommit && entry.framingVersion < walFormatVersionCurrent {
		converted, err := convertV4SemanticWALRecord(entry)
		if err != nil {
			return entry, walRecordFrames{}, fmt.Errorf("wal: convert historical entity commit %d: %w", entry.Sequence, err)
		}
		entry = converted
	}
	if r != nil && r.transform != nil && entry.framingVersion < walFormatVersionCurrent {
		converted, err := r.transform(entry.framingVersion, entry.Operation, entry.Data)
		if err != nil {
			return entry, walRecordFrames{}, fmt.Errorf("wal: convert historical payload %d: %w", entry.Sequence, err)
		}
		entry.Data = converted
		entry.Checksum = crc32Checksum(converted)
	}
	record, err := encodeWALRecord(buf, &entry, r.carries())
	return entry, record, err
}

// planV4WALMigration validates every historical record and its authority
// boundaries without changing the file, returning the offsets to remap. With
// decide it also converts each record other than a property-epoch control as the
// upgrade will (records.convert) and discards the result; when the conversion gives the
// same bytes again, a record it would refuse is refused here, before anything is written.
func planV4WALMigration(source string, allowTornTail bool, records *v4UpgradeRecords, decide bool) (map[uint64]bool, error) {
	targets := make(map[uint64]bool)
	var buf bytes.Buffer
	if err := scanV4MigrationWAL(source, allowTornTail, records.carries(), func(entry WALEntry, epoch *walPropertyEpochFrame, start, _ uint64) error {
		if decide && epoch == nil {
			if _, _, err := records.convert(&buf, entry); err != nil {
				return fmt.Errorf("wal: historical frame at %d: %w", start, err)
			}
		}
		if entry.Operation == OpBufferedCommit {
			data, err := decodeBufferedWALRecord(entry)
			if err != nil {
				return err
			}
			if !data.namedPropertiesAuthoritative {
				// The old JSON encoder replaced invalid UTF-8 in physical
				// map keys. U+FFFD is ambiguous: the original bytes may
				// already be lost, so never apply those keys silently.
				for key := range data.Writes {
					if strings.ContainsRune(key, '\ufffd') {
						return fmt.Errorf("wal: historical physical key contains ambiguous UTF-8 replacement")
					}
				}
				for _, key := range data.Deletes {
					if strings.ContainsRune(key, '\ufffd') {
						return fmt.Errorf("wal: historical physical key contains ambiguous UTF-8 replacement")
					}
				}
			}
		}
		if epoch == nil {
			return nil
		}
		if epoch.through != ^uint64(0) {
			targets[epoch.through] = true
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(targets) != 0 {
		remaining := make(map[uint64]bool, len(targets))
		for target := range targets {
			remaining[target] = true
		}
		delete(remaining, 0)
		if err := scanV4MigrationWAL(source, allowTornTail, records.carries(), func(_ WALEntry, _ *walPropertyEpochFrame, start, end uint64) error {
			delete(remaining, start)
			delete(remaining, end)
			return nil
		}); err != nil {
			return nil, err
		}
		if len(remaining) != 0 {
			return nil, fmt.Errorf("wal: historical property epoch points outside a record boundary")
		}
	}
	return targets, nil
}

type v4WALControlRewrite struct {
	offset  int64
	length  int
	through uint64
	entry   WALEntry
	frame   walPropertyEpochFrame
}

// upgradeWALFileV4 writes one fully validated current-frame file beside its
// untouched source. Callers must publish the staged file only after every WAL
// file has been validated and the originals durably backed up. Each record other than a
// property-epoch control is converted by records.convert, which validation already ran
// for it (UpgradeWALV4), under the same journal policy; a control is encoded only here.
func upgradeWALFileV4(source, destination string, allowTornTail bool, records *v4UpgradeRecords) (resultErr error) {
	targets, err := planV4WALMigration(source, allowTornTail, records, false)
	if err != nil {
		return err
	}
	info, err := os.Stat(source)
	if err != nil {
		return err
	}
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm())
	if err != nil {
		return err
	}
	defer func() {
		_ = out.Close()
		if resultErr != nil {
			_ = os.Remove(destination)
		}
	}()
	writer := bufio.NewWriterSize(out, 64*1024)
	mapped := map[uint64]uint64{0: 0}
	var offset int64
	var controls []v4WALControlRewrite
	var buf bytes.Buffer
	if err := scanV4MigrationWAL(source, allowTornTail, records.carries(), func(entry WALEntry, epoch *walPropertyEpochFrame, start, end uint64) error {
		if targets[start] {
			mapped[start] = uint64(offset)
		}
		var record walRecordFrames
		var control *v4WALControlRewrite
		if epoch != nil {
			frame := *epoch
			frame.Through, frame.Last = fmt.Sprintf("%016x", frame.through), fmt.Sprintf("%016x", frame.last)
			record.single, err = encodeWALPropertyEpochFrame(frame, entry.Sequence, entry.Timestamp)
			if err != nil {
				return err
			}
			control = &v4WALControlRewrite{offset: offset, through: frame.through, entry: entry, frame: frame}
		} else {
			var err error
			entry, record, err = records.convert(&buf, entry)
			if err != nil {
				return err
			}
		}
		// Spanned parts are current framing: they keep their own version.
		frameVersion := walFormatVersionCurrent
		if entry.framingVersion == walFormatVersionSpanned {
			frameVersion = walFormatVersionSpanned
		}
		var length int64
		var err error
		if record.parts != nil {
			length, err = record.write(writer, writeAtomicRecordBufio)
		} else {
			length, err = writeAtomicRecordBufio(writer, record.single, frameVersion)
		}
		if err != nil {
			return err
		}
		if control != nil {
			control.length = int(length)
			controls = append(controls, *control)
		}
		offset += length
		if targets[end] {
			mapped[end] = uint64(offset)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	for _, control := range controls {
		if control.through == ^uint64(0) {
			continue
		}
		end, ok := mapped[control.through]
		if !ok {
			return fmt.Errorf("wal: historical property epoch points outside a record boundary")
		}
		control.frame.Through = fmt.Sprintf("%016x", end)
		payload, err := encodeWALPropertyEpochFrame(control.frame, control.entry.Sequence, control.entry.Timestamp)
		if err != nil {
			return err
		}
		record, _ := buildAtomicRecord(payload, walFormatVersionCurrent)
		if len(record) != control.length {
			return fmt.Errorf("wal: property epoch migration changed control length")
		}
		if _, err := out.WriteAt(record, control.offset); err != nil {
			return err
		}
	}
	if err := out.Sync(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return nil
}
