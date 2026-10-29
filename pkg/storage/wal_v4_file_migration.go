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
// its decoded property epoch.
func scanV4MigrationWAL(path string, allowTornTail bool, visit func(WALEntry, *walPropertyEpochFrame, uint64, uint64) error) error {
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
			knownVersion := n < 5 || header[4] >= 1 && header[4] <= walFormatVersionCurrent
			if allowTornTail && err == io.ErrUnexpectedEOF && validPrefix && knownVersion {
				return nil
			}
			return fmt.Errorf("wal: incomplete historical frame at %d: %w", offset, err)
		}
		if binary.LittleEndian.Uint32(header[:4]) != walMagic {
			return fmt.Errorf("wal: historical frame at %d: %w", offset, ErrWALCorrupted)
		}
		version := header[4]
		if version < 1 || version > walFormatVersionCurrent {
			return unsupportedWALVersion(version)
		}
		size := binary.LittleEndian.Uint32(header[5:])
		if size > walMaxEntrySize {
			return fmt.Errorf("wal: historical frame at %d exceeds maximum size", offset)
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
			// Versions 3, 4 and 5 use the same raw-Data envelope; only
			// the outer frame and v4 control semantics differ.
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
		end := offset + uint64(length)
		if err := visit(entry, epoch, offset, end); err != nil {
			return err
		}
		offset = end
	}
}

// planV4WALMigration validates every historical record and its authority
// boundaries without changing the file, returning the offsets to remap.
func planV4WALMigration(source string, allowTornTail bool) (map[uint64]bool, error) {
	targets := make(map[uint64]bool)
	if err := scanV4MigrationWAL(source, allowTornTail, func(entry WALEntry, epoch *walPropertyEpochFrame, _, _ uint64) error {
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
		if err := scanV4MigrationWAL(source, allowTornTail, func(_ WALEntry, _ *walPropertyEpochFrame, start, end uint64) error {
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
// file has been validated and the originals durably backed up.
func upgradeWALFileV4(source, destination string, allowTornTail bool) (resultErr error) {
	targets, err := planV4WALMigration(source, allowTornTail)
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
	if err := scanV4MigrationWAL(source, allowTornTail, func(entry WALEntry, epoch *walPropertyEpochFrame, start, end uint64) error {
		if targets[start] {
			mapped[start] = uint64(offset)
		}
		var payload []byte
		var control *v4WALControlRewrite
		if epoch != nil {
			frame := *epoch
			frame.Through, frame.Last = fmt.Sprintf("%016x", frame.through), fmt.Sprintf("%016x", frame.last)
			payload, err = encodeWALPropertyEpochFrame(frame, entry.Sequence, entry.Timestamp)
			if err != nil {
				return err
			}
			control = &v4WALControlRewrite{offset: offset, through: frame.through, entry: entry, frame: frame}
		} else {
			var err error
			payload, err = processWALEntryPayload(walPayloadEncode, &buf, &entry, nil, 0)
			if err != nil {
				return err
			}
		}
		length, err := writeAtomicRecordBufio(writer, payload, walFormatVersionCurrent)
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
