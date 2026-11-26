package storage

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// A journal that predates WALConfig.EncryptionKey stores plaintext frames. The
// operator converts it once, before NewWAL is called with the key: these two
// helpers rewrite the frames it reads into authenticated ones and leave the
// envelope alone. They are a one-time migration operation, not part of the
// running journal's lifecycle.
//
// Every file is converted through a sibling temporary file that is synced and
// atomically renamed over the source, so an interrupted conversion leaves each
// file in exactly one format. A file that already holds encrypted frames is
// authenticated and accepted, which makes a repeated or resumed conversion
// safe. A frame this build cannot read (older framing, damage, a payload above
// the encrypted frame bound) is refused: conversion never drops or reinterprets
// a record.

// EncryptWALFile upgrades one plaintext journal file in place. The plaintext
// file remains untouched until a complete authenticated replacement has been
// written, synced and atomically published. An already-converted file is
// authenticated with key before it is accepted.
func EncryptWALFile(ctx context.Context, path string, key []byte) error {
	cipher, err := walConversionCipher(key)
	if err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("wal: encryption conversion refuses non-regular file %s", path)
	}
	encrypted, err := walFileEncrypted(path)
	if err != nil {
		return err
	}
	if encrypted {
		return visitWALEntriesWithLogger(ctx, path, nil, walPayloadBorrowed, walFrameReader{cipher: cipher}, func(WALEntry) error {
			return nil
		})
	}
	return convertPlaintextWALFile(path, cipher, info.Mode().Perm())
}

// EncryptWALDirectory upgrades the retained segments and the active file of one
// journal directory, then publishes the manifest with the converted segment
// sizes. A segment the manifest lists must exist: a directory whose journal
// files disagree with its manifest is not converted. A directory that holds no
// journal files is left alone.
func EncryptWALDirectory(ctx context.Context, dir string, key []byte) error {
	if _, err := walConversionCipher(key); err != nil {
		return err
	}
	manifest, err := accessWALManifest(dir, walReadManifest, nil)
	if err != nil {
		return err
	}
	sizes := make([]int64, len(manifest.Segments))
	listed := make(map[string]bool, len(manifest.Segments))
	for i, segment := range manifest.Segments {
		listed[segment.Path] = true
		path, err := resolveWALSegmentPath(dir, segment.Path)
		if err != nil {
			return err
		}
		if err := EncryptWALFile(ctx, path, key); err != nil {
			return err
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		sizes[i] = info.Size()
	}
	// Retention can leave a retired segment on disk after a crash. The manifest
	// deliberately does not rediscover it for replay, but an encrypted store
	// must not leave its old plaintext bytes in the WAL directory.
	diskSegments, err := scanSegmentsFromDir(dir)
	if err != nil {
		return err
	}
	for _, segment := range diskSegments {
		if listed[segment.Path] {
			continue
		}
		path, err := resolveWALSegmentPath(dir, segment.Path)
		if err != nil {
			return err
		}
		if err := EncryptWALFile(ctx, path, key); err != nil {
			return fmt.Errorf("encrypt unlisted WAL segment %s: %w", path, err)
		}
	}
	active := walActivePath(dir)
	if _, err := os.Lstat(active); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else if err := EncryptWALFile(ctx, active, key); err != nil {
		return err
	}
	changed := false
	for i := range manifest.Segments {
		if manifest.Segments[i].SizeBytes != sizes[i] {
			manifest.Segments[i].SizeBytes = sizes[i]
			changed = true
		}
	}
	if !changed {
		return nil
	}
	_, err = accessWALManifest(dir, walPublishManifest, manifest)
	return err
}

// walConversionCipher is the cipher a conversion writes with. A conversion
// always needs a key; an absent one is not a plaintext conversion.
func walConversionCipher(key []byte) (*walFrameCipher, error) {
	cipher, err := walConfigCipher(key)
	if err != nil {
		return nil, err
	}
	if cipher == nil {
		return nil, fmt.Errorf("%w, got 0", ErrWALEncryptionKeyInvalid)
	}
	return cipher, nil
}

// convertPlaintextWALFile rewrites every complete frame of path as its
// encrypted form. The plaintext payload is sealed verbatim, so the entry bytes
// a reader decodes are the ones the file held; only the framing changes.
func convertPlaintextWALFile(path string, cipher *walFrameCipher, mode os.FileMode) error {
	source, err := os.Open(path)
	if err != nil {
		return err
	}
	defer source.Close()
	temp, err := os.CreateTemp(filepath.Dir(path), ".wal-encrypt-*.tmp")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer func() {
		_ = temp.Close()
		_ = os.Remove(tempPath)
	}()

	reader := bufio.NewReaderSize(source, 64*1024)
	writer := bufio.NewWriterSize(temp, 64*1024)
	plain := walFrameReader{}
	var offset int64
	for {
		var header [9]byte
		n, err := io.ReadFull(reader, header[:])
		if err == io.EOF && n == 0 {
			break
		}
		if err != nil {
			return fmt.Errorf("wal: convert %s: incomplete frame at %d: %w", path, offset, err)
		}
		if binary.LittleEndian.Uint32(header[:4]) != walMagic {
			return fmt.Errorf("wal: convert %s: frame at %d: %w", path, offset, ErrWALCorrupted)
		}
		version := header[4]
		if err := plain.version(version); err != nil {
			return fmt.Errorf("wal: convert %s: frame at %d: %w", path, offset, err)
		}
		size := binary.LittleEndian.Uint32(header[5:])
		if walFrameOversized(size, version) {
			return fmt.Errorf("wal: convert %s: frame at %d: %w: stored payload %d exceeds the frame bound", path, offset, ErrWALCorrupted, size)
		}
		payload := make([]byte, size)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return fmt.Errorf("wal: convert %s: frame at %d: %w", path, offset, err)
		}
		var crc [4]byte
		if _, err := io.ReadFull(reader, crc[:]); err != nil {
			return fmt.Errorf("wal: convert %s: frame at %d: %w", path, offset, err)
		}
		if binary.LittleEndian.Uint32(crc[:]) != crc32Checksum(payload) {
			return fmt.Errorf("wal: convert %s: frame at %d: %w", path, offset, ErrWALChecksumFailed)
		}
		var trailer [8]byte
		if _, err := io.ReadFull(reader, trailer[:]); err != nil {
			return fmt.Errorf("wal: convert %s: frame at %d: %w", path, offset, err)
		}
		if binary.LittleEndian.Uint64(trailer[:]) != walTrailer {
			return fmt.Errorf("wal: convert %s: frame at %d: %w", path, offset, ErrWALCorrupted)
		}
		frameLen := walFrameAlignedLen(nil, int(size), version)
		rawLen := int64(9) + int64(size) + 4 + 8
		if padding := frameLen - rawLen; padding > 0 {
			if _, err := reader.Discard(int(padding)); err != nil {
				return fmt.Errorf("wal: convert %s: frame at %d: %w", path, offset, err)
			}
		}
		if _, err := writeWALFrame(cipher, writer, payload, version); err != nil {
			return fmt.Errorf("wal: convert %s: frame at %d: %w", path, offset, err)
		}
		offset += frameLen
	}

	if err := writer.Flush(); err != nil {
		return err
	}
	if err := temp.Chmod(mode); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}
