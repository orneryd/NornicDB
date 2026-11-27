package storage

import (
	"context"
	"fmt"
	"io"
	"os"
)

// EncryptSnapshotFile upgrades a pre-encryption snapshot in place. The old
// snapshot remains untouched until a complete authenticated replacement has
// been written, synced, and atomically published. A restart can therefore
// repeat an interrupted conversion safely. Existing encrypted snapshots are
// authenticated before they are accepted.
func EncryptSnapshotFile(ctx context.Context, path string, key []byte) error {
	if len(key) != SnapshotEncryptionKeySize {
		return ErrSnapshotEncryptionKeyInvalid
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("snapshot migration refuses non-regular file %s", path)
	}
	encrypted, err := IsEncryptedSnapshot(path)
	if err != nil {
		return err
	}
	if encrypted {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		_, err = ReadSnapshotWithKey(ctx, file, discardMigrationSnapshotVisitor{}, key)
		return err
	}
	streaming, err := IsStreamingSnapshot(path)
	if err != nil {
		return err
	}
	if !streaming {
		// Historical JSON snapshots have to be decoded before they can be
		// written as the NDS2 stream inside the encrypted envelope.
		snapshot, err := LoadSnapshot(ctx, path)
		if err != nil {
			return fmt.Errorf("validate historical snapshot %s: %w", path, err)
		}
		return saveSnapshotFile(path, func(dst io.Writer) error {
			writer, err := newSnapshotEnvelopeWriter(dst, key)
			if err != nil {
				return err
			}
			if err := writeSnapshotCollection(writer, snapshot); err != nil {
				return err
			}
			return writer.Finish()
		})
	}
	source, err := os.Open(path)
	if err != nil {
		return err
	}
	defer source.Close()
	// Do not seal a corrupt plaintext snapshot as if it were a healthy
	// recovery point. This scan is bounded by the existing NDS2 frame reader.
	if _, err := ReadSnapshot(ctx, source, discardMigrationSnapshotVisitor{}); err != nil {
		return fmt.Errorf("validate snapshot %s: %w", path, err)
	}
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return saveSnapshotFile(path, func(dst io.Writer) error {
		writer, err := newSnapshotEnvelopeWriter(dst, key)
		if err != nil {
			return err
		}
		if _, err := io.Copy(writer, source); err != nil {
			return err
		}
		return writer.Finish()
	})
}

type discardMigrationSnapshotVisitor struct{}

func (discardMigrationSnapshotVisitor) VisitNode(context.Context, *Node) error { return nil }
func (discardMigrationSnapshotVisitor) VisitEdge(context.Context, *Edge) error { return nil }
