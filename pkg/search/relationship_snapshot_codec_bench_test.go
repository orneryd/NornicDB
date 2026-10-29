package search

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// BenchmarkRelationshipSnapshotFileEncoding compares the historical gob and
// current packed MessagePack encoders, including file sync on both paths.
func BenchmarkRelationshipSnapshotFileEncoding(b *testing.B) {
	const dimensions = 384
	for _, count := range []int{128, 2048} {
		snapshot := relationshipVectorsFile{Format: relationshipVectorsFormat, Entries: make(map[string]*relationshipEntry, count)}
		for i := 0; i < count; i++ {
			vector := make([]float32, dimensions)
			for j := range vector {
				vector[j] = float32((i+j)%53) / 53
			}
			snapshot.Entries[fmt.Sprintf("edge-%06d", i)] = &relationshipEntry{Source: uint64(i + 1), Vectors: [][]float32{vector}}
		}
		for _, codec := range []string{"gob", "msgpack-packed-stream"} {
			b.Run(fmt.Sprintf("entries=%d/%s", count, codec), func(b *testing.B) {
				path := filepath.Join(b.TempDir(), "snapshot")
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
					if err != nil {
						b.Fatal(err)
					}
					if codec == "gob" {
						err = gob.NewEncoder(file).Encode(&snapshot)
					} else {
						writer := bufio.NewWriterSize(file, 64*1024)
						err = encodeRelationshipSnapshot(writer, &snapshot)
						if err == nil {
							err = writer.Flush()
						}
					}
					if err == nil {
						err = file.Sync()
					}
					if closeErr := file.Close(); err == nil {
						err = closeErr
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
