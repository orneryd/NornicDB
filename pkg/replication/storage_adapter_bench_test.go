package replication

import (
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkStorageAdapter_GetWALEntries(b *testing.B) {
	engine := fixtureowner.Own(b, fixtureowner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	adapter, err := NewStorageAdapterWithWAL(engine, b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = adapter.Close() })

	const total = 20_000
	for i := 0; i < total; i++ {
		node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("nornic:n%d", i))}
		data, _ := storage.EncodeNodePayload(node)
		cmd := &Command{Type: CmdCreateNode, Data: data, Timestamp: time.Now()}
		if err := adapter.ApplyCommand(cmd); err != nil {
			b.Fatal(err)
		}
	}

	// Mimic the "catch up from near the end" path: fromPosition is close to total,
	// but GetWALEntries should still be fast.
	fromPosition := uint64(total - 1500)
	batchSize := 1000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries, err := adapter.GetWALEntries(fromPosition, batchSize)
		if err != nil {
			b.Fatal(err)
		}
		if len(entries) == 0 {
			b.Fatalf("expected entries")
		}
	}
}

func BenchmarkStorageAdapter_ApplyCommand_CreateNode(b *testing.B) {
	engine := fixtureowner.Own(b, fixtureowner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	adapter, err := NewStorageAdapterWithWAL(engine, b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = adapter.Close() })

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("nornic:n%d", i))}
		data, _ := storage.EncodeNodePayload(node)
		cmd := &Command{Type: CmdCreateNode, Data: data, Timestamp: time.Now()}
		if err := adapter.ApplyCommand(cmd); err != nil {
			b.Fatal(err)
		}
	}
}
