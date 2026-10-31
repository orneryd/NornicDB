// Author: NOR-291 implementation lane, Codex, 2026-10-27; TJ API rebase 2026-10-29.
package replication

import (
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkStorageAdapterAcceptedProjection(b *testing.B) {
	engine := storage.NewMemoryEngine()
	b.Cleanup(func() { _ = engine.Close() })
	adapter, err := NewStorageAdapterWithWAL(engine, b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = adapter.Close() })
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		id := fmt.Sprintf("command-%d", i)
		node := &storage.Node{ID: storage.NodeID("bench:" + id)}
		data, err := storage.EncodeEntityTransaction(storage.EntityTransactionRequest{Namespace: "bench", LogicalID: id, Operations: []storage.Operation{{Type: storage.OpCreateNode, Node: node}}})
		if err != nil {
			b.Fatal(err)
		}
		if err := adapter.ApplyCommand(&Command{Type: CmdCommitTransaction, Data: data, Timestamp: time.Now(), RequestID: id}); err != nil {
			b.Fatal(err)
		}
	}
	if err := adapter.FlushWAL(); err != nil {
		b.Fatal(err)
	}
}
