// Author: NOR-291 implementation lane, Codex, 2026-10-27; TJ API rebase 2026-10-29.
package replication

import (
	"context"
	"fmt"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkStorageAdapterAcceptedProjection(b *testing.B) {
	engine := fixtureowner.Own(b, fixtureowner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed))
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
		if _, err := adapter.ApplyCommand(context.Background(), &Command{Type: CmdCommitTransaction, Data: data, Timestamp: time.Now(), RequestID: id}, WALQueued); err != nil {
			b.Fatal(err)
		}
	}
	if err := adapter.FlushWAL(); err != nil {
		b.Fatal(err)
	}
}
