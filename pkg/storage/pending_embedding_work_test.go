package storage

import (
	"expvar"
	"fmt"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestPendingClaimPhysicalWork(t *testing.T) {
	metric, ok := expvar.Get("embed_pending_parse_work").(*expvar.Int)
	if !ok {
		if os.Getenv("NORNICDB_EMBED_PENDING_WORK_COUNTER") == "required" {
			t.Fatal("Badger work counter was not installed")
		}
		t.Skip("requires instrumented Badger iterator; run scripts/embedding/pending_work_probe.go")
	}
	for _, n := range []int{0, 100, 1000, 10000} {
		for _, tail := range []bool{false, true} {
			t.Run(fmt.Sprintf("deleted=%d/tail=%t", n, tail), func(t *testing.T) {
				e := newTestBadgerEngineForPending(t)
				// Keep deleted markers in the current memtable: no compaction timing dependency.
				require.NoError(t, e.withUpdate(func(txn *badger.Txn) error {
					for i := 0; i < n; i++ {
						if err := txn.Set(pendingEmbedKey(NodeID(fmt.Sprintf("test:m%06d", i))), nil); err != nil {
							return err
						}
					}
					return nil
				}))
				require.NoError(t, e.withUpdate(func(txn *badger.Txn) error {
					for i := 0; i < n; i++ {
						if err := txn.Delete(pendingEmbedKey(NodeID(fmt.Sprintf("test:m%06d", i)))); err != nil {
							return err
						}
					}
					return nil
				}))
				id := NodeID("test:z")
				if tail {
					id = "test:a"
				}
				_, err := e.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"content": "pending"}})
				require.NoError(t, err)
				// Recovery may load the persistent index once; active claims must not repeat it.
				require.NotNil(t, e.FindNodeNeedingEmbedding())
				for i := 0; i < 3; i++ {
					before := metric.Value()
					node := e.FindNodeNeedingEmbedding()
					work := metric.Value() - before
					require.NotNil(t, node)
					require.Equal(t, id, node.ID)
					t.Logf("PHYSICAL_WORK deleted=%d tail=%t claim=%d parse_items=%d", n, tail, i, work)
					require.LessOrEqual(t, work, int64(32), "claim revisits deleted pending markers")
				}
			})
		}
	}
}
