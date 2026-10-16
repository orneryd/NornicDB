package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestEmbeddingCleanup_SingleOwnerRepairLinear(t *testing.T) {
	var timings []time.Duration
	for _, n := range []int{8192, 32768} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			b := newTestEngine(t)
			id := NodeID("test:large-orphan")
			require.NoError(t, b.commitEngineWrite(func(cw *commitWriter) error {
				for i := 0; i < n; i++ {
					key := embeddingKey(id, i)
					if e := cw.write(func(txn *badger.Txn) error { return txn.Set(key, []byte{1}) }); e != nil {
						return e
					}
				}
				return nil
			}))
			// Hold an older reader as a real long-lived snapshot would, so
			// compaction cannot mask the cost by reclaiming old versions.
			snapshot, ts, err := b.db.beginTxn(false)
			require.NoError(t, err)
			defer b.db.endRead(ts)
			defer snapshot.Discard()
			started := time.Now()
			removed, err := b.RepairOrphanedEmbeddingKeys()
			require.NoError(t, err)
			require.Equal(t, n, removed)
			t.Logf("single owner keys=%d repair=%s (old snapshot held)", n, time.Since(started))
			timings = append(timings, time.Since(started))
			require.Zero(t, countEmbeddingKeys(t, b, id))
		})
	}
	require.Less(t, float64(timings[1])/float64(timings[0]), 8.0, "4x keys should not take quadratic time")
}
