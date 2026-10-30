package storage

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func bindingCostFixture(t testing.TB, count int) (*BadgerEngine, *WAL, *bufferedCommit) {
	t.Helper()
	// Appends are silent no-ops while the process-wide switch is off, and
	// earlier package tests may leave it off.
	t.Cleanup(config.WithWALEnabled())
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	properties := make(map[string]any, count)
	for i := 0; i < count; i++ {
		properties[fmt.Sprintf("property_%03d", i)] = int64(i)
	}
	_, err = b.CreateNode(&Node{ID: "perf:a", Properties: properties})
	require.NoError(t, err)
	var body []byte
	require.NoError(t, b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey("perf:a"))
		if err == nil {
			body, err = item.ValueCopy(nil)
		}
		return err
	}))
	w, err := NewWAL(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return b, w, &bufferedCommit{namespace: "perf", txID: "cost", walIDs: b.idDict, writes: map[string][]byte{string(nodeKey("perf:a")): body}}
}

// Fixed samples measure production steady append before and after a much larger
// dictionary. AllocsPerRun's untimed warm-up publishes the changed epoch once.
func TestBufferedWALBindingAllocationBounds(t *testing.T) {
	for _, count := range []int{1, 160} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			b, w, c := bindingCostFixture(t, count)
			appendRecord := func() {
				if err := w.AppendBufferedCommit(c, b.propKeyDict); err != nil {
					t.Fatal(err)
				}
			}
			appendAllocs := testing.AllocsPerRun(32, appendRecord)
			unused := make(map[string]any, 4096)
			for i := 0; i < 4096; i++ {
				unused[fmt.Sprintf("unused_%04d", i)] = int64(i)
			}
			_, err := b.CreateNode(&Node{ID: "perf:unused", Properties: unused})
			require.NoError(t, err)
			after := testing.AllocsPerRun(32, appendRecord)
			require.LessOrEqual(t, after, appendAllocs+4, "unreferenced dictionary names must not add steady-append work")
			// Race builds randomly bypass sync.Pool; allow constant headroom
			// for JSON/compression pool misses, not an allocation per key.
			// The bounded non-race benchmark reports the actual bytes/counts.
			require.LessOrEqual(t, appendAllocs, float64(64), "append must avoid per-key JSON object allocations")
			entries, err := ReadWALEntriesFromDir(w.Config().Dir)
			require.NoError(t, err)
			require.NotEmpty(t, entries, "every measured append must reach the log")
			beforePayload, err := decompressWALBufferedCommitPayload(entries[0].Data)
			require.NoError(t, err)
			afterPayload, err := decompressWALBufferedCommitPayload(entries[len(entries)-1].Data)
			require.NoError(t, err)
			require.Equal(t, beforePayload, afterPayload, "schema growth changes the epoch, never steady record bytes")
			t.Logf("keys=%d append_allocs=%.0f expanded_schema_append_allocs=%.0f", count, appendAllocs, after)
		})
	}
}

// Run with -benchtime=500x to keep samples bounded; report bytes as well as
// allocation counts. Replay includes decoding and real Badger application.
func BenchmarkWALPropertyBindingCost(b *testing.B) {
	for _, count := range []int{1, 160} {
		for _, action := range []string{"EpochBoundary", "Append", "Replay"} {
			b.Run(fmt.Sprintf("%s/%d", action, count), func(b *testing.B) {
				store, w, c := bindingCostFixture(b, count)
				require.NoError(b, w.AppendBufferedCommit(c, store.propKeyDict))
				entries, err := ReadWALEntriesFromDir(w.Config().Dir)
				require.NoError(b, err)
				require.Len(b, entries, 1)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					switch action {
					case "EpochBoundary":
						_, _, err = store.propKeyDict.captureWALEpoch()
					case "Append":
						err = w.AppendBufferedCommit(c, store.propKeyDict)
					case "Replay":
						err = ReplayWALEntry(store, entries[0])
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
