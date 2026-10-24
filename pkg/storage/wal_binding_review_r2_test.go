package storage

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func r2BindingData(t testing.TB, count int) (*BadgerEngine, WALBufferedCommitData) {
	source, _, c := bindingCostFixture(t, count)
	bindings, err := bufferedWALPropertyBindings(c.writes, source.propKeyDict)
	require.NoError(t, err)
	return source, WALBufferedCommitData{Namespace: c.namespace, TxID: c.txID, Writes: c.writes, PropertyBindings: bindings}
}

func TestReviewR2RemapCrashBeforeApply(t *testing.T) {
	_, data := r2BindingData(t, 1)
	dir := t.TempDir()
	dest := reviewBindingEngine(t, dir)
	_, err := dest.CreateNode(&Node{ID: "perf:sentinel", Properties: map[string]any{"unrelated": "keep"}})
	require.NoError(t, err)
	c, err := dest.importBufferedWALRecord(&data, walForeignImport)
	require.NoError(t, err)
	require.NotEmpty(t, c.propKeyDrain.pending)
	// Crash boundary before the imported body or its destination drain applies.
	require.NoError(t, dest.Close())
	dest = reviewBindingEngine(t, dir)

	require.True(t, dest.walPropertySource.foreign)
	data.PropertyBindings = nil
	_, err = dest.importBufferedWALRecord(&data, walForeignImport)
	require.ErrorContains(t, err, "missing property name binding")
	assertPropertyDictionaryBijection(t, dest)
}

func TestReviewR2ConcurrentAllocation(t *testing.T) {
	_, data := r2BindingData(t, 160)
	dir := t.TempDir()
	dest := reviewBindingEngine(t, dir)
	_, err := dest.CreateNode(&Node{ID: "perf:sentinel", Properties: map[string]any{"unrelated": "keep"}})
	require.NoError(t, err)
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	drains := make(chan propKeyTxnDrain, 16)
	for i := 0; i < 16; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			c, e := dest.importBufferedWALRecord(&data, walForeignImport)
			if e == nil {
				drains <- c.propKeyDrain
			}
			errs <- e
		}(i)
		go func(i int) {
			defer wg.Done()
			_, e := dest.CreateNode(&Node{ID: NodeID(fmt.Sprintf("perf:local%d", i)), Properties: map[string]any{fmt.Sprintf("property_%03d", i): int64(i), fmt.Sprintf("local_%03d", i): int64(i)}})
			errs <- e
		}(i)
	}
	wg.Wait()
	close(errs)
	close(drains)
	for e := range errs {
		require.NoError(t, e)
	}
	for d := range drains {
		require.NoError(t, dest.propKeyDict.persistTxnCounters(dest.db, d))
	}
	assertPropertyDictionaryBijection(t, dest)
	for i := 0; i < 160; i++ {
		_, ok := dest.propKeyDict.lookupID("perf", fmt.Sprintf("property_%03d", i))
		require.True(t, ok)
	}
	require.NoError(t, dest.Close())
	dest = reviewBindingEngine(t, dir)
	assertPropertyDictionaryBijection(t, dest)
	for i := 0; i < 160; i++ {
		_, ok := dest.propKeyDict.lookupID("perf", fmt.Sprintf("property_%03d", i))
		require.True(t, ok)
	}
}

func TestReviewR2DoubleReplay(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	_, data := r2BindingData(t, 160)
	raw, err := json.Marshal(data)
	require.NoError(t, err)
	entry := WALEntry{Operation: OpBufferedCommit, Data: raw}
	dest := reviewBindingEngine(t, t.TempDir())
	_, err = dest.CreateNode(&Node{ID: "perf:sentinel", Properties: map[string]any{"other": "keep"}})
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		require.NoError(t, ReplayWALEntry(dest, entry))
		assertPropertyDictionaryBijection(t, dest)
	}
	node, err := dest.GetNode("perf:a")
	require.NoError(t, err)
	require.Len(t, node.Properties, 160)
	for i := 0; i < 160; i++ {
		require.Equal(t, int64(i), node.Properties[fmt.Sprintf("property_%03d", i)])
	}
}

func TestReviewR2ForeignIdentityWithoutObservedRemap(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source := reviewBindingEngine(t, t.TempDir())
	_, err := source.CreateNode(&Node{ID: "identity:a", Properties: map[string]any{"alpha": int64(0)}})
	require.NoError(t, err)
	_, err = source.CreateNode(&Node{ID: "identity:b", Properties: map[string]any{"beta": int64(0)}})
	require.NoError(t, err)
	capture, err := NewWAL(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = capture.Close() })
	source.SetBufferedCommitWAL(capture)
	for i, name := range []string{"alpha", "beta"} {
		tx, e := source.BeginTransaction()
		require.NoError(t, e)
		require.NoError(t, tx.SetNamespace("identity"))
		require.NoError(t, tx.SetImplicit(true))
		require.NoError(t, tx.UpdateNode(&Node{ID: NodeID(fmt.Sprintf("identity:%c", 'a'+i)), Properties: map[string]any{name: int64(i + 1)}}))
		require.NoError(t, tx.Commit())
		require.NoError(t, source.FlushWriteBehind())
	}
	entries, err := ReadWALEntriesFromDir(capture.Config().Dir)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	for i := range entries {
		record, e := decodeBufferedWALRecord(entries[i])
		require.NoError(t, e)
		if i == 1 {
			record.PropKeys = nil
			record.PropKeyCounters = nil
			record.PropertyBindings = nil
			record.PropertyDictionaryID = ""
		}
		entries[i].Data, e = json.Marshal(record)
		require.NoError(t, e)
	}
	wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	for _, entry := range entries {
		require.NoError(t, wal.Append(OpBufferedCommit, json.RawMessage(entry.Data)))
	}
	require.NoError(t, wal.Close())
	dir := t.TempDir()
	dest := reviewBindingEngine(t, dir)
	_, err = dest.CreateNode(&Node{ID: "identity:a", Properties: map[string]any{"alpha": int64(0)}})
	require.NoError(t, err)
	_, err = dest.CreateNode(&Node{ID: "identity:b", Properties: map[string]any{"other": int64(0)}})
	require.NoError(t, err)
	// The imported prefix happens to use an identical mapping; its unobserved
	// source token 2 means beta, while destination token 2 means other.
	require.NoError(t, ReplayWALEntry(dest, entries[0]))
	require.NoError(t, dest.Close())
	dest = reviewBindingEngine(t, dir)
	n, recoverErr := RecoverBufferedCommitsAtOpen(dest, wal)
	node, readErr := dest.GetNode("identity:b")
	require.NoError(t, readErr)
	t.Logf("replayed=%d err=%v cold-body=%#v foreign=%v", n, recoverErr, node.Properties, dest.walPropertySource.foreign)
	require.ErrorContains(t, recoverErr, "missing property name binding", "foreign destination identity must survive restart even when the first imported token happened to match")
}

func BenchmarkReviewR2OriginalImport(b *testing.B) {
	for _, count := range []int{1, 160} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			source, data := r2BindingData(b, count)
			// Simulate loaded provenance; no writes or dictionary changes occur in sample.
			data.PropertyDictionaryID = source.walPropertySource.identity

			data.PropertyBindings = nil
			_, err := source.importBufferedWALRecord(&data, walOwnDirectoryAtOpen)
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err = source.importBufferedWALRecord(&data, walOwnDirectoryAtOpen)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
