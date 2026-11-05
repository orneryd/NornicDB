package storage

import (
	"context"
	"errors"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func bufferedReceiptEntries(t *testing.T) []WALEntry {
	t.Helper()
	source := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true, manualDrain: true})
	require.NoError(t, source.BulkCreateNodes(t.Context(), []*Node{{ID: "test:receipt-a", Labels: []string{"Receipt"}}, {ID: "test:receipt-b", Labels: []string{"Receipt"}}}))
	require.NoError(t, source.BulkCreateNodes(t.Context(), []*Node{{ID: "test:receipt-c", Labels: []string{"Receipt"}}}))
	entries, err := ReadWALEntriesFromDir(source.bufferedCommitWAL.Config().Dir)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	return entries
}

func TestBufferedReplayDuplicateConflictPinsManagedReadAndRetries(t *testing.T) {
	entries := bufferedReceiptEntries(t)
	dest := newTestMemoryEngine(t).BadgerEngine
	require.True(t, dest.db.Opts().DetectConflicts)
	var notifications atomic.Int32
	dest.OnNodeCreated(func(*Node) { notifications.Add(1) })
	waiting := make(chan struct{}, 8)
	restore := setAdmissionWaitObserver(func(owner any, _ context.Context) {
		if owner == &dest.totalCountWriteMu || owner == &dest.labelCountWriteMu {
			select {
			case waiting <- struct{}{}:
			default:
			}
		}
	})
	defer restore()
	dest.totalCountWriteMu.Lock()
	var once sync.Once
	unlock := func() { once.Do(dest.totalCountWriteMu.Unlock) }
	defer unlock()
	done := make(chan error, 2)
	for range 2 {
		go func() { done <- ReplayWALEntry(dest, entries[0]) }()
	}
	joined := 0
	defer func() {
		unlock()
		for joined < 2 {
			<-done
			joined++
		}
	}()
	for range 2 {
		select {
		case <-waiting:
		case <-time.After(5 * time.Second):
			t.Fatal("both duplicate applies must reach the real count publication gate")
		}
	}
	dest.db.oracle.mu.Lock()
	readers := 0
	for _, n := range dest.db.oracle.readers {
		readers += n
	}
	dest.db.oracle.mu.Unlock()
	require.GreaterOrEqual(t, readers, 2, "both marker read sets have a managed read floor")
	dest.db.advanceDiscardTs()
	unlock()
	results := make([]error, 2)
	for i := range results {
		results[i] = <-done
		joined++
	}
	successful, conflicted := 0, 0
	for _, err := range results {
		if err == nil {
			successful++
		} else {
			require.ErrorIs(t, err, badger.ErrConflict)
			conflicted++
		}
	}
	require.Equal(t, 1, successful)
	require.Equal(t, 1, conflicted)
	require.NoError(t, ReplayWALEntry(dest, entries[0]), "explicit retry observes the applied marker")
	count, err := dest.NodeCount(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	nodes, err := dest.GetNodesByLabel(t.Context(), "Receipt")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	require.EqualValues(t, 2, notifications.Load())
	dest.db.oracle.mu.Lock()
	remaining := len(dest.db.oracle.readers)
	dest.db.oracle.mu.Unlock()
	require.Zero(t, remaining)
}

func TestBufferedReplayMixedDuplicateAndNewUsesOneApplyOwner(t *testing.T) {
	entries := bufferedReceiptEntries(t)
	dest := newTestMemoryEngine(t).BadgerEngine
	require.NoError(t, ReplayWALEntry(dest, entries[0]))
	var applied []bufferedCommit
	for _, index := range []int{0, 1, 0, 1} {
		data, err := decodeBufferedWALRecord(entries[index])
		require.NoError(t, err)
		commit, err := dest.importBufferedWALRecord(&data, walForeignImport)
		require.NoError(t, err)
		applied = append(applied, commit)
	}
	require.NoError(t, dest.applyBufferedCommits(applied))
	count, err := dest.NodeCount(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 3, count)
	nodes, err := dest.GetNodesByLabel(t.Context(), "Receipt")
	require.NoError(t, err)
	require.Len(t, nodes, 3)
	require.NoError(t, dest.applyBufferedCommits(applied))
	count, err = dest.NodeCount(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 3, count)
}

func TestBufferedReplayMarkerFailureReopenAndRetry(t *testing.T) {
	entries := bufferedReceiptEntries(t)
	dir := t.TempDir()
	dest := openLargeCommitTestEngine(t, dir)
	injected := errors.New("component native commit admission failure")
	dest.db.oracle.fail(injected)
	require.ErrorIs(t, ReplayWALEntry(dest, entries[0]), injected)
	data, err := decodeBufferedWALRecord(entries[0])
	require.NoError(t, err)
	require.NoError(t, dest.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(bufferedCommitMarkerKey(data.Namespace, data.TxID, data.LogicalTxID))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
	dest.db.oracle.mu.Lock()
	remaining := len(dest.db.oracle.readers)
	dest.db.oracle.mu.Unlock()
	require.Zero(t, remaining)
	require.NoError(t, dest.Close())
	reopened := openLargeCommitTestEngine(t, dir)
	require.NoError(t, ReplayWALEntry(reopened, entries[0]))
	count, err := reopened.NodeCount(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
}

func TestBufferedReplayEmptyAndMetadataOnlyRetireOnce(t *testing.T) {
	dest := newTestMemoryEngine(t).BadgerEngine
	commits := []bufferedCommit{{namespace: "test", txID: "empty-component"}, {namespace: "test", txID: "metadata-component", writes: map[string][]byte{"component:metadata": []byte("retained")}}}
	require.NoError(t, dest.applyBufferedCommits(commits))
	var retired, completed int
	require.NoError(t, dest.applyBufferedCommitsWithCountPublication(commits, func() { retired++ }, func() { completed++ }))
	require.Equal(t, 1, retired)
	require.Equal(t, 1, completed)
	count, err := dest.NodeCount(t.Context())
	require.NoError(t, err)
	require.Zero(t, count)
	require.NoError(t, dest.withView(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("component:metadata"))
		if err != nil {
			return err
		}
		value, err := item.ValueCopy(nil)
		require.Equal(t, []byte("retained"), value)
		return err
	}))
	dest.db.oracle.mu.Lock()
	remaining := len(dest.db.oracle.readers)
	dest.db.oracle.mu.Unlock()
	require.Zero(t, remaining)
}

func TestBufferedReplaySameLogicalMarkerAcrossPhysicalIDs(t *testing.T) {
	entries := bufferedReceiptEntries(t)
	dest := newTestMemoryEngine(t).BadgerEngine
	var commits []bufferedCommit
	for _, entry := range entries {
		data, err := decodeBufferedWALRecord(entry)
		require.NoError(t, err)
		data.LogicalTxID = "one-accepted-logical-receipt"
		commit, err := dest.importBufferedWALRecord(&data, walForeignImport)
		require.NoError(t, err)
		commits = append(commits, commit)
	}
	require.NotEqual(t, commits[0].txID, commits[1].txID)
	require.NoError(t, dest.applyBufferedCommits(commits))
	count, err := dest.NodeCount(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, count, "canonical logical receipt identity takes precedence over physical transaction IDs")
}

func TestBufferedCommitMarkerKeyPreservesExactStoredBytes(t *testing.T) {
	cases := [][3]string{{"", "", ""}, {"test", "physical", ""}, {"test", "ignored", "logical"}, {"café", "physical", "id/with:separators"}, {string([]byte{0xff, 0, ':', '/'}), "ignored", "logical"}, {strings.Repeat("n", 1024), "ignored", "long"}}
	for _, input := range cases {
		namespace, physical, logical := input[0], input[1], input[2]
		identity := physical
		if logical != "" {
			identity = "replication/" + strconv.Itoa(len(namespace)) + ":" + namespace + "/" + logical
		}
		expected := append([]byte{prefixBufferedFlush}, identity...)
		require.Equal(t, expected, bufferedCommitMarkerKey(namespace, physical, logical))
	}
	var retained []byte
	allocations := testing.AllocsPerRun(100, func() { retained = bufferedCommitMarkerKey("test", "physical", "logical") })
	require.NotEmpty(t, retained)
	require.LessOrEqual(t, allocations, float64(1), "the canonical key has one owned result, no intermediate identity copy")
}
