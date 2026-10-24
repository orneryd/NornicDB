package storage

import (
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLargeCommitDeleteVisibleBeforeCacheCallbacks(t *testing.T) {
	b := openLargeCommitTestEngine(t, t.TempDir())
	t.Cleanup(func() { _ = b.Close() })
	id := NodeID("test:large-durable-delete")
	_, err := b.CreateNode(&Node{ID: id, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	_, err = b.GetNode(id)
	require.NoError(t, err)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	stageLargeCreate(t, tx, 6000)
	require.NoError(t, tx.DeleteNode(id))
	batches := 0
	restoreBatch := setLargeCommitBatchHook(func(int) error { batches++; return nil })
	defer restoreBatch()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	restoreTail := setCommitTailHook(func() { close(entered); <-release })
	defer restoreTail()
	defer unblock()
	done := make(chan error, 1)
	go func() { done <- tx.Commit() }()
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("large transaction did not reach its durable tail")
	}
	_, readErr := b.GetNode(id)
	_, queryErr := b.GetQueryNode(id)
	unblock()
	require.NoError(t, <-done)
	require.Greater(t, batches, 1, "the test must exercise multiple physical batches")
	require.ErrorIs(t, readErr, ErrNotFound)
	require.ErrorIs(t, queryErr, ErrNotFound)
}

func TestLargeCommitRollbackIssuesBeforePublication(t *testing.T) {
	b := openLargeCommitTestEngine(t, t.TempDir())
	t.Cleanup(func() { _ = b.Close() })
	id := NodeID("test:rollback-visible")
	_, err := b.CreateNode(&Node{ID: id, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.UpdateNode(&Node{ID: id, Properties: map[string]any{"v": int64(2)}}))
	stageLargeCreate(t, tx, 6000)
	injected := errors.New("stop after durable batch")
	var generation, published uint64
	restore := setLargeCommitBatchHook(func(int) error {
		generation, published = b.nodeCacheGen.current(), b.db.oracle.published.Load()
		return injected
	})
	err = tx.Commit()
	restore()
	require.ErrorIs(t, err, injected)
	require.Greater(t, b.nodeCacheGen.current(), generation, "restore batches must issue invalidations before publishing")
	require.Greater(t, b.db.oracle.published.Load(), published)
	node, err := b.GetNode(id)
	require.NoError(t, err)
	require.Equal(t, int64(1), node.Properties["v"])
}

func TestDurableIssuanceGuardMutation(t *testing.T) {
	data, err := os.ReadFile("badger_cache_commit.go")
	require.NoError(t, err)
	source := strings.ReplaceAll(string(data), "\r\n", "\n")
	call := "\tissueNodeCacheCommit(engine, ts, cacheToken, scope)"
	require.Contains(t, source, call)
	for name, mutation := range map[string]string{
		"deleted":   strings.Replace(source, call, "", 1),
		"reordered": strings.Replace(strings.Replace(source, call, "", 1), "\tif err := txn.CommitAt", call+"\n\tif err := txn.CommitAt", 1),
	} {
		t.Run(name, func(t *testing.T) {
			require.NotEmpty(t, durableWriterViolations("badger_cache_commit.go", []byte(mutation)))
		})
	}
}

func TestDurableWriterGuardAdditionalBypasses(t *testing.T) {
	for _, body := range []string{
		"_ = b.db.DB.DropPrefix([]byte{prefixNode})", "_ = b.db.DB.DropAll()", "_ = b.db.DB.Load(nil, 1)",
		"wb := b.db.DB.NewManagedWriteBatch(); _ = wb.SetEntryAt(nil, 9)", "sw := b.db.DB.NewStreamWriter(); _ = sw.Prepare()",
		"s := singleNodeCacheWrites(id); s.nodeID = \"\"", "s := unknownNodeCacheWrites(nodeCacheUnknownManaged); s.known = true",
		"txn := b.db.openBatch(); _ = b.db.commitWithNodeCache(txn, nil, nil, noNodeCacheWrites())",
		"txn := b.db.openBatch(); _ = b.db.commitMetadata(txn)",
		"w := func(txn *badger.Txn) error { return txn.Set(nodeKey(id), nil) }; _ = b.withMetadataUpdate(w)",
	} {
		source := "package storage; func (b *BadgerEngine) writer(id NodeID) { " + body + " }"
		require.NotEmpty(t, durableWriterViolations("new_writer.go", []byte(source)), body)
	}
	source := "package storage; type sc = nodeCacheWriteScope; func writer() { _ = sc{known:true, declared:true} }"
	require.NotEmpty(t, durableWriterViolations("new_writer.go", []byte(source)))
}
