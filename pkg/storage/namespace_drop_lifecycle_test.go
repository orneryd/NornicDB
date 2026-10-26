package storage

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

func TestNamespaceDropPendingScopesRemainIndependent(t *testing.T) {
	b := newTestEngine(t)
	for _, id := range []NodeID{"first:n", "second:n", "peer:n"} {
		_, err := b.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	sentinel := errors.New("cleanup unavailable")
	b.dropPrefixTailHook = func(prefix string) error {
		if prefix == "first:" || prefix == "second:" {
			return sentinel
		}
		return nil
	}
	for _, prefix := range []string{"first:", "second:"} {
		_, _, err := b.DeleteByPrefix(prefix)
		require.ErrorIs(t, err, sentinel)
	}
	require.EqualValues(t, 2, b.prefixDrops.pendingCount.Load())
	_, _, err := b.DeleteByPrefix("peer:")
	require.NoError(t, err, "a pending victim must not fence peer lifecycle work")
	_, err = b.CreateNode(&Node{ID: "peer:after"})
	require.NoError(t, err)
	_, _, err = b.DeleteByPrefix("first:partial")
	require.ErrorIs(t, err, ErrConflict, "only the exact pending whole DROP may resume")
	b.dropPrefixTailHook = nil
	_, _, err = b.DeleteByPrefix("first:")
	require.NoError(t, err)
	require.EqualValues(t, 1, b.prefixDrops.pendingCount.Load())
	_, err = b.CreateNode(&Node{ID: "first:after"})
	require.NoError(t, err)
	_, err = b.CreateNode(&Node{ID: "second:blocked"})
	require.ErrorIs(t, err, ErrConflict)
	_, _, err = b.DeleteByPrefix("second:")
	require.NoError(t, err)
	require.Zero(t, b.prefixDrops.pendingCount.Load())
	count, err := b.NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
}

func TestNamespaceDropSchemaWriterBeforeAndAfterFence(t *testing.T) {
	b := newTestEngine(t)
	_, err := b.CreateNode(&Node{ID: "victim:old"})
	require.NoError(t, err)
	schema := b.GetSchemaForNamespace("victim")
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	var releaseOnce sync.Once
	resume := func() { releaseOnce.Do(func() { close(release) }) }
	restore := setCommitTailHook(func() {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
	})
	schemaDone, dropDone := make(chan error, 1), make(chan error, 1)
	schemaJoined, dropJoined := make(chan struct{}), make(chan struct{})
	dropStarted := false
	t.Cleanup(func() {
		resume()
		select {
		case <-schemaJoined:
		case <-time.After(5 * time.Second):
			t.Error("schema writer did not join")
		}
		if dropStarted {
			select {
			case <-dropJoined:
			case <-time.After(5 * time.Second):
				t.Error("DROP writer did not join")
			}
		}
		restore()
	})
	go func() { defer close(schemaJoined); schemaDone <- schema.AddUniqueConstraint("before", "Before", "uid") }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("schema writer did not reach its real committed metadata tail")
	}
	sentinel := errors.New("intrinsic tail failed")
	b.dropPrefixTailHook = func(string) error { return sentinel }
	dropStarted = true
	go func() { defer close(dropJoined); _, _, err := b.DeleteByPrefix("victim:"); dropDone <- err }()
	select {
	case err := <-dropDone:
		require.ErrorIs(t, err, sentinel)
	case <-time.After(5 * time.Second):
		t.Fatal("fence installation waited for a schema mutex held by an already committed writer")
	}
	resume()
	select {
	case err := <-schemaDone:
		require.NoError(t, err, "its physical metadata commit preceded the installation cut")
	case <-time.After(5 * time.Second):
		t.Fatal("prior schema writer did not return")
	}
	require.ErrorIs(t, schema.AddUniqueConstraint("after", "After", "uid"), ErrConflict, "new schema persistence cannot slip behind the installed physical fence")
	restore()
	b.dropPrefixTailHook = nil
	_, _, err = b.DeleteByPrefix("victim:")
	require.NoError(t, err)
	require.Empty(t, b.GetSchemaForNamespace("victim").ExportDefinition().Constraints)
}

func TestNamespaceDropSuccessUnfencesBeforeResetListener(t *testing.T) {
	b := newTestEngine(t)
	_, err := b.CreateNode(&Node{ID: "victim:old"})
	require.NoError(t, err)
	var callbackErr error
	var callbacks int
	b.OnNamespaceReset(func(prefix string) {
		if prefix == "victim:" {
			callbacks++
			_, callbackErr = b.CreateNode(&Node{ID: "victim:listener"})
		}
	})
	_, _, err = b.DeleteByPrefix("victim:")
	require.NoError(t, err)
	require.Equal(t, 1, callbacks)
	require.NoError(t, callbackErr)
	require.NoError(t, b.pendingNamespaceConflict("victim"))
	_, err = b.GetNode("victim:listener")
	require.NoError(t, err)
}

func TestNamespaceDropPendingMetadataWritersUseActualTarget(t *testing.T) {
	b := newTestEngine(t)
	for _, id := range []NodeID{"victim:n", "peer:n"} {
		_, err := b.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	sentinel := errors.New("intrinsic cleanup failed")
	b.dropPrefixTailHook = func(string) error { return sentinel }
	_, _, err := b.DeleteByPrefix("victim:")
	require.ErrorIs(t, err, sentinel)
	require.ErrorIs(t, b.PutAccessMeta("victim:n", &knowledgepolicy.AccessMetaEntry{TargetID: "victim:n"}), ErrConflict)
	require.ErrorIs(t, b.DeleteAccessMeta("victim:n"), ErrConflict)
	require.ErrorIs(t, b.PutDeindexWorkItem(&DeindexWorkItem{WorkItemID: "opaque-peer-looking", TargetID: "victim:n"}), ErrConflict)
	require.NoError(t, b.PutDeindexWorkItem(&DeindexWorkItem{WorkItemID: "victim:opaque", TargetID: "peer:n"}))
	require.NoError(t, b.DeleteDeindexWorkItem("victim:opaque"))
	require.NoError(t, b.PutAccessMeta("peer:n", &knowledgepolicy.AccessMetaEntry{TargetID: "peer:n"}))
	b.AddToPendingEmbeddings("victim:new")
	err = b.withView(func(txn *badger.Txn) error { _, err := txn.Get(pendingEmbedKey("victim:new")); return err })
	require.ErrorIs(t, err, badger.ErrKeyNotFound, "void pending producer must not leave durable work in an unfinished DROP")
	b.dropPrefixTailHook = nil
	_, _, err = b.DeleteByPrefix("victim:")
	require.NoError(t, err)
	require.NoError(t, b.PutAccessMeta("victim:n", &knowledgepolicy.AccessMetaEntry{TargetID: "victim:n"}))
}
