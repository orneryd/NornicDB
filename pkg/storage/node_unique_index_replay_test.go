package storage

import (
	"context"
	"errors"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNodeUniqueIndexPrefixReclamation(t *testing.T) {
	b, schema := newUniqueIndexFixture(t)
	_, err := b.CreateNode(uniqueIndexNode("test:victim", "old"))
	require.NoError(t, err)
	_, err = b.CreateNode(uniqueIndexNode("test:keeper", "other"))
	require.NoError(t, err)
	peer, err := b.BeginTransaction()
	require.NoError(t, err)
	defer peer.Rollback()
	require.NoError(t, peer.SetDeferredConstraintValidation(true))
	require.NoError(t, peer.UpdateNode(uniqueIndexNode("test:victim", "renamed")))
	require.NoError(t, peer.UpdateNode(uniqueIndexNode("test:keeper", "old")))
	planned, resume := make(chan struct{}), make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(resume) })
	b.nodeUniquePrefixDropPlanned = func() { close(planned); <-resume }
	dropped := make(chan error, 1)
	go func() { _, _, err := b.DeleteByPrefix("test:victim"); dropped <- err }()
	select {
	case <-planned:
	case <-time.After(10 * time.Second):
		t.Fatal("prefix drop never planned its keys")
	}
	committed := make(chan error, 1)
	go func() { committed <- peer.Commit() }()
	var peerErr error
	finished := false
	// With the old split boundary the peer can reclaim the selected key.
	// With atomic planning it waits for the native drop to publish.
	select {
	case peerErr = <-committed:
		finished = true
	case <-time.After(time.Second):
	}
	release.Do(func() { close(resume) })
	select {
	case err := <-dropped:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("prefix drop did not finish")
	}
	if !finished {
		select {
		case peerErr = <-committed:
		case <-time.After(10 * time.Second):
			t.Fatal("peer did not finish")
		}
	}
	if peerErr != nil {
		require.True(t, errors.Is(peerErr, ErrConflict), "peer lost to prefix deletion: %v", peerErr)
	}
	// Reuse is also valid when the peer lost its victim to the drop.
	// Rewriting the same value must not be needed to repair a lost index entry.
	require.NoError(t, b.UpdateNode(uniqueIndexNode("test:keeper", "old")))
	id, found, _, complete, err := schema.LookupUniqueConstraintValueForPlanningChecked("Doc", "id", "old")
	require.NoError(t, err)
	require.True(t, found, "a prefix drop cannot delete another node's newly committed reservation")
	require.True(t, complete)
	require.Equal(t, NodeID("test:keeper"), id)
	_, err = b.CreateNode(uniqueIndexNode("test:duplicate", "old"))
	var violation *ConstraintViolationError
	require.ErrorAs(t, err, &violation)
}

// A commit compares its current key image with the merged stored-node view,
// even when an older replay and a newer cache publication disagree.
func TestNodeUniqueIndexCurrentImageAfterReplay(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	defer b.Close()
	schema := b.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddUniqueConstraint("doc_id", "Doc", "id"))
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(NewNamespacedEngine(b, "test"), schema))
	makeNode := func(value any) *Node {
		return &Node{ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{"id": value, "note": "seed"}}
	}
	_, err = b.CreateNode(makeNode("seed"))
	require.NoError(t, err)
	ack, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, ack.SetImplicit(true))
	require.NoError(t, ack.UpdateNode(makeNode("old")))
	require.NoError(t, ack.Commit())
	peer, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, peer.UpdateNode(makeNode(nil)))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	defer release()
	var armed atomic.Bool
	reset := setCommitTailHook(func() {
		if armed.CompareAndSwap(false, true) {
			close(entered)
			select {
			case <-resume:
			case <-ctx.Done():
			}
		}
	})
	defer reset()
	committed := make(chan error, 1)
	go func() { committed <- peer.CommitContext(ctx) }()
	select {
	case <-entered:
	case err := <-committed:
		t.Fatalf("peer missed publication barrier: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	flushed := make(chan error, 1)
	go func() { flushed <- b.FlushWriteBehind() }()
	select {
	case err := <-flushed:
		require.NoError(t, err)
	case <-ctx.Done():
		release()
		t.Fatal("flush blocked before peer publication")
	}
	release()
	require.NoError(t, <-committed)
	cached, err := b.GetNode("test:a")
	require.NoError(t, err)
	var physical *Node
	require.NoError(t, b.StreamStoredNodesByLabelProjected("Doc", []string{"id", "note"}, func(node *Node) error {
		if node.ID == "test:a" {
			physical = copyNode(node)
		}
		return nil
	}))
	require.NotNil(t, physical)
	t.Logf("PUBLIC CACHED=%v MERGED STORED=%v", cached.Properties, physical.Properties)
	require.Equal(t, "old", physical.Properties["id"], "the acknowledged replay was published")
	// The cache disagreement is owned separately. It must not make this new
	// non-key transaction conflict with an unchanged stored key image.
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	after := copyNode(physical)
	after.Properties["note"] = "nonkey"
	require.NoError(t, tx.UpdateNodeObserved(physical, after))
	require.NoError(t, tx.Commit(), "current key validation must use stored data, not an obsolete read cache")
	_, err = b.CreateNode(&Node{ID: "test:b", Labels: []string{"Doc"}, Properties: map[string]any{"id": "old"}})
	t.Logf("CREATE old after forced physical read: %v", err)
	var count int
	require.NoError(t, b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerPrefixIteratorOptions(append([]byte{prefixNode}, []byte("test:")...)))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := it.Item().Value(func(data []byte) error {
				node, err := b.decodeNode("test", data)
				if err == nil && node.Properties["id"] == "old" {
					count++
				}
				return err
			}); err != nil {
				return err
			}
		}
		return nil
	}))
	t.Logf("physical holders of old: %d", count)
	require.Equal(t, 1, count, "physical graph must not contain duplicate constrained keys")
}
