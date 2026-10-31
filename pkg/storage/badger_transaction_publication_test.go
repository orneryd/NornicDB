package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// runPublicationTransaction stages through the same transaction owner for
// synchronous writes and the asynchronous commits used to observe listener
// publication. Staging completes before the asynchronous commit starts.
func runPublicationTransaction(engine interface {
	BeginTransaction() (*BadgerTransaction, error)
}, implicit bool, write func(*BadgerTransaction) error, result chan<- error) error {
	tx, err := engine.BeginTransaction()
	if err != nil {
		return err
	}
	if implicit {
		if err := tx.SetImplicit(true); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if err := write(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if result != nil {
		go func() { result <- tx.Commit() }()
		return nil
	}
	return tx.Commit()
}

func TestBadgerTransaction_PublicationBeforeReentrantListener(t *testing.T) {
	engine := newTestMemoryEngine(t)
	published := false
	engine.OnNodeCreated(func(node *Node) {
		if node.ID != "review:outer" {
			return
		}
		require.True(t, published, "accounting/admission callback precedes listener")
		_, err := engine.CreateNode(&Node{ID: "review:child"})
		require.NoError(t, err)
	})
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "review:outer"})
	require.NoError(t, err)
	require.NoError(t, tx.PublishCommit(context.Background(), CommitPublication{AfterPublication: func() { published = true }}))
	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
}

func TestBadgerTransaction_ListenerOrderAndSnapshotUnderReentry(t *testing.T) {
	engine := newTestMemoryEngine(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var observed []string
	engine.OnNodeCreated(func(node *Node) {
		if node.ID != "review:ordered" {
			return
		}
		close(entered)
		<-release
		mu.Lock()
		observed = append(observed, node.Properties["value"].(string))
		mu.Unlock()
	})
	engine.OnNodeUpdated(func(node *Node) {
		mu.Lock()
		observed = append(observed, node.Properties["value"].(string))
		mu.Unlock()
	})
	done := make(chan error, 1)
	require.NoError(t, runPublicationTransaction(engine, false, func(tx *BadgerTransaction) error {
		_, err := tx.CreateNode(&Node{ID: "review:ordered", Properties: map[string]any{"value": "first"}})
		return err
	}, done))
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("listener did not start")
	}
	updated := make(chan error, 1)
	require.NoError(t, runPublicationTransaction(engine, false, func(tx *BadgerTransaction) error {
		return tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "review:ordered", Properties: map[string]any{"value": "second"}}, Policy: ClientNodeEdits})
	}, updated))
	select {
	case err := <-updated:
		require.NoError(t, err)
	case <-time.After(time.Second):
		close(release)
		t.Fatal("independent publication waited for listener delivery")
	}
	close(release)
	require.NoError(t, <-done)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"first", "second"}, observed)
}

func TestBadgerTransaction_CloseWaitsForListenerWithoutWriteBarrier(t *testing.T) {
	engine := newTestMemoryEngine(t)
	entered, release := make(chan struct{}), make(chan struct{})
	callbackResult := make(chan error, 1)
	engine.OnNodeCreated(func(node *Node) {
		if node.ID != "review:close" {
			return
		}
		close(entered)
		<-release
		_, err := engine.CreateNode(&Node{ID: "review:after-close"})
		callbackResult <- err
	})
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "review:close"})
	require.NoError(t, err)
	committed := make(chan error, 1)
	go func() { committed <- tx.Commit() }()
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- engine.Close() }()
	require.Eventually(t, func() bool { engine.mu.RLock(); defer engine.mu.RUnlock(); return engine.closed }, time.Second, time.Millisecond)
	close(release)
	select {
	case err := <-callbackResult:
		require.ErrorIs(t, err, ErrStorageClosed)
	case <-time.After(time.Second):
		t.Fatal("Close held write barrier against listener re-entry")
	}
	require.NoError(t, <-committed)
	require.NoError(t, <-closed)
}

func TestBadgerTransaction_RejectedCommitDoesNotPublish(t *testing.T) {
	engine := newTestMemoryEngine(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "review:canceled"})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, tx.PublishCommit(ctx, CommitPublication{AfterPublication: func() { t.Error("failed commit published") }}), context.Canceled)
	require.NoError(t, tx.Rollback())
	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestBadgerTransaction_ListenerPanicPreservesPublicationAndQueue(t *testing.T) {
	engine := newTestMemoryEngine(t)
	engine.OnNodeCreated(func(node *Node) {
		if node.ID == "review:panic" {
			panic("listener failed")
		}
	})
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "review:panic"})
	require.NoError(t, err)
	published := false
	require.PanicsWithValue(t, "listener failed", func() {
		_ = tx.PublishCommit(context.Background(), CommitPublication{AfterPublication: func() { published = true }})
	})
	require.True(t, published)
	_, err = engine.GetNode("review:panic")
	require.NoError(t, err, "listener failure cannot erase committed data")
	next, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = next.CreateNode(&Node{ID: "review:after-panic"})
	require.NoError(t, err)
	require.NoError(t, next.Commit(), "the notification queue remains usable after a surfaced callback panic")
}

// publicationContextCheckpoint places an ownership handoff deterministically
// after PublishCommit's initial client check and before its locked validation.
type publicationContextCheckpoint struct {
	context.Context
	once    sync.Once
	handoff func()
}

func (c *publicationContextCheckpoint) Err() error {
	c.once.Do(c.handoff)
	return c.Context.Err()
}

func TestPublishCommitRechecksDetachedOwnershipUnderLock(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{synchronous: true})
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer func() {
		// The accepted owner is simulated only at the lifecycle boundary. Restore
		// client cleanup after the assertion so this isolated fixture always closes.
		tx.clientDetached.Store(false)
		_ = tx.Rollback()
	}()
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(&Node{ID: "test:detached-publication"})
	require.NoError(t, err)
	ctx := &publicationContextCheckpoint{Context: context.Background(), handoff: func() {
		tx.mu.Lock()
		tx.clientDetached.Store(true)
		tx.mu.Unlock()
	}}
	published := false
	err = tx.PublishCommit(ctx, CommitPublication{AfterPublication: func() { published = true }})
	require.ErrorIs(t, err, ErrTransactionClosed)
	require.False(t, published)
	_, err = engine.GetNode("test:detached-publication")
	require.ErrorIs(t, err, ErrNotFound)
}

// Client detachment can happen while a caller waits for tx.mu. Neither a
// lifecycle query nor rollback may regain ownership after that handoff.
func TestClientLifecycleRechecksDetachedOwnershipUnderLock(t *testing.T) {
	for _, operation := range []string{"active", "rollback"} {
		t.Run(operation, func(t *testing.T) {
			engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{synchronous: true})
			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() {
				tx.clientDetached.Store(false)
				_ = tx.Rollback()
			})
			waiting := make(chan struct{})
			var once sync.Once
			t.Cleanup(setAdmissionWaitObserver(func(owner any, _ context.Context) {
				if owner == &tx.mu {
					once.Do(func() { close(waiting) })
				}
			}))
			tx.mu.Lock()
			locked := true
			defer func() {
				if locked {
					tx.mu.Unlock()
				}
			}()
			type outcome struct {
				active bool
				err    error
			}
			done := make(chan outcome, 1)
			joined := make(chan struct{})
			t.Cleanup(func() {
				select {
				case <-joined:
				case <-time.After(3 * time.Second):
					t.Error("client lifecycle worker did not join")
				}
			})
			go func() {
				defer close(joined)
				if operation == "active" {
					done <- outcome{active: tx.IsActive()}
				} else {
					done <- outcome{err: tx.Rollback()}
				}
			}()
			select {
			case <-waiting:
			case <-time.After(3 * time.Second):
				t.Fatal("client did not reach the held transaction mutex")
			}
			tx.clientDetached.Store(true)
			tx.mu.Unlock()
			locked = false
			select {
			case got := <-done:
				if operation == "active" {
					require.False(t, got.active)
				} else {
					require.ErrorIs(t, got.err, ErrTransactionClosed)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("client did not finish")
			}
			tx.mu.Lock()
			status := tx.Status
			tx.mu.Unlock()
			require.Equal(t, TxStatusActive, status, "the native accepted owner retains the transaction")
		})
	}
}
