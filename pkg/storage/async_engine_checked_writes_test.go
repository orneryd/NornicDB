package storage

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newCheckedWritesAsync is an AsyncEngine whose flush never runs on its own,
// over an in-memory engine with a UNIQUE, a NODE KEY, a node DOMAIN and a
// relationship DOMAIN constraint in namespace "test".
func newCheckedWritesAsync(t *testing.T) (*AsyncEngine, *MemoryEngine) {
	t.Helper()
	inner := NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	schema := inner.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{Name: "u_email", Type: ConstraintUnique, Label: "User", Properties: []string{"email"}}))
	require.NoError(t, schema.AddConstraint(Constraint{Name: "k_user", Type: ConstraintNodeKey, Label: "Acct", Properties: []string{"tenant", "uid"}}))
	require.NoError(t, schema.AddConstraint(Constraint{Name: "d_state", Type: ConstraintDomain, Label: "Status", Properties: []string{"s"}, AllowedValues: []any{"a", "b"}}))
	require.NoError(t, schema.AddConstraint(Constraint{Name: "d_w", Type: ConstraintDomain, EntityType: ConstraintEntityRelationship, Label: "RD", Properties: []string{"w"}, AllowedValues: []any{int64(1), int64(2)}}))
	ae := NewAsyncEngine(inner, &AsyncEngineConfig{FlushInterval: time.Hour})
	t.Cleanup(func() { _ = ae.Close() })
	return ae, inner
}

func requireViolation(t *testing.T, err error, kind ConstraintType) {
	t.Helper()
	var cve *ConstraintViolationError
	require.ErrorAs(t, err, &cve)
	require.Equal(t, kind, cve.Type)
}

// TestAsyncEngineCheckedWritesAreCheckedBeforeTheyAreAcknowledged: a write a
// constraint applies to goes through to the engine, which checks it with
// every constraint kind; a violation is returned to the writer and nothing is
// cached, so a later flush never fails on it and never blocks other writers
// (#700). Writes no constraint applies to stay in the write cache.
func TestAsyncEngineCheckedWritesAreCheckedBeforeTheyAreAcknowledged(t *testing.T) {
	ae, inner := newCheckedWritesAsync(t)

	// A checked node is in the engine at once, not in the cache.
	_, err := ae.CreateNode(&Node{ID: "test:u1", Labels: []string{"User"}, Properties: map[string]any{"email": "a@x"}})
	require.NoError(t, err)
	stored, err := inner.GetNode("test:u1")
	require.NoError(t, err)
	require.Equal(t, "a@x", stored.Properties["email"])
	require.False(t, ae.HasPendingWrites())

	// UNIQUE, DOMAIN and NODE KEY violations are the writer's errors.
	_, err = ae.CreateNode(&Node{ID: "test:u2", Labels: []string{"User"}, Properties: map[string]any{"email": "a@x"}})
	requireViolation(t, err, ConstraintUnique)
	_, err = ae.CreateNode(&Node{ID: "test:s1", Labels: []string{"Status"}, Properties: map[string]any{"s": "c"}})
	requireViolation(t, err, ConstraintDomain)
	_, err = ae.CreateNode(&Node{ID: "test:k1", Labels: []string{"Acct"}, Properties: map[string]any{"tenant": "t"}})
	requireViolation(t, err, ConstraintNodeKey)
	require.False(t, ae.HasPendingWrites())

	// A batch is checked whole, duplicates within it included.
	err = ae.BulkCreateNodes([]*Node{
		{ID: "test:b1", Labels: []string{"User"}, Properties: map[string]any{"email": "dup@x"}},
		{ID: "test:b2", Labels: []string{"User"}, Properties: map[string]any{"email": "dup@x"}},
	})
	requireViolation(t, err, ConstraintUnique)
	err = ae.BulkCreateNodes([]*Node{
		{ID: "test:b3", Labels: []string{"Acct"}, Properties: map[string]any{"tenant": "t", "uid": "1"}},
		{ID: "test:b4", Labels: []string{"Acct"}, Properties: map[string]any{"tenant": "t", "uid": "1"}},
	})
	requireViolation(t, err, ConstraintNodeKey)
	_, err = inner.GetNode("test:b1")
	require.ErrorIs(t, err, ErrNotFound)

	// An unchecked node is cached; a checked relationship between cached
	// nodes flushes them first, then is checked.
	_, err = ae.CreateNode(&Node{ID: "test:p1", Labels: []string{"Plain"}})
	require.NoError(t, err)
	_, err = ae.CreateNode(&Node{ID: "test:p2", Labels: []string{"Plain"}})
	require.NoError(t, err)
	require.True(t, ae.HasPendingWrites())
	_, err = inner.GetNode("test:p1")
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, ae.CreateEdge(&Edge{ID: "test:r1", Type: "RD", StartNode: "test:p1", EndNode: "test:p2", Properties: map[string]any{"w": int64(1)}}))
	_, err = inner.GetEdge("test:r1")
	require.NoError(t, err)
	err = ae.CreateEdge(&Edge{ID: "test:r2", Type: "RD", StartNode: "test:p1", EndNode: "test:p2", Properties: map[string]any{"w": int64(3)}})
	requireViolation(t, err, ConstraintDomain)
	err = ae.UpdateEdge(&Edge{ID: "test:r1", Type: "RD", StartNode: "test:p1", EndNode: "test:p2", Properties: map[string]any{"w": int64(9)}})
	requireViolation(t, err, ConstraintDomain)

	// An update that gives a cached node a constrained label is checked.
	_, err = ae.CreateNode(&Node{ID: "test:p3", Labels: []string{"Plain"}, Properties: map[string]any{"email": "a@x"}})
	require.NoError(t, err)
	err = ae.UpdateNode(&Node{ID: "test:p3", Labels: []string{"Plain", "User"}, Properties: map[string]any{"email": "a@x"}})
	requireViolation(t, err, ConstraintUnique)

	// A cached delete of the value's holder is applied first, so the value
	// can be taken again.
	require.NoError(t, ae.DeleteNode("test:u1"))
	_, err = ae.CreateNode(&Node{ID: "test:u3", Labels: []string{"User"}, Properties: map[string]any{"email": "a@x"}})
	require.NoError(t, err)

	// Nothing rejected was cached: the flush writes the rest and leaves no
	// failure behind.
	result := ae.FlushWithResult()
	require.False(t, result.HasErrors(), "%+v", result)
	require.NoError(t, ae.Flush())
	require.False(t, ae.HasPendingWrites())
}

// TestAsyncEngineUnprefixedNodeIsRejectedWhenWritten: on an engine with no
// namespace, a node ID without a database prefix fails the write that brings
// it, not the flush.
func TestAsyncEngineUnprefixedNodeIsRejectedWhenWritten(t *testing.T) {
	inner := NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	ae := NewAsyncEngine(inner, &AsyncEngineConfig{FlushInterval: time.Hour})
	t.Cleanup(func() { _ = ae.Close() })
	_, err := ae.CreateNode(&Node{ID: "no-prefix", Labels: []string{"User"}})
	require.ErrorContains(t, err, "must be prefixed")
	require.ErrorContains(t, ae.BulkCreateNodes([]*Node{{ID: "no-prefix", Labels: []string{"User"}}}), "must be prefixed")
	require.False(t, ae.HasPendingWrites())
}

// TestAsyncEngineConcurrentWritersOfOneUniqueValue: writers racing on one
// UNIQUE value through the AsyncEngine, and a transaction committing it on
// the engine, store it once; every other writer gets the violation (#700).
func TestAsyncEngineConcurrentWritersOfOneUniqueValue(t *testing.T) {
	ae, inner := newCheckedWritesAsync(t)
	const writers = 16
	var wg sync.WaitGroup
	var won, violated atomic.Int32
	start := make(chan struct{})
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			var err error
			if i%4 == 0 {
				tx, beginErr := inner.BeginTransaction()
				require.NoError(t, beginErr)
				if _, err = tx.CreateNode(&Node{ID: NodeID("test:tx" + string(rune('a'+i))), Labels: []string{"User"}, Properties: map[string]any{"email": "race@x"}}); err == nil {
					err = tx.Commit()
				} else {
					_ = tx.Rollback()
				}
			} else {
				_, err = ae.CreateNode(&Node{ID: NodeID("test:ae" + string(rune('a'+i))), Labels: []string{"User"}, Properties: map[string]any{"email": "race@x"}})
			}
			var cve *ConstraintViolationError
			switch {
			case err == nil:
				won.Add(1)
			case errors.As(err, &cve):
				violated.Add(1)
			default:
				t.Errorf("writer %d: %v", i, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	require.EqualValues(t, 1, won.Load())
	require.EqualValues(t, writers-1, violated.Load())
	nodes, err := inner.GetNodesByLabel("User")
	require.NoError(t, err)
	holders := 0
	for _, node := range nodes {
		if node.Properties["email"] == "race@x" {
			holders++
		}
	}
	require.Equal(t, 1, holders)
	require.NoError(t, ae.Flush())
}

// TestAsyncEngineSchemaChangePausesWrites: while a schema change holds the
// write gate, a node write waits; it runs, checked against the new rule,
// once the change resumes writes.
func TestAsyncEngineSchemaChangePausesWrites(t *testing.T) {
	ae, inner := newCheckedWritesAsync(t)
	resume, err := ae.PauseWritesForSchemaChange()
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		_, err := ae.CreateNode(&Node{ID: "test:g1", Labels: []string{"Gated"}, Properties: map[string]any{"v": "x"}})
		done <- err
	}()
	select {
	case <-done:
		t.Fatal("write ran while the schema change held the gate")
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, inner.GetSchemaForNamespace("test").AddConstraint(Constraint{Name: "d_gated", Type: ConstraintDomain, Label: "Gated", Properties: []string{"v"}, AllowedValues: []any{"y"}}))
	resume()
	requireViolation(t, <-done, ConstraintDomain)
	require.False(t, ae.HasPendingWrites())
}

// TestTransactionConstraintChecksSeeValuesCommittedAfterItBegan: a UNIQUE or
// NODE KEY value another writer committed after a transaction began conflicts
// with the transaction's write, whether it's checked per write or at commit,
// and whether or not the constraint's value cache is complete (#700; the
// check used to scan the transaction's snapshot).
func TestTransactionConstraintChecksSeeValuesCommittedAfterItBegan(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		for _, completeCache := range []bool{false, true} {
			ae, inner := newCheckedWritesAsync(t)
			if completeCache {
				require.NoError(t, RefreshUniqueConstraintValuesForEngine(inner, inner.GetSchemaForNamespace("test")))
			}
			tx, err := inner.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetDeferredConstraintValidation(deferred))
			_, err = tx.GetNodesByLabel("User") // the transaction's snapshot
			require.NoError(t, err)

			_, err = ae.CreateNode(&Node{ID: "test:first", Labels: []string{"User"}, Properties: map[string]any{"email": "late@x"}})
			require.NoError(t, err)
			_, err = ae.CreateNode(&Node{ID: "test:firstk", Labels: []string{"Acct"}, Properties: map[string]any{"tenant": "t", "uid": "7"}})
			require.NoError(t, err)

			_, err = tx.CreateNode(&Node{ID: "test:second", Labels: []string{"User"}, Properties: map[string]any{"email": "late@x"}})
			if err == nil {
				err = tx.Commit()
			} else {
				_ = tx.Rollback()
			}
			requireViolation(t, err, ConstraintUnique)

			tx, err = inner.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetDeferredConstraintValidation(deferred))
			_, err = tx.CreateNode(&Node{ID: "test:secondk", Labels: []string{"Acct"}, Properties: map[string]any{"tenant": "t", "uid": "7"}})
			if err == nil {
				err = tx.Commit()
			} else {
				_ = tx.Rollback()
			}
			requireViolation(t, err, ConstraintNodeKey)
		}
	}
}

// TestConcurrentWritersOfOneNodeKey: writers racing on one NODE KEY value,
// through the AsyncEngine and in transactions, store it once.
func TestConcurrentWritersOfOneNodeKey(t *testing.T) {
	ae, inner := newCheckedWritesAsync(t)
	const writers = 12
	var wg sync.WaitGroup
	var won atomic.Int32
	start := make(chan struct{})
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			node := &Node{ID: NodeID("test:nk" + string(rune('a'+i))), Labels: []string{"Acct"}, Properties: map[string]any{"tenant": "t", "uid": "race"}}
			var err error
			if i%3 == 0 {
				tx, beginErr := inner.BeginTransaction()
				require.NoError(t, beginErr)
				if _, err = tx.CreateNode(node); err == nil {
					err = tx.Commit()
				} else {
					_ = tx.Rollback()
				}
			} else {
				_, err = ae.CreateNode(node)
			}
			if err == nil {
				won.Add(1)
				return
			}
			var cve *ConstraintViolationError
			if !errors.As(err, &cve) {
				t.Errorf("writer %d: %v", i, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	require.EqualValues(t, 1, won.Load())
}
