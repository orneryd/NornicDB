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

// TestAsyncEngineCheckedWritesThatPassAreStoredAtOnce: checked updates, bulk
// node creates and bulk relationship creates that pass their constraints are
// in the engine when the call returns, nothing is left in the write cache, and
// a violating update or bulk relationship create is the writer's error (#700).
func TestAsyncEngineCheckedWritesThatPassAreStoredAtOnce(t *testing.T) {
	ae, inner := newCheckedWritesAsync(t)

	_, err := ae.CreateNode(&Node{ID: "test:u1", Labels: []string{"User"}, Properties: map[string]any{"email": "a@x"}})
	require.NoError(t, err)
	require.NoError(t, ae.UpdateNode(&Node{ID: "test:u1", Labels: []string{"User"}, Properties: map[string]any{"email": "b@x"}}))
	stored, err := inner.GetNode("test:u1")
	require.NoError(t, err)
	require.Equal(t, "b@x", stored.Properties["email"])

	require.NoError(t, ae.BulkCreateNodes([]*Node{
		{ID: "test:u2", Labels: []string{"User"}, Properties: map[string]any{"email": "c@x"}},
		{ID: "test:p1", Labels: []string{"Plain"}},
		{ID: "test:p2", Labels: []string{"Plain"}},
	}))
	for _, id := range []NodeID{"test:u2", "test:p1", "test:p2"} {
		_, err := inner.GetNode(id)
		require.NoError(t, err, id)
	}

	require.NoError(t, ae.BulkCreateEdges([]*Edge{
		{ID: "test:e1", Type: "RD", StartNode: "test:p1", EndNode: "test:p2", Properties: map[string]any{"w": int64(1)}},
		{ID: "test:e2", Type: "PLAIN", StartNode: "test:p2", EndNode: "test:p1"},
	}))
	_, err = inner.GetEdge("test:e1")
	require.NoError(t, err)
	require.NoError(t, ae.UpdateEdge(&Edge{ID: "test:e1", Type: "RD", StartNode: "test:p1", EndNode: "test:p2", Properties: map[string]any{"w": int64(2)}}))
	edge, err := inner.GetEdge("test:e1")
	require.NoError(t, err)
	require.Equal(t, int64(2), edge.Properties["w"])
	requireViolation(t, ae.UpdateEdge(&Edge{ID: "test:e1", Type: "RD", StartNode: "test:p1", EndNode: "test:p2", Properties: map[string]any{"w": int64(3)}}), ConstraintDomain)
	requireViolation(t, ae.BulkCreateEdges([]*Edge{
		{ID: "test:e3", Type: "RD", StartNode: "test:p1", EndNode: "test:p2", Properties: map[string]any{"w": int64(9)}},
	}), ConstraintDomain)
	_, err = inner.GetEdge("test:e3")
	require.ErrorIs(t, err, ErrNotFound)
	require.False(t, ae.HasPendingWrites())
}

// TestAsyncEngineCheckedUpdateUsesTheStoredLabels: an update is checked by
// the labels the node had as well as the ones it gets, read from the write
// cache or, once flushed, from the engine; a node the engine doesn't have has
// none.
func TestAsyncEngineCheckedUpdateUsesTheStoredLabels(t *testing.T) {
	ae, inner := newCheckedWritesAsync(t)

	_, err := ae.CreateNode(&Node{ID: "test:n1", Labels: []string{"Plain"}})
	require.NoError(t, err)
	require.Equal(t, []string{"Plain"}, ae.cachedNodeLabels("test:n1"))
	require.NoError(t, ae.Flush())
	require.Equal(t, []string{"Plain"}, ae.cachedNodeLabels("test:n1"))
	require.Nil(t, ae.cachedNodeLabels("test:missing"))

	// Moving a node onto a checked label goes through to the engine.
	require.NoError(t, ae.UpdateNode(&Node{ID: "test:n1", Labels: []string{"User"}, Properties: map[string]any{"email": "n@x"}}))
	stored, err := inner.GetNode("test:n1")
	require.NoError(t, err)
	require.Equal(t, []string{"User"}, stored.Labels)
	require.False(t, ae.HasPendingWrites())

	// A relationship whose ID has no namespace isn't checked here; the engine
	// refuses it when it is written.
	require.False(t, ae.edgeWriteChecked(&Edge{ID: "no-prefix", Type: "RD"}))
}

// TestAsyncEngineSchemaPauseReportsAFailedFlush: when the cached writes can't
// be flushed before a schema change, the pause returns the error, and its
// resume function still releases the writers.
func TestAsyncEngineSchemaPauseReportsAFailedFlush(t *testing.T) {
	ae, inner := newCheckedWritesAsync(t)
	_, err := ae.CreateNode(&Node{ID: "test:p1", Labels: []string{"Plain"}})
	require.NoError(t, err)
	require.True(t, ae.HasPendingWrites())
	require.NoError(t, inner.Close())

	resume, err := ae.PauseWritesForSchemaChange()
	require.Error(t, err)
	require.NotNil(t, resume)
	resume()
	require.True(t, ae.writeGate.TryLock(), "the writers are released")
	ae.writeGate.Unlock()
}

// TestSchemaWriteChecksCoverEveryRuleKind: which labels and relationship
// types the schema checks on write: constraints, relationship policies (by
// their source and target labels), property types and constraint contracts.
func TestSchemaWriteChecksCoverEveryRuleKind(t *testing.T) {
	var none *SchemaManager
	require.False(t, none.NodeWriteChecked([]string{"A"}))
	require.False(t, none.EdgeWriteChecked("R"))
	require.False(t, none.HasWriteRules())

	sm := NewSchemaManager()
	require.False(t, sm.HasWriteRules())
	require.False(t, sm.NodeWriteChecked(nil))
	require.False(t, sm.EdgeWriteChecked(""))

	sm.constraints["p"] = Constraint{Name: "p", Type: ConstraintPolicy, EntityType: ConstraintEntityRelationship, Label: "LINKS", SourceLabel: "Src", TargetLabel: "Dst"}
	require.True(t, sm.HasWriteRules())
	require.True(t, sm.NodeWriteChecked([]string{"Other", "Src"}))
	require.True(t, sm.NodeWriteChecked([]string{"Dst"}))
	require.False(t, sm.NodeWriteChecked([]string{"Other"}))
	require.True(t, sm.EdgeWriteChecked("LINKS"))
	require.False(t, sm.EdgeWriteChecked("OTHER"))

	typed := NewSchemaManager()
	require.NoError(t, typed.AddPropertyTypeConstraint("t_node", "Typed", "v", PropertyTypeString))
	require.NoError(t, typed.AddPropertyTypeConstraint("t_rel", "TYPED", "v", PropertyTypeString, ConstraintEntityRelationship))
	require.True(t, typed.HasWriteRules())
	require.True(t, typed.NodeWriteChecked([]string{"Typed"}))
	require.True(t, typed.EdgeWriteChecked("TYPED"))
	require.False(t, typed.NodeWriteChecked([]string{"Other"}))
	require.False(t, typed.EdgeWriteChecked("OTHER"))

	contracts := NewSchemaManager()
	contracts.constraintContracts["cn"] = ConstraintContract{Name: "cn", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "Doc"}
	contracts.constraintContracts["cr"] = ConstraintContract{Name: "cr", TargetEntityType: string(ConstraintEntityRelationship), TargetLabelOrType: "CITES"}
	require.True(t, contracts.HasWriteRules())
	require.True(t, contracts.NodeWriteChecked([]string{"Doc"}))
	require.False(t, contracts.NodeWriteChecked([]string{"CITES"}))
	require.True(t, contracts.EdgeWriteChecked("CITES"))
	require.False(t, contracts.EdgeWriteChecked("Doc"))
}

// TestTransactionMayReuseAValueItDeleted: a transaction that deletes the node
// holding a UNIQUE value can create another node with that value, as in
// Neo4j: the committed-state check leaves out the node it deletes.
func TestTransactionMayReuseAValueItDeleted(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{Name: "u_email", Type: ConstraintUnique, Label: "User", Properties: []string{"email"}}))
	_, err = engine.CreateNode(&Node{ID: "test:old", Labels: []string{"User"}, Properties: map[string]any{"email": "a@x"}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetDeferredConstraintValidation(false))
	require.NoError(t, tx.DeleteNode("test:old"))
	_, err = tx.CreateNode(&Node{ID: "test:new", Labels: []string{"User"}, Properties: map[string]any{"email": "a@x"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	_, err = engine.GetNode("test:old")
	require.ErrorIs(t, err, ErrNotFound)
	stored, err := engine.GetNode("test:new")
	require.NoError(t, err)
	require.Equal(t, "a@x", stored.Properties["email"])
}

// TestAsyncEngineCheckedWriteFailsWhenTheCacheCannotBeFlushed: a checked
// write flushes the cached writes first; if that fails, the write fails with
// it and isn't made.
func TestAsyncEngineCheckedWriteFailsWhenTheCacheCannotBeFlushed(t *testing.T) {
	ae, inner := newCheckedWritesAsync(t)
	_, err := ae.CreateNode(&Node{ID: "test:p1", Labels: []string{"Plain"}})
	require.NoError(t, err)
	require.NoError(t, inner.Close())
	_, err = ae.CreateNode(&Node{ID: "test:u1", Labels: []string{"User"}, Properties: map[string]any{"email": "a@x"}})
	require.Error(t, err)
}

// TestConstraintKeyLocksCoverNodeConstraintsOnly: no nodes, or no schema,
// take no lock; a relationship constraint whose type shares a node's label
// takes none either.
func TestConstraintKeyLocksCoverNodeConstraintsOnly(t *testing.T) {
	var none *SchemaManager
	lockNodesForTest(none, &Node{ID: "test:n", Labels: []string{"X"}, Properties: map[string]any{"k": 1}})()
	sm := NewSchemaManager()
	lockNodesForTest(sm)()
	require.NoError(t, sm.AddConstraint(Constraint{Name: "r_k", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "X", Properties: []string{"k"}}))
	release := lockNodesForTest(sm, &Node{ID: "test:n", Labels: []string{"X"}, Properties: map[string]any{"k": 1}})
	release()
	sm.uniqueConstraintCommitLocksMu.Lock()
	require.Empty(t, sm.uniqueConstraintCommitLocks)
	sm.uniqueConstraintCommitLocksMu.Unlock()
}

// TestTransactionConstraintCheckFailsWhenTheEngineCannotBeRead: a constraint
// check that can't read the committed nodes fails the write.
func TestTransactionConstraintCheckFailsWhenTheEngineCannotBeRead(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{Name: "u_email", Type: ConstraintUnique, Label: "User", Properties: []string{"email"}}))
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	tx.mu.Lock()
	_, err = tx.committedConstraintNodesLocked("User")
	tx.mu.Unlock()
	require.NoError(t, err)
	require.NoError(t, engine.Close())
	tx.mu.Lock()
	_, err = tx.committedConstraintNodesLocked("User")
	tx.mu.Unlock()
	require.Error(t, err)
}
