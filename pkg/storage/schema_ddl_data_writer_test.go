package storage

// Ordinary schema DDL and an unrelated concurrent data writer.
//
// A schema statement's fill reads the stored population at the transaction's
// snapshot, and the namespace MVCC commit sequence moves only for entity
// commits. The commit-time guard in badger_transaction_publication.go compares
// that sequence with the statement's read snapshot, so an autocommit schema
// statement that let a data commit land inside its window rejected its own
// schema commit with "schema snapshot changed before commit". An autocommit
// schema statement now admits itself before capturing its snapshot
// (AutocommitSchemaBegin): it owns the engine's exclusive lifecycle write
// barrier until publication, so its window holds no entity commit at all, and
// the population it published holds every write acknowledged before it.
//
// An explicit transaction keeps its own snapshot and its guards. A data commit
// that lands inside an explicit DDL statement's window still rejects that
// statement's commit, and a peer schema publication that lands inside it still
// rejects it too: those tests pin that the fix narrowed nothing.
//
// The schedules below are exact at the fill's documented entry point; the tests
// observe the concurrent writer's own admission (its completion, or its blocked
// write admission) rather than sleeping. The only budget is a join guard for a
// signal that never arrives; it never bounds an assertion.
//
// These tests install package-level hooks, so they must not call t.Parallel.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// schemaDDLJoinBudget bounds a wait for a control point or for a launched
// writer that a lost observation could otherwise turn into a hung run.
const schemaDDLJoinBudget = 10 * time.Second

// schemaDDLFixture is one namespace with its live schema manager.
type schemaDDLFixture struct {
	engine    *MemoryEngine
	badger    *BadgerEngine
	namespace string
	schema    *SchemaManager
}

func newSchemaDDLFixture(t *testing.T, namespace string) *schemaDDLFixture {
	t.Helper()
	engine := newTestMemoryEngine(t)
	return &schemaDDLFixture{engine: engine, badger: engine.BadgerEngine, namespace: namespace, schema: engine.GetSchemaForNamespace(namespace)}
}

// seed nodes go through the engine's own write path, like an autocommit
// statement's data write.
func (f *schemaDDLFixture) seed(t *testing.T, values ...string) {
	t.Helper()
	for _, value := range values {
		_, err := f.engine.CreateNode(context.Background(), &Node{
			ID:         NodeID(f.namespace + ":seed-" + value),
			Labels:     []string{"W"},
			Properties: map[string]interface{}{"v": value},
		})
		require.NoError(t, err)
	}
}

// writeThroughEngine commits one more node through the engine's own write path,
// as an autocommit data statement's writer does.
func (f *schemaDDLFixture) writeThroughEngine(t *testing.T, id, value string) {
	t.Helper()
	_, err := f.engine.CreateNode(context.Background(), &Node{
		ID:         NodeID(f.namespace + ":" + id),
		Labels:     []string{"W"},
		Properties: map[string]interface{}{"v": value},
	})
	require.NoError(t, err)
}

// beginStagedDDL opens an autocommit schema statement: the schema admission, its
// snapshot taken under that admission, and the isolated schema view.
func (f *schemaDDLFixture) beginStagedDDL(t *testing.T) (*BadgerTransaction, *SchemaManager) {
	t.Helper()
	tx, err := f.schema.BeginSchemaTransaction(context.Background(), f.engine.BeginTransaction)
	require.NoError(t, err)
	return f.bindSchemaView(t, tx)
}

// beginExplicitDDL opens an explicit transaction's schema statement: its own
// snapshot and its commit-time guards, with no autocommit admission. Two
// autocommit schema statements cannot be open at once, so this is the shape a
// statement that must observe a peer's publication inside its window uses.
func (f *schemaDDLFixture) beginExplicitDDL(t *testing.T) (*BadgerTransaction, *SchemaManager) {
	t.Helper()
	tx, err := f.engine.BeginTransaction(context.Background(), OrdinaryBegin)
	require.NoError(t, err)
	return f.bindSchemaView(t, tx)
}

// bindSchemaView pins the statement's namespace and binds its isolated schema
// view. A failed assertion still closes the statement's snapshot and admission.
func (f *schemaDDLFixture) bindSchemaView(t *testing.T, tx *BadgerTransaction) (*BadgerTransaction, *SchemaManager) {
	t.Helper()
	require.NoError(t, tx.SetNamespace(f.namespace))
	t.Cleanup(func() { _ = tx.Rollback() })
	view, err := tx.Schema(OrdinarySchemaChanges)
	require.NoError(t, err)
	return tx, view
}

// fillEntries reads the label's stored population as the statement's fill does.
func (f *schemaDDLFixture) fillEntries(t *testing.T, label string) map[NodeID]map[string]interface{} {
	t.Helper()
	nodes, err := GetStoredNodesByLabel(context.Background(), f.engine, label)
	require.NoError(t, err)
	entries := make(map[NodeID]map[string]interface{}, len(nodes))
	for _, node := range nodes {
		if node == nil || node.Properties == nil {
			continue
		}
		entries[node.ID] = node.Properties
	}
	return entries
}

// storedNodeCount counts the label's stored nodes, the population assertion a
// rejected schema commit must leave untouched.
func (f *schemaDDLFixture) storedNodeCount(t *testing.T, label string) int {
	t.Helper()
	nodes, err := GetStoredNodesByLabel(context.Background(), f.engine, label)
	require.NoError(t, err)
	return len(nodes)
}

// schemaDDLDataWriter is one launched autocommit data write. blocked closes if
// the write's own admission waits, which is the exact control point showing the
// schema statement excludes data commits for its window.
type schemaDDLDataWriter struct {
	blocked chan struct{}
	done    chan struct{}
	once    sync.Once
	mu      sync.Mutex
	err     error
}

func (f *schemaDDLFixture) launchDataWriter(t *testing.T, node *Node) *schemaDDLDataWriter {
	t.Helper()
	writer := &schemaDDLDataWriter{blocked: make(chan struct{}), done: make(chan struct{})}
	restore := setAdmissionWaitObserver(func(owner any, _ context.Context) {
		if owner == &f.badger.writeBarrier {
			writer.once.Do(func() { close(writer.blocked) })
		}
	})
	go func() {
		ctx := context.Background()
		tx, err := f.engine.BeginTransaction(ctx, OrdinaryBegin)
		if err == nil {
			err = tx.SetNamespace(f.namespace)
		}
		if err == nil {
			_, err = tx.CreateNode(ctx, node)
		}
		if err == nil {
			err = tx.Commit()
		}
		writer.mu.Lock()
		writer.err = err
		writer.mu.Unlock()
		close(writer.done)
	}()
	t.Cleanup(func() {
		restore()
		select {
		case <-writer.done:
		case <-time.After(schemaDDLJoinBudget):
			t.Errorf("the concurrent data writer did not return within %s", schemaDDLJoinBudget)
		}
	})
	return writer
}

// join waits for the launched writer's outcome after the schema statement has
// released its admission.
func (w *schemaDDLDataWriter) join(t *testing.T) error {
	t.Helper()
	select {
	case <-w.done:
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.err
	case <-time.After(schemaDDLJoinBudget):
		t.Fatalf("the concurrent data writer did not return within %s", schemaDDLJoinBudget)
		return nil
	}
}

// schemaDDLParkedFill is one fill parked at the index fill's documented entry
// point for one index identity.
type schemaDDLParkedFill struct {
	entered chan struct{}
	done    chan struct{}
	release chan struct{}
	unpark  sync.Once
	once    sync.Once
	mu      sync.Mutex
	err     error
}

// parkSchemaIndexFill parks the index fill for indexName at its documented entry
// point and runs it on its own goroutine. Its cleanup releases a fill a failed
// assertion left parked, restores the hook and joins that goroutine, so a
// failing test leaves neither a parked fill nor a running goroutine behind.
func parkSchemaIndexFill(t *testing.T, indexName string, fill func() error) *schemaDDLParkedFill {
	t.Helper()
	parked := &schemaDDLParkedFill{entered: make(chan struct{}), done: make(chan struct{}), release: make(chan struct{})}
	restore := SetSchemaIndexFillHookForTesting(func(name, _ string) error {
		if name != indexName {
			return nil
		}
		parked.once.Do(func() { close(parked.entered) })
		<-parked.release
		return nil
	})
	go func() {
		err := fill()
		parked.mu.Lock()
		parked.err = err
		parked.mu.Unlock()
		close(parked.done)
	}()
	t.Cleanup(func() {
		parked.unparkFill()
		restore()
		select {
		case <-parked.done:
		case <-time.After(schemaDDLJoinBudget):
			t.Errorf("the %q fill goroutine did not return within %s", indexName, schemaDDLJoinBudget)
		}
	})
	select {
	case <-parked.entered:
	case <-time.After(schemaDDLJoinBudget):
		t.Fatalf("the %q fill never parked", indexName)
	}
	return parked
}

func (p *schemaDDLParkedFill) unparkFill() {
	p.unpark.Do(func() { close(p.release) })
}

// finish releases the parked fill and returns its outcome.
func (p *schemaDDLParkedFill) finish(t *testing.T) error {
	t.Helper()
	p.unparkFill()
	select {
	case <-p.done:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.err
	case <-time.After(schemaDDLJoinBudget):
		t.Fatalf("the parked fill did not return within %s", schemaDDLJoinBudget)
		return nil
	}
}

// TestSchemaSequenceMovesOnlyForEntityCommits pins the causal scope of the
// guard's input: a namespace's MVCC commit sequence advances for entity commits
// and not for a schema-only publication.
func TestSchemaSequenceMovesOnlyForEntityCommits(t *testing.T) {
	ctx := context.Background()
	fixture := newSchemaDDLFixture(t, "guard_scope")
	fixture.seed(t, "seed")

	ddl, view := fixture.beginStagedDDL(t)
	require.NoError(t, view.AddCompositeIndex("w_v", "W", []string{"v"}))
	require.NoError(t, view.BackfillCompositeIndex(ctx, "w_v", fixture.fillEntries(t, "W")))
	require.NoError(t, ddl.StageSchemaChanges())
	before := fixture.badger.currentMVCCReadVersion(fixture.namespace).CommitSequence
	require.NoError(t, ddl.Commit())
	after := fixture.badger.currentMVCCReadVersion(fixture.namespace).CommitSequence
	require.Equal(t, before, after, "a schema-only commit must not move the namespace entity sequence")

	before = fixture.badger.currentMVCCReadVersion(fixture.namespace).CommitSequence
	fixture.writeThroughEngine(t, "data", "data")
	after = fixture.badger.currentMVCCReadVersion(fixture.namespace).CommitSequence
	require.Greater(t, after, before, "an entity commit is the only mover of the guard's input")
}

// TestCreateIndexCommitSurvivesConcurrentDataWriter is the fail-first
// reproduction for an index-build statement whose window contains an unrelated
// data commit: the statement must commit, and the index it publishes must hold
// that commit's node.
func TestCreateIndexCommitSurvivesConcurrentDataWriter(t *testing.T) {
	ctx := context.Background()
	fixture := newSchemaDDLFixture(t, "ddl_data_peer")
	fixture.seed(t, "seed")

	ddl, view := fixture.beginStagedDDL(t)
	require.NoError(t, view.AddCompositeIndex("w_v", "W", []string{"v"}))

	// The fill is the statement's population owner: it reads the stored nodes
	// at the statement's snapshot, parked here before it files any entry.
	fill := parkSchemaIndexFill(t, "w_v", func() error {
		return view.BackfillCompositeIndex(ctx, "w_v", fixture.fillEntries(t, "W"))
	})

	writer := fixture.launchDataWriter(t, &Node{
		ID:         NodeID(fixture.namespace + ":peer"),
		Labels:     []string{"W"},
		Properties: map[string]interface{}{"v": "peer"},
	})

	// The statement's window holds no entity commit: the launched writer either
	// is waiting for the schema statement's admission, or returned already.
	writerAdmitted := false
	select {
	case <-writer.done:
		require.NoError(t, writer.join(t), "the concurrent data write must commit")
		writerAdmitted = true
	case <-writer.blocked:
	case <-time.After(schemaDDLJoinBudget):
		t.Fatalf("the concurrent data writer neither committed nor waited for admission within %s", schemaDDLJoinBudget)
	}

	require.NoError(t, fill.finish(t), "the index fill must complete")
	require.NoError(t, ddl.StageSchemaChanges())
	require.NoError(t, ddl.Commit(), "an unrelated data writer must not reject the schema commit")
	if !writerAdmitted {
		require.NoError(t, writer.join(t), "the concurrent data write must commit")
	}

	require.True(t, func() bool { _, ok := fixture.schema.GetCompositeIndex("w_v"); return ok }(),
		"the committed statement must publish its index")
	for _, value := range []string{"seed", "peer"} {
		ids, err := fixture.schema.PropertyIndexLookup(ctx, "W", "v", value)
		require.NoError(t, err)
		require.Lenf(t, ids, 1, "the published index must hold the %q node", value)
	}
}

// TestDropIndexCommitSurvivesConcurrentDataWriter is the same schedule for a
// statement that publishes no population at all: a drop staged while an
// unrelated data commit lands must commit.
func TestDropIndexCommitSurvivesConcurrentDataWriter(t *testing.T) {
	ctx := context.Background()
	fixture := newSchemaDDLFixture(t, "drop_data_peer")
	fixture.seed(t, "seed")

	create, createView := fixture.beginStagedDDL(t)
	require.NoError(t, createView.AddCompositeIndex("w_v", "W", []string{"v"}))
	require.NoError(t, createView.BackfillCompositeIndex(ctx, "w_v", fixture.fillEntries(t, "W")))
	require.NoError(t, create.StageSchemaChanges())
	require.NoError(t, create.Commit())

	drop, dropView := fixture.beginStagedDDL(t)
	require.NoError(t, dropView.DropIndex("w_v"))
	require.NoError(t, drop.StageSchemaChanges())

	writer := fixture.launchDataWriter(t, &Node{
		ID:         NodeID(fixture.namespace + ":peer"),
		Labels:     []string{"W"},
		Properties: map[string]interface{}{"v": "peer"},
	})
	writerAdmitted := false
	select {
	case <-writer.done:
		require.NoError(t, writer.join(t), "the concurrent data write must commit")
		writerAdmitted = true
	case <-writer.blocked:
	case <-time.After(schemaDDLJoinBudget):
		t.Fatalf("the concurrent data writer neither committed nor waited for admission within %s", schemaDDLJoinBudget)
	}

	require.NoError(t, drop.Commit(), "an unrelated data writer must not reject the schema commit")
	if !writerAdmitted {
		require.NoError(t, writer.join(t), "the concurrent data write must commit")
	}
	_, published := fixture.schema.GetCompositeIndex("w_v")
	require.False(t, published, "the committed drop must remove its index")
}

// TestSchemaDDLCommitStillRejectsConcurrentSchemaChange preserves the real
// schema race: a peer schema publication inside the statement's window changes
// the definition the statement built on, so its commit is rejected and the
// peer's publication survives. The stale statement is an explicit transaction:
// two autocommit schema statements cannot be open at once (the second waits for
// the first's admission), so opening both synchronously under one schema
// mutation mutex would deadlock instead of scheduling this race.
func TestSchemaDDLCommitStillRejectsConcurrentSchemaChange(t *testing.T) {
	ctx := context.Background()
	fixture := newSchemaDDLFixture(t, "ddl_schema_peer")
	fixture.seed(t, "seed")

	staged, stagedView := fixture.beginExplicitDDL(t)
	require.NoError(t, stagedView.AddCompositeIndex("staged", "W", []string{"v"}))
	require.NoError(t, stagedView.BackfillCompositeIndex(ctx, "staged", fixture.fillEntries(t, "W")))
	require.NoError(t, staged.StageSchemaChanges())

	// The peer publishes its definition while the explicit statement holds its
	// own snapshot: the statement's definition read predates that publication.
	peer, peerView := fixture.beginStagedDDL(t)
	require.NoError(t, peerView.AddCompositeIndex("peer", "W", []string{"v"}))
	require.NoError(t, peerView.BackfillCompositeIndex(ctx, "peer", fixture.fillEntries(t, "W")))
	require.NoError(t, peer.StageSchemaChanges())
	require.NoError(t, peer.Commit())

	require.ErrorIs(t, staged.Commit(), ErrConflict,
		"a concurrent schema publication must still reject the earlier schema snapshot")
	_, stagedPublished := fixture.schema.GetCompositeIndex("staged")
	_, peerPublished := fixture.schema.GetCompositeIndex("peer")
	require.False(t, stagedPublished, "a rejected schema commit must publish nothing")
	require.True(t, peerPublished, "a rejected schema commit must not discard the peer's publication")
}

// TestExplicitDDLCommitStillRejectsConcurrentDataWriter preserves the schema
// snapshot guard for the transaction shape that still spans a data commit: an
// explicit statement validates the population its fill read at commit, so a data
// commit inside its window rejects the schema commit instead of publishing an
// index that omits it. The autocommit statement's exclusive admission is what
// removes that window, not a weaker guard.
func TestExplicitDDLCommitStillRejectsConcurrentDataWriter(t *testing.T) {
	ctx := context.Background()
	fixture := newSchemaDDLFixture(t, "explicit_ddl_data_peer")
	fixture.seed(t, "seed")

	staged, stagedView := fixture.beginExplicitDDL(t)
	require.NoError(t, stagedView.AddCompositeIndex("w_v", "W", []string{"v"}))
	require.NoError(t, stagedView.BackfillCompositeIndex(ctx, "w_v", fixture.fillEntries(t, "W")))
	require.NoError(t, staged.StageSchemaChanges())

	// An explicit transaction takes no lifecycle admission before its snapshot,
	// so this commit lands inside the statement's window.
	fixture.writeThroughEngine(t, "peer", "peer")

	require.ErrorIs(t, staged.Commit(), ErrConflict,
		"a data commit inside an explicit DDL window must still reject the schema snapshot")
	_, published := fixture.schema.GetCompositeIndex("w_v")
	require.False(t, published, "a rejected schema commit must publish nothing")
	require.Equal(t, 2, fixture.storedNodeCount(t, "W"), "the peer's data write must survive the rejected schema commit")
}
