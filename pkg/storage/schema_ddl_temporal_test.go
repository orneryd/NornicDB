package storage

// A temporal constraint created over nodes that already exist publishes the
// history and current-version entries its index needs in the same transaction
// as the constraint itself.
//
// The statement's begin already holds the engine's exclusive lifecycle
// admission from before its snapshot (AutocommitSchemaBegin), so the pass that
// derives those entries must not take a write of the engine's own: it stages
// them in the statement's transaction (MarkNewTemporalConstraintsForEngine ->
// stageTemporalBackfillForLabel), and its reads are that transaction's own
// snapshot. These controls pin that mechanism at its own seam: nothing reaches
// storage before the commit, the reviewed completeness mark survives
// publication, a rollback discards the entries with the constraint, a rollback
// during the pass stages nothing after it, and a view whose report does not own
// its transaction is refused instead of trusted.
//
// The public transport route is the companion test in
// schema_ddl_temporal_public_test.go.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// schemaStatementView is a schema statement's storage view over its own
// transaction, as the Cypher executor's transaction wrapper is: the native
// transaction capability the storage owner stages through, the engine and
// database the view serves, and the stored reads of that transaction. The
// engine it serves answers the view's other methods, as the transaction it
// staged in does not offer the whole Engine surface.
type schemaStatementView struct {
	Engine
	tx       *BadgerTransaction
	database string
}

func newSchemaStatementView(tx *BadgerTransaction, served Engine, database string) *schemaStatementView {
	return &schemaStatementView{Engine: served, tx: tx, database: database}
}

func (v *schemaStatementView) NativeSchemaTransaction() *BadgerTransaction { return v.tx }
func (v *schemaStatementView) Namespace() string                           { return v.database }
func (v *schemaStatementView) GetInnerEngine() Engine                      { return v.Engine }
func (v *schemaStatementView) GetSchema() *SchemaManager                   { return v.tx.knowledgeSchema }

func (v *schemaStatementView) GetStoredNode(ctx context.Context, id NodeID) (*Node, error) {
	return v.tx.GetStoredNode(ctx, id)
}

func (v *schemaStatementView) GetStoredEdge(ctx context.Context, id EdgeID) (*Edge, error) {
	return v.tx.GetStoredEdge(ctx, id)
}

func (v *schemaStatementView) ReadStoredAdjacentEdges(ctx context.Context, request AdjacentEdgeRead) ([]*Edge, error) {
	return v.tx.ReadStoredAdjacentEdges(ctx, request)
}

func (v *schemaStatementView) GetStoredNodesByLabel(ctx context.Context, label string) ([]*Node, error) {
	return v.tx.GetStoredNodesByLabel(ctx, label)
}

func (v *schemaStatementView) StreamStoredNodesByLabelProjected(ctx context.Context, label string, properties []string, visit func(*Node) error) error {
	return v.tx.StreamStoredNodesByLabelProjected(ctx, label, properties, visit)
}

func (v *schemaStatementView) StreamStoredEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error {
	return v.tx.StreamStoredEdgesByType(ctx, edgeType, visit)
}

// currentFact is a stored "Fact" node whose validity window covers now, so the
// backfill records it as the key's current version as well as in its history.
func currentFact(id string, record interface{}, from, to time.Duration) *Node {
	now := time.Now().UTC()
	return &Node{ID: NodeID(prefixTestID(id)), Labels: []string{"Fact"}, Properties: map[string]interface{}{
		"record_id":  record,
		"valid_from": now.Add(from),
		"valid_to":   now.Add(to),
	}}
}

// beginTemporalDDLStatement opens the statement transaction a temporal
// constraint's DDL runs in and its storage view, with the "Fact" node already
// stored before the constraint exists.
func beginTemporalDDLStatement(t *testing.T, engine *BadgerEngine) (*BadgerTransaction, *schemaStatementView) {
	t.Helper()
	sm := engine.GetSchemaForNamespace("test")
	tx, err := sm.BeginSchemaTransaction(t.Context(), engine.BeginTransaction)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	// The statement's isolated schema view, as the Cypher executor opens it
	// before it applies the change.
	_, err = tx.Schema(OrdinarySchemaChanges)
	require.NoError(t, err)
	view := newSchemaStatementView(tx, NewNamespacedEngine(engine, "test"), "test")
	return tx, view
}

// stagedTemporalKeys reports how many keys the transaction has staged and not
// written yet.
func stagedTemporalKeys(t *testing.T, tx *BadgerTransaction) int {
	t.Helper()
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return len(tx.pendingWrites)
}

// storedSchemaDefinition reports whether the namespace's schema definition is
// stored, without reading through any transaction of the test's own.
func storedSchemaDefinition(t *testing.T, engine *BadgerEngine, namespace string) bool {
	t.Helper()
	stored := true
	err := engine.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(schemaKey(namespace))
		if errors.Is(err, badger.ErrKeyNotFound) {
			stored = false
			return nil
		}
		return err
	})
	require.NoError(t, err)
	return stored
}

// TestTemporalConstraintDDLStagesItsBackfillInTheStatementTransaction: the
// entries of a node that existed before the constraint are staged in the
// statement's transaction, reach storage only with its commit, and leave the
// published history index trusted.
func TestTemporalConstraintDDLStagesItsBackfillInTheStatementTransaction(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	ctx := context.Background()
	commitNodes(t, engine, currentFact("pre", int64(1), -2*time.Hour, 2*time.Hour))
	require.NoError(t, ValidateConstraintOnCreationForEngine(ctx, engine, factWindow))

	tx, view := beginTemporalDDLStatement(t, engine)
	require.NoError(t, view.GetSchema().AddConstraint(factWindow))
	require.NoError(t, MarkNewTemporalConstraintsForEngine(view, view.GetSchema(), []Constraint{factWindow}))

	require.NotZero(t, stagedTemporalKeys(t, tx), "the pass stages its entries in the statement's transaction")
	require.Zero(t, countKeysWithPrefix(t, engine, prefixTemporalIndex),
		"a statement's backfill writes no entry of the engine's own before its commit")
	require.Zero(t, countKeysWithPrefix(t, engine, prefixTemporalHead))

	require.NoError(t, tx.StageSchemaChanges())
	require.NoError(t, tx.Commit())

	require.Equal(t, 1, countKeysWithPrefix(t, engine, prefixTemporalIndex), "the pre-existing node's history entry")
	require.Equal(t, 1, countKeysWithPrefix(t, engine, prefixTemporalHead), "the pre-existing node's current version")
	require.True(t, engine.temporalIndexAuthoritative(engine.GetSchemaForNamespace("test"), factWindow),
		"the statement's reviewed completeness mark survives publication")

	scans := countTemporalScans(t)
	overlap, err := engine.BeginTransaction(ctx, OrdinaryBegin)
	require.NoError(t, err)
	_, err = overlap.CreateNode(ctx, currentFact("overlap", int64(1), -time.Hour, time.Hour))
	require.Error(t, err, "an interval overlapping one written before the constraint must be rejected")
	require.NoError(t, overlap.Rollback())
	require.Zero(t, scans.Load(), "the published history index answers without scanning the label")
}

// TestTemporalConstraintDDLRollbackLeavesNoDerivedKeysOrConstraint: the entries
// and the constraint are one transaction's writes, so a rollback leaves
// neither, and a pass that outlives its transaction stages nothing.
func TestTemporalConstraintDDLRollbackLeavesNoDerivedKeysOrConstraint(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	commitNodes(t, engine, currentFact("pre", int64(1), -2*time.Hour, 2*time.Hour))

	tx, view := beginTemporalDDLStatement(t, engine)
	require.NoError(t, view.GetSchema().AddConstraint(factWindow))
	require.NoError(t, MarkNewTemporalConstraintsForEngine(view, view.GetSchema(), []Constraint{factWindow}))
	require.NotZero(t, stagedTemporalKeys(t, tx))
	require.NoError(t, tx.Rollback())

	require.Zero(t, countKeysWithPrefix(t, engine, prefixTemporalIndex), "a rolled-back statement publishes no history entry")
	require.Zero(t, countKeysWithPrefix(t, engine, prefixTemporalHead), "a rolled-back statement publishes no current version")
	require.False(t, storedSchemaDefinition(t, engine, "test"), "a rolled-back statement publishes no definition")
	require.False(t, engine.GetSchemaForNamespace("test").HasConstraintNamed(factWindow.Name),
		"a rolled-back statement leaves no registered constraint")
	require.ErrorIs(t, tx.stageTemporalRebuildKey([]byte{prefixTemporalIndex}, []byte("test:pre")), ErrTransactionClosed,
		"staging after the rollback is refused instead of buffered")
}

// rollbackDuringBackfill reads the pass's population through the transaction,
// rolls the statement back, and only then lets the pass walk the population it
// read: every operation the pass performs from there on must be refused.
type rollbackDuringBackfill struct {
	*schemaStatementView
	t          *testing.T
	rolledBack bool
}

func (v *rollbackDuringBackfill) StreamStoredNodesByLabelProjected(ctx context.Context, label string, properties []string, visit func(*Node) error) error {
	var population []*Node
	if err := v.tx.StreamStoredNodesByLabelProjected(ctx, label, properties, func(node *Node) error {
		population = append(population, node)
		return nil
	}); err != nil {
		return err
	}
	v.rolledBack = true
	if err := v.tx.Rollback(); err != nil {
		return err
	}
	for _, node := range population {
		if err := visit(node); err != nil {
			return err
		}
	}
	return nil
}

// TestTemporalConstraintDDLRollbackDuringBackfillStagesNothingMore: every
// staged key re-checks the transaction's own operation admission, so a
// statement that rolls back while its backfill is still running cannot stage
// another entry.
func TestTemporalConstraintDDLRollbackDuringBackfillStagesNothingMore(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	commitNodes(t, engine, currentFact("pre", int64(1), -2*time.Hour, 2*time.Hour))

	tx, view := beginTemporalDDLStatement(t, engine)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, view.GetSchema().AddConstraint(factWindow))
	during := &rollbackDuringBackfill{schemaStatementView: view, t: t}

	err := MarkNewTemporalConstraintsForEngine(during, view.GetSchema(), []Constraint{factWindow})
	require.True(t, during.rolledBack, "the pass read the stored node before the rollback")
	require.ErrorIs(t, err, ErrTransactionClosed, "the pass's own operations after the rollback are refused")
	require.Zero(t, countKeysWithPrefix(t, engine, prefixTemporalIndex))
	require.Zero(t, countKeysWithPrefix(t, engine, prefixTemporalHead))
	require.False(t, engine.GetSchemaForNamespace("test").HasConstraintNamed(factWindow.Name))
}

// TestTemporalConstraintBackfillRejectsForeignSchemaTransactionView: a view
// whose reported transaction does not own it is refused. Staging there would
// publish entries the statement never derived, in a transaction it does not own.
func TestTemporalConstraintBackfillRejectsForeignSchemaTransactionView(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	commitNodes(t, engine, currentFact("pre", int64(1), -2*time.Hour, 2*time.Hour))
	foreign := newTestMemoryEngine(t)

	t.Run("another engine's transaction", func(t *testing.T) {
		tx, err := foreign.BeginTransaction(ctx, OrdinaryBegin)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		view := newSchemaStatementView(tx, NewNamespacedEngine(engine, "test"), "test")
		require.ErrorIs(t, MarkNewTemporalConstraintsForEngine(view, engine.GetSchemaForNamespace("test"), []Constraint{factWindow}), ErrInvalidData)
		require.Zero(t, stagedTemporalKeys(t, tx))
		require.Zero(t, countKeysWithPrefix(t, engine, prefixTemporalIndex))
	})

	t.Run("another database of the same engine", func(t *testing.T) {
		tx, err := engine.GetSchemaForNamespace("other").BeginSchemaTransaction(ctx, engine.BeginTransaction)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		require.NoError(t, tx.SetNamespace("other"))
		// The transaction's own isolated view is the "other" database's:
		// the view below serves "test" over it.
		_, err = tx.Schema(OrdinarySchemaChanges)
		require.NoError(t, err)
		view := newSchemaStatementView(tx, NewNamespacedEngine(engine, "test"), "test")
		require.ErrorIs(t, MarkNewTemporalConstraintsForEngine(view, view.GetSchema(), []Constraint{factWindow}), ErrInvalidData)
		require.Zero(t, stagedTemporalKeys(t, tx))
		require.Zero(t, countKeysWithPrefix(t, engine, prefixTemporalIndex))
	})

	t.Run("the live schema instead of the statement's view", func(t *testing.T) {
		tx, view := beginTemporalDDLStatement(t, engine)
		t.Cleanup(func() { _ = tx.Rollback() })
		require.ErrorIs(t, MarkNewTemporalConstraintsForEngine(view, engine.GetSchemaForNamespace("test"), []Constraint{factWindow}), ErrInvalidData)
		require.Zero(t, stagedTemporalKeys(t, tx))
		require.Zero(t, countKeysWithPrefix(t, engine, prefixTemporalIndex))
	})
}
