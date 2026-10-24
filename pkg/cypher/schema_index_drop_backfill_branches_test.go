package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type getNodesByLabelErrorEngine struct {
	*storage.MemoryEngine
	errOnLabel string
}

func (e *getNodesByLabelErrorEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	if label == e.errOnLabel {
		return nil, fmt.Errorf("forced label lookup error for %s", label)
	}
	return e.MemoryEngine.GetNodesByLabel(label)
}

// The database wrapper delegates label reads through the scoped methods
// (storage.ScopedLabelNodeReader); without these overrides the embedded
// engine's ones would bypass the fake.
func (e *getNodesByLabelErrorEngine) GetNodesByLabelInScope(_, label string) ([]*storage.Node, error) {
	return e.GetNodesByLabel(label)
}

// Index backfills read stored nodes (storage.StoredEntityReader and its
// scoped form), so the fake fails those label reads too.
func (e *getNodesByLabelErrorEngine) GetStoredNodesByLabel(label string) ([]*storage.Node, error) {
	if label == e.errOnLabel {
		return nil, fmt.Errorf("forced label lookup error for %s", label)
	}
	return e.MemoryEngine.GetStoredNodesByLabel(label)
}

func (e *getNodesByLabelErrorEngine) GetStoredNodesByLabelInScope(_, label string) ([]*storage.Node, error) {
	return e.GetStoredNodesByLabel(label)
}

func TestSchema_CreateIndexBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "schema_index_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	fixtureAdmission1412Context, fixtureAdmission1412 := statementTestContext(ctx, "CREATE INDEX idx_named IF NOT EXISTS FOR (n:IdxNodeA) ON (n.p)")
	_ = fixtureAdmission1412Context
	_, err := exec.executeCreateIndex(fixtureAdmission1412, fixtureAdmission1412Context, "CREATE INDEX idx_named IF NOT EXISTS FOR (n:IdxNodeA) ON (n.p)")
	require.NoError(t, err)

	fixtureAdmission1413Context, fixtureAdmission1413 := statementTestContext(ctx, "CREATE INDEX IF NOT EXISTS FOR (n:IdxNodeB) ON (n.p, n.q)")
	_ = fixtureAdmission1413Context
	_, err = exec.executeCreateIndex(fixtureAdmission1413, fixtureAdmission1413Context, "CREATE INDEX IF NOT EXISTS FOR (n:IdxNodeB) ON (n.p, n.q)")
	require.NoError(t, err)

	fixtureAdmission1414Context, fixtureAdmission1414 := statementTestContext(ctx, "CREATE INDEX idx_legacy ON :IdxNodeC(p)")
	_ = fixtureAdmission1414Context
	_, err = exec.executeCreateIndex(fixtureAdmission1414, fixtureAdmission1414Context, "CREATE INDEX idx_legacy ON :IdxNodeC(p)")
	require.NoError(t, err)

	fixtureAdmission1415Context, fixtureAdmission1415 := statementTestContext(ctx, "CREATE INDEX FOR (n:IdxNodeD) ON n.p")
	_ = fixtureAdmission1415Context
	_, err = exec.executeCreateIndex(fixtureAdmission1415, fixtureAdmission1415Context, "CREATE INDEX FOR (n:IdxNodeD) ON n.p")
	require.NoError(t, err)

	fixtureAdmission1416Context, fixtureAdmission1416 := statementTestContext(ctx, "CREATE INDEX idx_invalid")
	_ = fixtureAdmission1416Context
	_, err = exec.executeCreateIndex(fixtureAdmission1416, fixtureAdmission1416Context, "CREATE INDEX idx_invalid")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid CREATE INDEX syntax")
}

func TestSchema_CreateIndexBranches_BackfillErrorsAcrossSyntaxes(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &getNodesByLabelErrorEngine{MemoryEngine: base, errOnLabel: "ErrBackfill"}
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	fixtureAdmission1417Context, fixtureAdmission1417 := statementTestContext(ctx, "CREATE INDEX idx_named_err FOR (n:ErrBackfill) ON (n.p)")
	_ = fixtureAdmission1417Context
	_, err := exec.executeCreateIndex(fixtureAdmission1417, fixtureAdmission1417Context, "CREATE INDEX idx_named_err FOR (n:ErrBackfill) ON (n.p)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to backfill index for label ErrBackfill")

	fixtureAdmission1418Context, fixtureAdmission1418 := statementTestContext(ctx, "CREATE INDEX FOR (n:ErrBackfill) ON (n.p)")
	_ = fixtureAdmission1418Context
	_, err = exec.executeCreateIndex(fixtureAdmission1418, fixtureAdmission1418Context, "CREATE INDEX FOR (n:ErrBackfill) ON (n.p)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to backfill index for label ErrBackfill")

	fixtureAdmission1419Context, fixtureAdmission1419 := statementTestContext(ctx, "CREATE INDEX idx_legacy_err ON :ErrBackfill(p)")
	_ = fixtureAdmission1419Context
	_, err = exec.executeCreateIndex(fixtureAdmission1419, fixtureAdmission1419Context, "CREATE INDEX idx_legacy_err ON :ErrBackfill(p)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to backfill index for label ErrBackfill")

	fixtureAdmission1420Context, fixtureAdmission1420 := statementTestContext(ctx, "CREATE INDEX FOR (n:ErrBackfill) ON n.p")
	_ = fixtureAdmission1420Context
	_, err = exec.executeCreateIndex(fixtureAdmission1420, fixtureAdmission1420Context, "CREATE INDEX FOR (n:ErrBackfill) ON n.p")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to backfill index for label ErrBackfill")
}

func TestSchema_DropIndexAndDropConstraintBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "schema_drop_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	fixtureAdmission1421Context, fixtureAdmission1421 := statementTestContext(ctx, "DROP INDEX")
	_ = fixtureAdmission1421Context
	_, err := exec.executeDropIndex(fixtureAdmission1421, fixtureAdmission1421Context, "DROP INDEX")
	require.Error(t, err)
	require.Contains(t, err.Error(), "index name required")

	fixtureAdmission1422Context, fixtureAdmission1422 := statementTestContext(ctx, "DROP CONSTRAINT")
	_ = fixtureAdmission1422Context
	_, err = exec.executeDropConstraint(fixtureAdmission1422, fixtureAdmission1422Context, "DROP CONSTRAINT")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid DROP CONSTRAINT syntax")

	fixtureAdmission1423Context, fixtureAdmission1423 := statementTestContext(ctx, "DROP CONSTRAINT a b")
	_ = fixtureAdmission1423Context
	_, err = exec.executeDropConstraint(fixtureAdmission1423, fixtureAdmission1423Context, "DROP CONSTRAINT a b")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid DROP CONSTRAINT syntax")

	fixtureAdmission1424Context, fixtureAdmission1424 := statementTestContext(ctx, "DROP CONSTRAINT IF EXISTS missing_c")
	_ = fixtureAdmission1424Context
	_, err = exec.executeDropConstraint(fixtureAdmission1424, fixtureAdmission1424Context, "DROP CONSTRAINT IF EXISTS missing_c")
	require.NoError(t, err)

	fixtureAdmission1425Context, fixtureAdmission1425 := statementTestContext(ctx, "CREATE INDEX `idx with space` FOR (n:DropNode) ON (n.p)")
	_ = fixtureAdmission1425Context
	_, err = exec.executeCreateIndex(fixtureAdmission1425, fixtureAdmission1425Context, "CREATE INDEX `idx with space` FOR (n:DropNode) ON (n.p)")
	require.NoError(t, err)

	fixtureAdmission1426Context, fixtureAdmission1426 := statementTestContext(ctx, "DROP INDEX `idx with space`")
	_ = fixtureAdmission1426Context
	_, err = exec.executeDropIndex(fixtureAdmission1426, fixtureAdmission1426Context, "DROP INDEX `idx with space`")
	require.NoError(t, err)

	fixtureAdmission1427Context, fixtureAdmission1427 := statementTestContext(ctx, "DROP INDEX missing_idx IF EXISTS")
	_ = fixtureAdmission1427Context
	_, err = exec.executeDropIndex(fixtureAdmission1427, fixtureAdmission1427Context, "DROP INDEX missing_idx IF EXISTS")
	require.NoError(t, err)

	// Vector index drop path should execute special cleanup branch.
	fixtureAdmission1428Context, fixtureAdmission1428 := statementTestContext(ctx, "CREATE VECTOR INDEX vec_drop FOR (n:VecNode) ON (n.embedding)")
	_ = fixtureAdmission1428Context
	_, err = exec.executeCreateVectorIndex(fixtureAdmission1428, fixtureAdmission1428Context, "CREATE VECTOR INDEX vec_drop FOR (n:VecNode) ON (n.embedding)")
	require.NoError(t, err)
	fixtureAdmission1429Context, fixtureAdmission1429 := statementTestContext(ctx, "DROP INDEX vec_drop")
	_ = fixtureAdmission1429Context
	_, err = exec.executeDropIndex(fixtureAdmission1429, fixtureAdmission1429Context, "DROP INDEX vec_drop")
	require.NoError(t, err)
}

func TestSchema_BackfillCompositeIndex_ErrorBranches(t *testing.T) {
	ctx := context.Background()

	// GetNodesByLabel error branch.
	baseErr := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = baseErr.Close() })
	errEng := &getNodesByLabelErrorEngine{MemoryEngine: baseErr, errOnLabel: "ErrLabel"}
	errStore := storage.NewNamespacedEngine(errEng, "schema_backfill_err")
	errExec := NewStorageExecutor(errStore)

	_, err := errStore.CreateNode(&storage.Node{ID: storage.NodeID("e1"), Labels: []string{"ErrLabel"}, Properties: map[string]interface{}{"p": "v"}})
	require.NoError(t, err)
	require.NoError(t, errStore.GetSchema().AddPropertyIndex("idx_err_label", "ErrLabel", []string{"p"}))

	err = errExec.backfillCompositeIndex("idx_err_label", "ErrLabel", []string{"p"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to backfill index for label ErrLabel")

	// Index missing for the target: BackfillCompositeIndex reports not found.
	baseMissing := newTestMemoryEngine(t)
	missingStore := storage.NewNamespacedEngine(baseMissing, "schema_backfill_missing_idx")
	missingExec := NewStorageExecutor(missingStore)

	_, err = missingStore.CreateNode(&storage.Node{ID: storage.NodeID("m1"), Labels: []string{"Doc"}, Properties: map[string]interface{}{"p": "v"}})
	require.NoError(t, err)
	err = missingExec.backfillCompositeIndex("missing_idx", "Doc", []string{"p"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to backfill property index")

	// Keep ctx used to avoid lints in future edits.
	require.NotNil(t, ctx)
}
