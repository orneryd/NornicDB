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

func TestSchema_CreateIndexBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "schema_index_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	fixtureAdmission1400Context, fixtureAdmission1400 := statementTestContext(ctx, "CREATE INDEX idx_named IF NOT EXISTS FOR (n:IdxNodeA) ON (n.p)")
	_ = fixtureAdmission1400Context
	_, err := exec.executeCreateIndex(fixtureAdmission1400, fixtureAdmission1400Context, "CREATE INDEX idx_named IF NOT EXISTS FOR (n:IdxNodeA) ON (n.p)")
	require.NoError(t, err)

	fixtureAdmission1401Context, fixtureAdmission1401 := statementTestContext(ctx, "CREATE INDEX IF NOT EXISTS FOR (n:IdxNodeB) ON (n.p, n.q)")
	_ = fixtureAdmission1401Context
	_, err = exec.executeCreateIndex(fixtureAdmission1401, fixtureAdmission1401Context, "CREATE INDEX IF NOT EXISTS FOR (n:IdxNodeB) ON (n.p, n.q)")
	require.NoError(t, err)

	fixtureAdmission1402Context, fixtureAdmission1402 := statementTestContext(ctx, "CREATE INDEX idx_legacy ON :IdxNodeC(p)")
	_ = fixtureAdmission1402Context
	_, err = exec.executeCreateIndex(fixtureAdmission1402, fixtureAdmission1402Context, "CREATE INDEX idx_legacy ON :IdxNodeC(p)")
	require.NoError(t, err)

	fixtureAdmission1403Context, fixtureAdmission1403 := statementTestContext(ctx, "CREATE INDEX FOR (n:IdxNodeD) ON n.p")
	_ = fixtureAdmission1403Context
	_, err = exec.executeCreateIndex(fixtureAdmission1403, fixtureAdmission1403Context, "CREATE INDEX FOR (n:IdxNodeD) ON n.p")
	require.NoError(t, err)

	fixtureAdmission1404Context, fixtureAdmission1404 := statementTestContext(ctx, "CREATE INDEX idx_invalid")
	_ = fixtureAdmission1404Context
	_, err = exec.executeCreateIndex(fixtureAdmission1404, fixtureAdmission1404Context, "CREATE INDEX idx_invalid")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid CREATE INDEX syntax")
}

func TestSchema_CreateIndexBranches_BackfillErrorsAcrossSyntaxes(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &getNodesByLabelErrorEngine{MemoryEngine: base, errOnLabel: "ErrBackfill"}
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	fixtureAdmission1405Context, fixtureAdmission1405 := statementTestContext(ctx, "CREATE INDEX idx_named_err FOR (n:ErrBackfill) ON (n.p)")
	_ = fixtureAdmission1405Context
	_, err := exec.executeCreateIndex(fixtureAdmission1405, fixtureAdmission1405Context, "CREATE INDEX idx_named_err FOR (n:ErrBackfill) ON (n.p)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to backfill index for label ErrBackfill")

	fixtureAdmission1406Context, fixtureAdmission1406 := statementTestContext(ctx, "CREATE INDEX FOR (n:ErrBackfill) ON (n.p)")
	_ = fixtureAdmission1406Context
	_, err = exec.executeCreateIndex(fixtureAdmission1406, fixtureAdmission1406Context, "CREATE INDEX FOR (n:ErrBackfill) ON (n.p)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to backfill index for label ErrBackfill")

	fixtureAdmission1407Context, fixtureAdmission1407 := statementTestContext(ctx, "CREATE INDEX idx_legacy_err ON :ErrBackfill(p)")
	_ = fixtureAdmission1407Context
	_, err = exec.executeCreateIndex(fixtureAdmission1407, fixtureAdmission1407Context, "CREATE INDEX idx_legacy_err ON :ErrBackfill(p)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to backfill index for label ErrBackfill")

	fixtureAdmission1408Context, fixtureAdmission1408 := statementTestContext(ctx, "CREATE INDEX FOR (n:ErrBackfill) ON n.p")
	_ = fixtureAdmission1408Context
	_, err = exec.executeCreateIndex(fixtureAdmission1408, fixtureAdmission1408Context, "CREATE INDEX FOR (n:ErrBackfill) ON n.p")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to backfill index for label ErrBackfill")
}

func TestSchema_DropIndexAndDropConstraintBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "schema_drop_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	fixtureAdmission1409Context, fixtureAdmission1409 := statementTestContext(ctx, "DROP INDEX")
	_ = fixtureAdmission1409Context
	_, err := exec.executeDropIndex(fixtureAdmission1409, fixtureAdmission1409Context, "DROP INDEX")
	require.Error(t, err)
	require.Contains(t, err.Error(), "index name required")

	fixtureAdmission1410Context, fixtureAdmission1410 := statementTestContext(ctx, "DROP CONSTRAINT")
	_ = fixtureAdmission1410Context
	_, err = exec.executeDropConstraint(fixtureAdmission1410, fixtureAdmission1410Context, "DROP CONSTRAINT")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid DROP CONSTRAINT syntax")

	fixtureAdmission1411Context, fixtureAdmission1411 := statementTestContext(ctx, "DROP CONSTRAINT a b")
	_ = fixtureAdmission1411Context
	_, err = exec.executeDropConstraint(fixtureAdmission1411, fixtureAdmission1411Context, "DROP CONSTRAINT a b")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid DROP CONSTRAINT syntax")

	fixtureAdmission1412Context, fixtureAdmission1412 := statementTestContext(ctx, "DROP CONSTRAINT IF EXISTS missing_c")
	_ = fixtureAdmission1412Context
	_, err = exec.executeDropConstraint(fixtureAdmission1412, fixtureAdmission1412Context, "DROP CONSTRAINT IF EXISTS missing_c")
	require.NoError(t, err)

	fixtureAdmission1413Context, fixtureAdmission1413 := statementTestContext(ctx, "CREATE INDEX `idx with space` FOR (n:DropNode) ON (n.p)")
	_ = fixtureAdmission1413Context
	_, err = exec.executeCreateIndex(fixtureAdmission1413, fixtureAdmission1413Context, "CREATE INDEX `idx with space` FOR (n:DropNode) ON (n.p)")
	require.NoError(t, err)

	fixtureAdmission1414Context, fixtureAdmission1414 := statementTestContext(ctx, "DROP INDEX `idx with space`")
	_ = fixtureAdmission1414Context
	_, err = exec.executeDropIndex(fixtureAdmission1414, fixtureAdmission1414Context, "DROP INDEX `idx with space`")
	require.NoError(t, err)

	fixtureAdmission1415Context, fixtureAdmission1415 := statementTestContext(ctx, "DROP INDEX missing_idx IF EXISTS")
	_ = fixtureAdmission1415Context
	_, err = exec.executeDropIndex(fixtureAdmission1415, fixtureAdmission1415Context, "DROP INDEX missing_idx IF EXISTS")
	require.NoError(t, err)

	// Vector index drop path should execute special cleanup branch.
	fixtureAdmission1416Context, fixtureAdmission1416 := statementTestContext(ctx, "CREATE VECTOR INDEX vec_drop FOR (n:VecNode) ON (n.embedding)")
	_ = fixtureAdmission1416Context
	_, err = exec.executeCreateVectorIndex(fixtureAdmission1416, fixtureAdmission1416Context, "CREATE VECTOR INDEX vec_drop FOR (n:VecNode) ON (n.embedding)")
	require.NoError(t, err)
	fixtureAdmission1417Context, fixtureAdmission1417 := statementTestContext(ctx, "DROP INDEX vec_drop")
	_ = fixtureAdmission1417Context
	_, err = exec.executeDropIndex(fixtureAdmission1417, fixtureAdmission1417Context, "DROP INDEX vec_drop")
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
