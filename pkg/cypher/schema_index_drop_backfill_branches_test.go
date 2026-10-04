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

	_, err := exec.executeCreateIndex(ctx, "CREATE INDEX idx_named IF NOT EXISTS FOR (n:IdxNodeA) ON (n.p)")
	require.NoError(t, err)

	_, err = exec.executeCreateIndex(ctx, "CREATE INDEX IF NOT EXISTS FOR (n:IdxNodeB) ON (n.p, n.q)")
	require.NoError(t, err)

	_, err = exec.executeCreateIndex(ctx, "CREATE INDEX idx_legacy ON :IdxNodeC(p)")
	require.NoError(t, err)

	_, err = exec.executeCreateIndex(ctx, "CREATE INDEX FOR (n:IdxNodeD) ON n.p")
	require.NoError(t, err)

	_, err = exec.executeCreateIndex(ctx, "CREATE INDEX idx_invalid")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid CREATE INDEX syntax")
}

func TestSchema_CreateIndexBranches_BackfillErrorsAcrossSyntaxes(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &getNodesByLabelErrorEngine{MemoryEngine: base, errOnLabel: "ErrBackfill"}
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := exec.executeCreateIndex(ctx, "CREATE INDEX idx_named_err FOR (n:ErrBackfill) ON (n.p)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to backfill index for label ErrBackfill")

	_, err = exec.executeCreateIndex(ctx, "CREATE INDEX FOR (n:ErrBackfill) ON (n.p)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to backfill index for label ErrBackfill")

	_, err = exec.executeCreateIndex(ctx, "CREATE INDEX idx_legacy_err ON :ErrBackfill(p)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to backfill index for label ErrBackfill")

	_, err = exec.executeCreateIndex(ctx, "CREATE INDEX FOR (n:ErrBackfill) ON n.p")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to backfill index for label ErrBackfill")
}

func TestSchema_DropIndexAndDropConstraintBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "schema_drop_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.executeDropIndex(ctx, "DROP INDEX")
	require.Error(t, err)
	require.Contains(t, err.Error(), "index name required")

	_, err = exec.executeDropConstraint(ctx, "DROP CONSTRAINT")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid DROP CONSTRAINT syntax")

	_, err = exec.executeDropConstraint(ctx, "DROP CONSTRAINT a b")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid DROP CONSTRAINT syntax")

	_, err = exec.executeDropConstraint(ctx, "DROP CONSTRAINT IF EXISTS missing_c")
	require.NoError(t, err)

	_, err = exec.executeCreateIndex(ctx, "CREATE INDEX `idx with space` FOR (n:DropNode) ON (n.p)")
	require.NoError(t, err)

	_, err = exec.executeDropIndex(ctx, "DROP INDEX `idx with space`")
	require.NoError(t, err)

	_, err = exec.executeDropIndex(ctx, "DROP INDEX missing_idx IF EXISTS")
	require.NoError(t, err)

	// Vector index drop path should execute special cleanup branch.
	_, err = exec.executeCreateVectorIndex(ctx, "CREATE VECTOR INDEX vec_drop FOR (n:VecNode) ON (n.embedding)")
	require.NoError(t, err)
	_, err = exec.executeDropIndex(ctx, "DROP INDEX vec_drop")
	require.NoError(t, err)
}

func TestSchema_BackfillPropertyIndex_ErrorBranches(t *testing.T) {
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

	err = errExec.backfillPropertyIndex("ErrLabel", []string{"p"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to backfill index for label ErrLabel")

	// PropertyIndexInsert error branch (index missing for target property).
	baseMissing := newTestMemoryEngine(t)
	missingStore := storage.NewNamespacedEngine(baseMissing, "schema_backfill_missing_idx")
	missingExec := NewStorageExecutor(missingStore)

	_, err = missingStore.CreateNode(&storage.Node{ID: storage.NodeID("m1"), Labels: []string{"Doc"}, Properties: map[string]interface{}{"p": "v"}})
	require.NoError(t, err)
	err = missingExec.backfillPropertyIndex("Doc", []string{"p"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to backfill property index")

	// len(properties) != 1 short-circuit branch.
	err = missingExec.backfillPropertyIndex("Doc", []string{"p", "q"})
	require.NoError(t, err)

	// Keep ctx used to avoid lints in future edits.
	require.NotNil(t, ctx)
}
