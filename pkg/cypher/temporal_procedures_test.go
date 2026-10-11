package cypher

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTemporalAssertNoOverlap(t *testing.T) {
	base := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	_, err := engine.CreateNode(&storage.Node{
		ID:     "v1",
		Labels: []string{"FactVersion"},
		Properties: map[string]interface{}{
			"fact_key":   "k1",
			"valid_from": start,
			"valid_to":   end,
		},
	})
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('FactVersion','fact_key','valid_from','valid_to','k1','2024-01-15','2024-02-15')", nil)
	require.Error(t, err)

	result, err := exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('FactVersion','fact_key','valid_from','valid_to','k1','2024-02-01','2024-03-01')", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, true, result.Rows[0][0])
}

// The procedures take evaluated arguments: a parameter, an expression and
// Unix seconds are read like literals; a name that isn't a string is the
// signature's type mismatch, and a missing argument an argument count error.
func TestTemporalProcedures_Arguments(t *testing.T) {
	base := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	_, err := engine.CreateNode(&storage.Node{
		ID:     "num-time",
		Labels: []string{"Fv"},
		Properties: map[string]interface{}{
			"k":    int64(12),
			"from": int64(1699999999),
			"to":   nil,
		},
	})
	require.NoError(t, err)
	for _, query := range []string{
		"CALL db.temporal.asOf('Fv', 'k', 12, 'from', 'to', 1700000000) YIELD node RETURN node",
		"CALL db.temporal.asOf($label, 'k', 6 * 2, 'from', 'to', $at) YIELD node RETURN node",
		"WITH 'Fv' AS label CALL db.temporal.asOf(label, 'k', 12, 'from', 'to', datetime('2023-11-14T22:13:20Z')) YIELD node RETURN node",
	} {
		result, err := exec.Execute(ctx, query, map[string]interface{}{"label": "Fv", "at": int64(1700000000)})
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 1, query)
	}
	result, err := exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('Fv', 'k', 'from', 'to', 12, 1600000000, 1600000001) YIELD ok RETURN ok", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{true}}, result.Rows)
	_, err = exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('Fv', 'k', 'from', 'to', 12, 1700000000, null) YIELD ok RETURN ok", nil)
	require.Error(t, err)

	_, err = exec.Execute(ctx, "CALL db.temporal.asOf(123, 'k', 12, 'from', 'to', 1700000000) YIELD node RETURN node", nil)
	require.Error(t, err)
	_, err = exec.Execute(ctx, "CALL db.temporal.asOf('Fv', 'k', 12, 'from', 'to') YIELD node RETURN node", nil)
	require.Error(t, err)
	_, err = exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('Fv', 'k', 'from', 'to', 12, 1600000000) YIELD ok RETURN ok", nil)
	require.Error(t, err)

	// A node with another key value isn't compared.
	result, err = exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('Fv', 'k', 'from', 'to', 13, 1700000000, null) YIELD ok RETURN ok", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{true}}, result.Rows)
	// An empty name, a systemTime that isn't a time and a systemSequence that
	// isn't a non-negative integer are each the procedure's error.
	for _, query := range []string{
		"CALL db.temporal.assertNoOverlap('Fv', '', 'from', 'to', 12, 1, 2) YIELD ok RETURN ok",
		"CALL db.temporal.assertNoOverlap('Fv', 'k', '', 'to', 12, 1, 2) YIELD ok RETURN ok",
		"CALL db.temporal.assertNoOverlap('Fv', 'k', 'from', '', 12, 1, 2) YIELD ok RETURN ok",
		"CALL db.temporal.assertNoOverlap('Fv', 'k', 'from', 'to', 12, 1, 2, 'not a time') YIELD ok RETURN ok",
		"CALL db.temporal.asOf('Fv', '', 12, 'from', 'to', 1700000000) YIELD node RETURN node",
		"CALL db.temporal.asOf('Fv', 'k', 12, '', 'to', 1700000000) YIELD node RETURN node",
		"CALL db.temporal.asOf('Fv', 'k', 12, 'from', '', 1700000000) YIELD node RETURN node",
		"CALL db.temporal.asOf('Fv', 'k', 12, 'from', 'to', 1700000000, 'not a time') YIELD node RETURN node",
		"CALL db.temporal.asOf('Fv', 'k', 12, 'from', 'to', 1700000000, 1700000001, -1) YIELD node RETURN node",
		"CALL db.temporal.asOf('Fv', 'k', 12, 'from', 'to', 1700000000, 1700000001, 'x') YIELD node RETURN node",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
	}
}

func TestTemporalAsOf(t *testing.T) {
	base := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	v1Start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	v1End := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	_, err := engine.CreateNode(&storage.Node{
		ID:     "v1",
		Labels: []string{"FactVersion"},
		Properties: map[string]interface{}{
			"fact_key":   "k1",
			"valid_from": v1Start,
			"valid_to":   v1End,
		},
	})
	require.NoError(t, err)

	v2Start := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	_, err = engine.CreateNode(&storage.Node{
		ID:     "v2",
		Labels: []string{"FactVersion"},
		Properties: map[string]interface{}{
			"fact_key":   "k1",
			"valid_from": v2Start,
			"valid_to":   nil,
		},
	})
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "CALL db.temporal.asOf('FactVersion','fact_key','k1','valid_from','valid_to','2024-01-15') YIELD node", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 1)

	switch node := result.Rows[0][0].(type) {
	case *storage.Node:
		require.Equal(t, storage.NodeID("v1"), node.ID)
	case storage.Node:
		require.Equal(t, storage.NodeID("v1"), node.ID)
	default:
		t.Fatalf("unexpected node type: %T", node)
	}
}

func TestTemporalAsOf_WithSnapshotVersion(t *testing.T) {
	// temporal.asOf with a snapshot version is a multi-version feature;
	// exercise it against the MVCC-retention variant.
	base := storage.NewMemoryEngineWithMVCCHistory()
	t.Cleanup(func() { _ = base.Close() })
	engine := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	validFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	validTo := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	_, err := engine.CreateNode(&storage.Node{
		ID:     "snap-v1",
		Labels: []string{"FactVersion"},
		Properties: map[string]interface{}{
			"fact_key":   "k1",
			"valid_from": validFrom,
			"valid_to":   validTo,
		},
	})
	require.NoError(t, err)
	head, err := engine.GetNodeCurrentHead("snap-v1")
	require.NoError(t, err)

	require.NoError(t, engine.DeleteNode("snap-v1"))

	result, err := exec.Execute(ctx, "CALL db.temporal.asOf('FactVersion','fact_key','k1','valid_from','valid_to','2024-01-15T00:00:00Z') YIELD node", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)

	query := fmt.Sprintf(
		"CALL db.temporal.asOf('FactVersion','fact_key','k1','valid_from','valid_to','2024-01-15T00:00:00Z','%s',%d) YIELD node",
		head.Version.CommitTimestamp.Format(time.RFC3339Nano),
		head.Version.CommitSequence,
	)
	result, err = exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	node, ok := result.Rows[0][0].(*storage.Node)
	require.True(t, ok)
	require.Equal(t, storage.NodeID("snap-v1"), node.ID)
}

func TestTemporalProcedures_ErrorAndSelectionBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Bad argument count/shape branches.
	_, err := exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('FactVersion')", nil)
	require.Error(t, err)
	_, err = exec.Execute(ctx, "CALL db.temporal.asOf('FactVersion')", nil)
	require.Error(t, err)

	// Invalid datetime coercion branches.
	_, err = exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('FactVersion','fact_key','valid_from','valid_to','k1','not-a-datetime',null)", nil)
	require.Error(t, err)
	_, err = exec.Execute(ctx, "CALL db.temporal.asOf('FactVersion','fact_key','k1','valid_from','valid_to','bad-datetime') YIELD node", nil)
	require.Error(t, err)

	// Seed with mixed records:
	// - one invalid existing start (ignored)
	// - two valid intervals where latest matching start should be selected by asOf
	_, err = engine.CreateNode(&storage.Node{
		ID:     "bad-start",
		Labels: []string{"FactVersion"},
		Properties: map[string]interface{}{
			"fact_key":   "k1",
			"valid_from": "invalid",
			"valid_to":   nil,
		},
	})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{
		ID:     "v1",
		Labels: []string{"FactVersion"},
		Properties: map[string]interface{}{
			"fact_key":   "k1",
			"valid_from": "2024-01-01T00:00:00Z",
			"valid_to":   "2024-03-01T00:00:00Z",
		},
	})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{
		ID:     "v2",
		Labels: []string{"FactVersion"},
		Properties: map[string]interface{}{
			"fact_key":   "k1",
			"valid_from": "2024-02-01T00:00:00Z",
			"valid_to":   nil,
		},
	})
	require.NoError(t, err)

	// Non-overlapping interval should pass.
	okRes, err := exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('FactVersion','fact_key','valid_from','valid_to','k1','2023-01-01T00:00:00Z','2023-12-31T00:00:00Z')", nil)
	require.NoError(t, err)
	require.Equal(t, true, okRes.Rows[0][0])

	// Overlap should fail.
	_, err = exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('FactVersion','fact_key','valid_from','valid_to','k1','2024-02-15T00:00:00Z','2024-04-01T00:00:00Z')", nil)
	require.Error(t, err)

	// asOf selects most recent valid start covering timestamp.
	asOfRes, err := exec.Execute(ctx, "CALL db.temporal.asOf('FactVersion','fact_key','k1','valid_from','valid_to','2024-02-15T00:00:00Z') YIELD node", nil)
	require.NoError(t, err)
	require.Len(t, asOfRes.Rows, 1)
	n, ok := asOfRes.Rows[0][0].(*storage.Node)
	require.True(t, ok)
	require.Equal(t, storage.NodeID("v2"), n.ID)

	// No matching key returns empty rows.
	noneRes, err := exec.Execute(ctx, "CALL db.temporal.asOf('FactVersion','fact_key','missing','valid_from','valid_to','2024-02-15T00:00:00Z') YIELD node", nil)
	require.NoError(t, err)
	require.Empty(t, noneRes.Rows)
}

func TestTemporalProcedures_RequiredStringArgsBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CALL db.temporal.assertNoOverlap(null,'fact_key','valid_from','valid_to','k1','2024-01-01T00:00:00Z',null)", nil)
	require.Error(t, err)

	_, err = exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('','fact_key','valid_from','valid_to','k1','2024-01-01T00:00:00Z',null)", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "label cannot be empty")

	_, err = exec.Execute(ctx, "CALL db.temporal.asOf('', 'fact_key','k1','valid_from','valid_to','2024-01-01T00:00:00Z') YIELD node", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "label cannot be empty")
}

func TestTemporalProcedures_LabelLookupErrorBranches(t *testing.T) {
	failStore := &failingNodeLookupEngine{
		Engine:     storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"),
		byLabelErr: errors.New("label lookup failed"),
	}
	exec := NewStorageExecutor(failStore)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('FactVersion','fact_key','valid_from','valid_to','k1','2024-01-01T00:00:00Z',null)", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read nodes for label")

	_, err = exec.Execute(ctx, "CALL db.temporal.asOf('FactVersion','fact_key','k1','valid_from','valid_to','2024-01-01T00:00:00Z') YIELD node", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read nodes for label")
}

func TestTemporalProcedures_StrictArgValidationAdditionalBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// assertNoOverlap: required string args beyond label.
	_, err := exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('FactVersion',null,'valid_from','valid_to','k1','2024-01-01T00:00:00Z',null)", nil)
	require.Error(t, err)

	_, err = exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('FactVersion','fact_key',null,'valid_to','k1','2024-01-01T00:00:00Z',null)", nil)
	require.Error(t, err)

	_, err = exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('FactVersion','fact_key','valid_from',null,'k1','2024-01-01T00:00:00Z',null)", nil)
	require.Error(t, err)

	// asOf: required string args for key/from/to props.
	_, err = exec.Execute(ctx, "CALL db.temporal.asOf('FactVersion',null,'k1','valid_from','valid_to','2024-01-01T00:00:00Z') YIELD node", nil)
	require.Error(t, err)

	_, err = exec.Execute(ctx, "CALL db.temporal.asOf('FactVersion','fact_key','k1',null,'valid_to','2024-01-01T00:00:00Z') YIELD node", nil)
	require.Error(t, err)

	_, err = exec.Execute(ctx, "CALL db.temporal.asOf('FactVersion','fact_key','k1','valid_from',null,'2024-01-01T00:00:00Z') YIELD node", nil)
	require.Error(t, err)
}
