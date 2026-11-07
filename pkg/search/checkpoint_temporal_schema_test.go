package search

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type temporalSchemaCheckpointEngine struct {
	*storage.BadgerEngine
	before, after func()
}
type temporalSchemaCheckpointSnapshot struct {
	storage.ReadSnapshotView
	before func()
}

func (e *temporalSchemaCheckpointEngine) OpenReadSnapshot(ctx context.Context) (storage.ReadSnapshotView, error) {
	inner, err := e.BadgerEngine.OpenReadSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return &temporalSchemaCheckpointSnapshot{inner, e.before}, nil
}
func (s *temporalSchemaCheckpointSnapshot) IndexSourceStamp(ctx context.Context) (string, error) {
	return storage.ReadIndexSourceStamp(ctx, s.ReadSnapshotView)
}
func (s *temporalSchemaCheckpointSnapshot) IterateNodes(yield func(*storage.Node) bool) error {
	s.before()
	return s.ReadSnapshotView.IterateNodes(yield)
}

// The boundary is after the native decision, not after IterateNodes: small
// populations are batched and classified only when iteration has returned.
func (e *temporalSchemaCheckpointEngine) IsCurrentTemporalNode(ctx context.Context, node *storage.Node, now time.Time) (bool, error) {
	current, err := e.BadgerEngine.IsCurrentTemporalNode(ctx, node, now)
	if err == nil && !current {
		e.after()
	}
	return current, err
}

func TestCheckpointRejectsTemporalSchemaRoundTripDuringPopulation(t *testing.T) {
	dir := t.TempDir()
	base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(filepath.Join(dir, "graph")))
	require.NoError(t, err)
	old := storage.CopyNode(compatStoreNodes[0])
	old.Labels = []string{"Historical"}
	old.Properties["key"] = "past"
	old.Properties["from"] = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	old.Properties["to"] = time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err = base.CreateNode(t.Context(), old)
	require.NoError(t, err)
	_, err = base.CreateNode(t.Context(), storage.CopyNode(compatStoreNodes[1]))
	require.NoError(t, err)
	schema := base.GetSchemaForNamespace("nornic")
	require.NoError(t, schema.AddConstraint(storage.Constraint{Name: "doc_text", Type: storage.ConstraintUnique, Label: "Doc", Properties: []string{"text"}}))
	constraint := storage.Constraint{Name: "historical_time", Type: storage.ConstraintTemporal, Label: "Historical", Properties: []string{"key", "from", "to"}}
	// ADD/DROP restores exactly the existing nonempty durable schema.
	require.NoError(t, schema.AddConstraint(constraint))
	require.NoError(t, schema.DropConstraint(constraint.Name))
	engine := &temporalSchemaCheckpointEngine{BadgerEngine: base,
		before: func() {
			require.NoError(t, schema.AddConstraint(constraint))
			current, err := base.IsCurrentTemporalNode(t.Context(), old, time.Now())
			require.NoError(t, err)
			require.False(t, current)
		},
		after: func() { require.NoError(t, schema.DropConstraint(constraint.Name)) }}
	svc := compatStoreService(engine, dir)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.BuildIndexes(t.Context()))
	record, err := svc.savedPopulationSource()
	require.NoError(t, err)
	require.NotNil(t, svc.vectorFileStore)
	require.Equal(t, 1, svc.vectorFileStore.Count(), "historical row was excluded during the transient rule")
	require.Empty(t, record.SourceStamp, "a transient temporal policy excluded a captured row; restored schema bytes do not certify the resulting population")
	require.NoError(t, svc.Close())
	// With the rule gone, restart must rebuild instead of trusting the
	// incomplete vector population and must recover both original vectors.
	engine.before, engine.after = func() {}, func() {}
	restarted := compatStoreService(engine, dir)
	t.Cleanup(func() { require.NoError(t, restarted.Close()) })
	require.NoError(t, restarted.BuildIndexes(t.Context()))
	require.NotNil(t, restarted.vectorFileStore)
	require.Equal(t, 2, restarted.vectorFileStore.Count())
	require.NoError(t, restarted.Close())
	for range 3 {
		require.NoError(t, base.Close())
		base, err = fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(filepath.Join(dir, "graph")))
		require.NoError(t, err)
		counting := &countingStoreEngine{BadgerEngine: base}
		unchanged := compatStoreService(counting, dir)
		require.NoError(t, unchanged.BuildIndexes(t.Context()))
		require.Zero(t, counting.iterations.Load(), "unchanged nonempty schema must preserve no-scan restart")
		require.Equal(t, 2, unchanged.vectorFileStore.Count())
		require.NoError(t, unchanged.Close())
	}

}
