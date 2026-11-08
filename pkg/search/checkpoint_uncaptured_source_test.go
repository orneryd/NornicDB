package search

import (
	"context"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// This reader preserves native rows and point-in-time identity but deliberately
// provides no snapshot spanning population. Two independent identities cannot
// prove that traversal observed one consistent database state.
type uncapturedCheckpointSource struct{ storage.Engine }

func (e *uncapturedCheckpointSource) IndexSourceStamp(ctx context.Context) (string, error) {
	return storage.ReadIndexSourceStamp(ctx, e.Engine)
}

func TestCheckpointFallbackTraversalRequiresCapturedSource(t *testing.T) {
	dir := t.TempDir()
	base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(filepath.Join(dir, "graph")))
	require.NoError(t, err)
	_, err = base.CreateNode(t.Context(), storage.CopyNode(compatStoreNodes[0]))
	require.NoError(t, err)
	source := &uncapturedCheckpointSource{Engine: base}
	require.False(t, storage.SupportsReadSnapshot(t.Context(), source))
	stamp, err := source.IndexSourceStamp(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, stamp)
	svc := compatStoreService(source, dir)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.BuildIndexes(t.Context()))
	require.True(t, svc.IsReady())
	require.NotNil(t, svc.vectorFileStore)
	require.Equal(t, 1, svc.vectorFileStore.Count(), "uncaptured readers still build and serve their population")
	record, err := svc.savedPopulationSource()
	require.NoError(t, err)
	require.True(t, record.Complete)
	require.Empty(t, record.SourceStamp, "no snapshot pin spans this traversal; independently equal stamps cannot certify it")
}
