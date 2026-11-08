package search

import (
	"context"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A completed initial population does not prove that later source writes were
// delivered to the search service. Saving must not certify omitted updates.
func TestCheckpointDoesNotCertifyUndeliveredSourceWrite(t *testing.T) {
	dir := t.TempDir()
	engine, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(filepath.Join(dir, "graph")))
	require.NoError(t, err)
	svc := compatStoreService(engine, dir)
	t.Cleanup(func() { _ = svc.Close(); _ = engine.Close() })
	for _, n := range compatStoreNodes {
		_, err = engine.CreateNode(t.Context(), storage.CopyNode(n))
		require.NoError(t, err)
	}
	require.NoError(t, svc.BuildIndexes(context.Background()))
	// Intentionally no IndexNode call: the database and index now differ.
	_, err = engine.CreateNode(t.Context(), storage.CopyNode(compatStoreLiveNode))
	require.NoError(t, err)
	svc.PersistIndexesToDisk()
	require.NoError(t, svc.Close())
	require.NoError(t, engine.Close())
	requireCompatStoreServes(t, dir, 1)
}
