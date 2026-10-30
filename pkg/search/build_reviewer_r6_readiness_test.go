package search

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"

	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReviewerR6FinalDrainDoesNotPublishReadiness(t *testing.T) {
	engine := &triageSnapshotEngine{MemoryEngine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))}
	t.Cleanup(func() { _ = engine.Close() })
	_, err := engine.CreateNode(triageNode("nornic:baseline"))
	require.NoError(t, err)
	svc := newSearchPersistenceTestService(t, engine, t.TempDir(), false)
	injected := errors.New("reviewer final-drain I/O failure")
	reached, published := false, false
	svc.SetLogger(slog.New(&triageReplayLog{match: "starting vector pipeline warmup", during: func() {
		svc.vectorFileStore.mu.Lock()
		svc.vectorFileStore.writeRecord = func(file *os.File, id string, vec []float32) error {
			if strings.Contains(id, "bad") {
				reached = true
				published = svc.IsReady()
				return injected
			}
			return writeVectorRecord(file, id, vec)
		}
		svc.vectorFileStore.mu.Unlock()
		svc.SetPersistenceEnabled(true) // atEnd: retain following writes for final drain
		for _, id := range []string{"nornic:bad", "nornic:good"} {
			node := triageNode(id)
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
			require.NoError(t, svc.IndexNode(node))
		}
	}}))
	err = svc.BuildIndexes(context.Background())
	require.ErrorIs(t, err, injected)
	require.True(t, reached)
	require.False(t, svc.IsReady(), "return must revoke readiness")
	_, found := svc.getVectorLookup()(nodeNamedVectorKey("nornic:good", "v"))
	require.True(t, found, "later write must still apply")
	require.False(t, published, "IsReady was true while an acknowledged final-drain write was failing")
}
