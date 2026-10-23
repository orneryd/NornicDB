package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEdgeOverwriteKeepsPhysicalCounts(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		for _, mode := range []string{"hidden-create", "hidden-bulk", "visible-update"} {
			for _, move := range []bool{false, true} {
				name := mode + "/sync"
				if writeBehind {
					name = mode + "/buffered"
				}
				if move {
					name += "/move"
				}
				t.Run(name, func(t *testing.T) {
					engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
						DataDir: t.TempDir(), WriteBehind: writeBehind, WriteBehindInterval: time.Hour,
					}))
					require.NoError(t, err)
					t.Cleanup(func() { _ = engine.Close() })
					for _, node := range []*Node{
						{ID: "test:a", Labels: []string{"Source"}},
						{ID: "test:b", Labels: []string{"Target"}},
						{ID: "test:c", Labels: []string{"Other"}},
					} {
						_, err := engine.CreateNode(node)
						require.NoError(t, err)
					}
					edge := &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "INGEST"}
					require.NoError(t, engine.CreateEdge(edge))
					if mode != "visible-update" {
						require.NoError(t, engine.SetEdgeManuallySuppressed(edge.ID, true))
					}
					replacement := copyEdge(edge)
					if move {
						replacement.Type, replacement.StartNode = "MOVED", "test:c"
					}
					tx, err := engine.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, tx.SetNamespace("test"))
					require.NoError(t, tx.SetImplicit(true))
					switch mode {
					case "hidden-create":
						err = tx.CreateEdge(replacement)
					case "hidden-bulk":
						err = tx.BulkCreateEdges([]*Edge{replacement})
					default:
						err = tx.UpdateEdge(replacement)
					}
					require.NoError(t, err)
					require.NoError(t, tx.Commit())
					verify := func() {
						t.Helper()
						count, err := engine.EdgeCount()
						require.NoError(t, err)
						require.EqualValues(t, 1, count)
						count, err = engine.EdgeCountByType(replacement.Type)
						require.NoError(t, err)
						require.EqualValues(t, 1, count)
						label := "Source"
						if move {
							label = "Other"
						}
						count, err = engine.EdgeCountByStartLabel(label, replacement.Type)
						require.NoError(t, err)
						require.EqualValues(t, 1, count)
						count, err = engine.EdgeCountByEndLabel("Target", replacement.Type)
						require.NoError(t, err)
						require.EqualValues(t, 1, count)
						if move {
							count, err = engine.EdgeCountByType("INGEST")
							require.NoError(t, err)
							require.Zero(t, count)
							count, err = engine.EdgeCountByStartLabel("Source", "INGEST")
							require.NoError(t, err)
							require.Zero(t, count)
						}
						if mode != "visible-update" {
							require.True(t, engine.manuallySuppressed(manualSuppressionEdge, string(edge.ID)))
						}
					}
					verify()
					require.NoError(t, engine.FlushWriteBehind())
					verify()
				})
			}
		}
	}
}

func TestEdgeOverwriteCountsSurviveBufferedReplay(t *testing.T) {
	dir := t.TempDir()
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
	wal := engine.bufferedCommitWAL
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Doc"}})
		require.NoError(t, err)
	}
	edge := &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "INGEST"}
	require.NoError(t, engine.CreateEdge(edge))
	require.NoError(t, engine.SetEdgeManuallySuppressed(edge.ID, true))
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.CreateEdge(copyEdge(edge)))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	crashClose(t, engine, wal)
	recovered := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
	wal2 := recovered.bufferedCommitWAL
	t.Cleanup(func() { _ = wal2.Close(); _ = recovered.Close() })
	_, err = RecoverBufferedCommits(recovered, wal2)
	require.NoError(t, err)
	for _, read := range []func() (int64, error){
		recovered.EdgeCount,
		func() (int64, error) { return recovered.EdgeCountByType("INGEST") },
		func() (int64, error) { return recovered.EdgeCountByStartLabel("Doc", "INGEST") },
		func() (int64, error) { return recovered.EdgeCountByEndLabel("Doc", "INGEST") },
	} {
		count, err := read()
		require.NoError(t, err)
		require.EqualValues(t, 1, count)
	}
	require.True(t, recovered.manuallySuppressed(manualSuppressionEdge, string(edge.ID)))
}
