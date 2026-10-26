package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Every registered domain scalar is compared with the independent durable
// decoder, both at the top level and in recursively retained containers.
func TestResidualRegisteredPropertyImages(t *testing.T) {
	for _, route := range []string{"direct", "transaction", "buffered", "wal"} {
		t.Run(route, func(t *testing.T) {
			dir := t.TempDir()
			open := func() *storage.BadgerEngine {
				b, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, WriteBehind: route == "buffered", WriteBehindInterval: time.Hour})
				require.NoError(t, err)
				return b
			}
			b := open()
			t.Cleanup(func() { _ = b.Close() })
			var wal *storage.WAL
			walDir := ""
			if route == "wal" {
				t.Cleanup(config.WithWALEnabled())
				walDir = t.TempDir()
				var err error
				wal, err = storage.NewWAL(walDir, &storage.WALConfig{Dir: walDir, SyncMode: "immediate"})
				require.NoError(t, err)
				t.Cleanup(func() { _ = wal.Close() })
			}
			zone, err := time.LoadLocation("Europe/London")
			require.NoError(t, err)
			at := time.Date(2026, 7, 2, 3, 4, 5, 123, zone)
			duration := &cypher.CypherDuration{Months: 2, Days: 3, Seconds: 4, Nanos: 5}
			floats, ints := []float64{1.5, 2.5}, []int64{1, 2}
			values := map[string]any{
				"date": cypher.CypherDate{Time: at}, "local-time": cypher.CypherLocalTime{Time: at},
				"time": cypher.CypherTime{Time: at}, "local-date-time": cypher.CypherLocalDateTime{Time: at},
				"zoned-date-time":  cypher.CypherDateTime{Time: at, ZoneID: "Europe/London"},
				"offset-date-time": cypher.CypherDateTime{Time: at}, "duration": duration,
				"point": cypher.CypherPoint{SRID: 4979, X: 1, Y: 2, Z: 3}, "uuid": cypher.CypherUUID{},
				"vector-float": cypher.CypherVector{Type: cypher.VectorFloat64, Floats: floats},
				"vector-int":   cypher.CypherVector{Type: cypher.VectorInteger64, Ints: ints},
				"vector-empty": cypher.CypherVector{Type: cypher.VectorFloat32, Floats: []float64{}},
				"plain-time":   at, "empty": []any{}, "binary": []byte{1, 2},
			}
			props := map[string]any{"nested": []any{values}}
			for name, value := range values {
				props[name] = value
			}
			n := &storage.Node{ID: "test:registered-a", Properties: props}
			other := &storage.Node{ID: "test:registered-b"}
			edge := &storage.Edge{ID: "test:registered-e", StartNode: n.ID, EndNode: other.ID, Type: "REGISTERED", Properties: props}
			if route == "direct" {
				_, err = b.CreateNode(n)
				require.NoError(t, err)
				_, err = b.CreateNode(other)
				require.NoError(t, err)
				require.NoError(t, b.CreateEdge(edge))
			} else {
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.SetNamespace("test"))
				require.NoError(t, tx.SetImplicit(route == "buffered"))
				_, err = tx.CreateNode(n)
				require.NoError(t, err)
				_, err = tx.CreateNode(other)
				require.NoError(t, err)
				require.NoError(t, tx.CreateEdge(edge))
				if route == "wal" {
					prepared, err := tx.PrepareEntityCommit(context.Background(), storage.EntityCommitRequest{LogicalID: "registered-wal"})
					require.NoError(t, err)
					defer prepared.Abort()
					owner := storage.NewWALEngine(b, wal)
					receipt, err := owner.SubmitEntityCommit(context.Background(), prepared, nil)
					require.NoError(t, err)
					require.NoError(t, receipt.Wait(context.Background()))
				} else {
					require.NoError(t, tx.CommitContext(context.Background()))
				}
			}
			duration.Days, floats[0], ints[0] = 99, 99, 99
			values["reused"] = true
			warmNode, err := b.GetStoredNode(n.ID)
			require.NoError(t, err)
			warmEdge, err := b.GetStoredEdge(edge.ID)
			require.NoError(t, err)
			require.Equal(t, int64(3), warmNode.Properties["duration"].(*cypher.CypherDuration).Days)
			require.Equal(t, float64(1.5), warmEdge.Properties["vector-float"].(cypher.CypherVector).Floats[0])
			// Returned scalars and containers also have independent ownership.
			returned, err := b.GetStoredNode(n.ID)
			require.NoError(t, err)
			returned.Properties["duration"].(*cypher.CypherDuration).Days = 88
			returned.Properties["vector-int"].(cypher.CypherVector).Ints[0] = 88
			func() {
				snapshot, err := b.OpenReadSnapshot(context.Background())
				require.NoError(t, err)
				defer snapshot.Close()
				for pass := 0; pass < 2; pass++ {
					foundNode, foundEdge := false, false
					require.NoError(t, snapshot.IterateNodes(func(node *storage.Node) bool {
						if node.ID == n.ID {
							foundNode = true
							require.Equal(t, warmNode.Properties, node.Properties)
							node.Properties["duration"].(*cypher.CypherDuration).Days = 77
						}
						return true
					}))
					require.NoError(t, snapshot.StreamEdges(context.Background(), func(row *storage.Edge) error {
						if row.ID == edge.ID {
							foundEdge = true
							require.Equal(t, warmEdge.Properties, row.Properties)
							row.Properties["vector-int"].(cypher.CypherVector).Ints[0] = 77
						}
						return nil
					}))
					require.True(t, foundNode)
					require.True(t, foundEdge)
				}
			}()
			if wal != nil {
				require.NoError(t, wal.Close())
				recovered, result, err := storage.RecoverFromWALWithResult(walDir, "")
				require.NoError(t, err)
				t.Cleanup(func() { _ = recovered.Close() })
				require.Empty(t, result.Errors)
				recoveredNode, err := recovered.GetStoredNode(n.ID)
				require.NoError(t, err)
				recoveredEdge, err := recovered.GetStoredEdge(edge.ID)
				require.NoError(t, err)
				require.Equal(t, warmNode.Properties, recoveredNode.Properties)
				require.Equal(t, warmEdge.Properties, recoveredEdge.Properties)
			}
			require.NoError(t, b.FlushWriteBehind())
			require.NoError(t, b.Close())
			b = open()
			coldNode, err := b.GetStoredNode(n.ID)
			require.NoError(t, err)
			coldEdge, err := b.GetStoredEdge(edge.ID)
			require.NoError(t, err)
			require.Equal(t, coldNode.Properties, warmNode.Properties)
			require.Equal(t, coldEdge.Properties, warmEdge.Properties)
		})
	}
}

func TestResidualPropertyImageKeepsCodecRejection(t *testing.T) {
	for _, value := range []any{
		cypher.CypherDateTime{Time: time.Unix(1, 0), ZoneID: "No/Such_Zone"},
		time.Unix(1, 0).In(time.FixedZone("No/Such_Zone", 0)),
		cypher.CypherVector{Type: cypher.VectorCoordinateType(255)},
		cypher.CypherPoint{SRID: -1},
	} {
		b, err := storage.NewBadgerEngine(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { _ = b.Close() })
		_, err = b.CreateNode(&storage.Node{ID: "test:invalid", Properties: map[string]any{"nested": []any{value}}})
		require.Error(t, err)
		_, err = b.GetStoredNode("test:invalid")
		require.ErrorIs(t, err, storage.ErrNotFound)
	}
}
