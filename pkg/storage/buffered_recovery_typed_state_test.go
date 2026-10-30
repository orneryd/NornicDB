package storage

import (
	"bytes"
	"context"
	"encoding/json"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertRecoveredTypedProperties(t *testing.T, expected, actual map[string]any, stage string) {
	t.Helper()
	require.Len(t, actual, len(expected), stage)
	for key, want := range expected {
		if at, ok := want.(time.Time); ok {
			got, ok := actual[key].(time.Time)
			if assert.True(t, ok, stage+" time type") {
				assert.True(t, at.Equal(got), stage+" time instant")
			}
		} else {
			assert.Equal(t, want, actual[key], stage+" "+key)
		}
	}
}

func TestBufferedRecoveryPreservesTypedCurrentAndHistory(t *testing.T) {
	for _, startup := range []bool{false, true} {
		for _, mutation := range []string{"update", "equal_type", "repeat", "delete"} {
			t.Run(mutation+map[bool]string{false: "/replay", true: "/startup"}[startup], func(t *testing.T) {
				dir := t.TempDir()
				open := func() (*BadgerEngine, *WAL) {
					engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour, EngineOptions: EngineOptions{RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: 100}}}))
					if engine != nil {
						fixtureowner.Own(t, engine)
					}
					require.NoError(t, err)
					cfg := DefaultWALConfig()
					cfg.Dir = filepath.Join(dir, "wal")
					cfg.BatchSyncInterval = time.Hour
					wal, err := NewWAL(cfg.Dir, cfg)
					require.NoError(t, err)
					engine.SetBufferedCommitWAL(wal)
					return engine, wal
				}
				engine, wal := open()
				initial := map[string]any{"v": float64(1), "time": time.Date(2026, 10, 15, 1, 2, 3, 0, time.UTC), "bytes": []byte{1, 2, 3}}
				tx, err := engine.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetNamespace("test"))
				_, err = tx.CreateNode(&Node{ID: "test:a", Properties: initial})
				require.NoError(t, err)
				_, err = tx.CreateNode(&Node{ID: "test:b"})
				require.NoError(t, err)
				require.NoError(t, tx.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: initial}))
				require.NoError(t, tx.Commit())
				nodeHead, err := engine.loadNodeMVCCHead("test:a")
				require.NoError(t, err)
				edgeHead, err := engine.loadEdgeMVCCHead("test:r")
				require.NoError(t, err)
				tx, err = engine.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetNamespace("test"))
				require.NoError(t, tx.SetImplicit(true))
				final := map[string]any{"v": any(float64(2)), "time": initial["time"], "bytes": initial["bytes"]}
				if mutation == "equal_type" {
					final["v"] = int64(1)
				}
				if mutation == "repeat" {
					node, err := tx.GetNode("test:a")
					require.NoError(t, err)
					node.Properties = map[string]any{"v": float64(4)}
					require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
					edge, err := tx.GetEdge("test:r")
					require.NoError(t, err)
					edge.Properties = map[string]any{"v": float64(4)}
					require.NoError(t, tx.UpdateEdge(EdgeUpdateRequest{After: edge}))
				}
				if mutation == "delete" {
					require.NoError(t, tx.DeleteEdge("test:r"))
					require.NoError(t, tx.DeleteGraphNode("test:a", CascadeEdges))
				} else {
					node, err := tx.GetNode("test:a")
					require.NoError(t, err)
					node.Properties = final
					require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
					edge, err := tx.GetEdge("test:r")
					require.NoError(t, err)
					edge.Properties = final
					require.NoError(t, tx.UpdateEdge(EdgeUpdateRequest{After: edge}))
				}
				require.NoError(t, tx.Commit())
				require.True(t, tx.AcknowledgedBuffered())
				entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
				require.NoError(t, err)
				crashClose(t, engine, wal)
				recovered, wal2 := open()
				closed := false
				t.Cleanup(func() {
					if !closed {
						_ = wal2.Close()
						_ = recovered.Close()
					}
				})
				if startup {
					n, err := RecoverBufferedCommits(recovered, wal2)
					require.NoError(t, err)
					require.Equal(t, 1, n)
				} else {
					for _, entry := range entries {
						if entry.Operation == OpBufferedCommit {
							require.NoError(t, ReplayWALEntry(recovered, entry))
						}
					}
				}
				check := func(stage string) {
					t.Helper()
					if mutation != "delete" {
						node, err := recovered.GetNode("test:a")
						require.NoError(t, err)
						assertRecoveredTypedProperties(t, final, node.Properties, stage+" current node")
						edge, err := recovered.GetEdge("test:r")
						require.NoError(t, err)
						assertRecoveredTypedProperties(t, final, edge.Properties, stage+" current edge")
					}
					node, err := recovered.GetNodeVisibleAt("test:a", nodeHead.Version)
					require.NoError(t, err)
					assertRecoveredTypedProperties(t, initial, node.Properties, stage+" historic node")
					edge, err := recovered.GetEdgeVisibleAt("test:r", edgeHead.Version)
					require.NoError(t, err)
					assertRecoveredTypedProperties(t, initial, edge.Properties, stage+" historic edge")
				}
				check("warm")
				recovered.nodeCacheMu.Lock()
				clear(recovered.nodeCache)
				recovered.nodeCacheMu.Unlock()
				recovered.edgeCacheMu.Lock()
				clear(recovered.edgeCache)
				recovered.edgeCacheMu.Unlock()
				check("evicted")
				require.NoError(t, wal2.Close())
				require.NoError(t, recovered.Close())
				closed = true
				recovered, wal2 = open()
				closed = false
				check("reopened")
			})
		}
	}
}

func TestSnapshotEmptyNamedVectorsDoNotAdvertiseAvailability(t *testing.T) {
	for _, vector := range [][]float32{nil, {}, {1, 2}} {
		t.Run(string(rune('0'+len(vector))), func(t *testing.T) {
			engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			chunks := make([][]float32, 16)
			for i := range chunks {
				chunks[i] = make([]float32, 1536)
				chunks[i][0] = 1
			}
			_, err = engine.CreateNode(&Node{ID: "test:n", ChunkEmbeddings: chunks, NamedEmbeddings: map[string][]float32{"title": vector}, EmbedMeta: map[string]any{"has_embedding": true, "has_chunks": true, "chunk_count": int64(16)}})
			require.NoError(t, err)
			var out bytes.Buffer
			require.NoError(t, WriteSnapshot(context.Background(), engine, &out, SnapshotOptions{}))
			visitor := &recoveryFieldsVisitor{}
			_, err = ReadSnapshot(bytes.NewReader(out.Bytes()), visitor)
			require.NoError(t, err)
			require.Len(t, visitor.nodes, 1)
			restored := visitor.nodes[0]
			require.Empty(t, restored.ChunkEmbeddings)
			assert.Equal(t, len(vector) > 0, restored.EmbedMeta["has_embedding"] == true)
			original, err := engine.GetNode("test:n")
			require.NoError(t, err)
			require.Equal(t, true, original.EmbedMeta["has_embedding"])
		})
	}
}

func TestBufferedRecoveryPreservesCreatedVectorsAndEndpoints(t *testing.T) {
	for _, startup := range []bool{false, true} {
		t.Run(map[bool]string{false: "replay", true: "startup"}[startup], func(t *testing.T) {
			dir := t.TempDir()
			open := func() (*BadgerEngine, *WAL) {
				b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour}))
				if b != nil {
					fixtureowner.Own(t, b)
				}
				require.NoError(t, err)
				cfg := DefaultWALConfig()
				cfg.Dir = filepath.Join(dir, "wal")
				w, err := NewWAL(cfg.Dir, cfg)
				require.NoError(t, err)
				b.SetBufferedCommitWAL(w)
				return b, w
			}
			b, w := open()
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetNamespace("test"))
			require.NoError(t, tx.SetImplicit(true))
			chunks := make([][]float32, 16)
			for i := range chunks {
				chunks[i] = make([]float32, 1536)
				chunks[i][0] = float32(i) + 0.25
			}
			_, err = tx.CreateNode(&Node{ID: "test:new-a", ChunkEmbeddings: chunks, NamedEmbeddings: map[string][]float32{"title": {1, 2}}, EmbedMeta: map[string]any{"has_embedding": true, "has_chunks": true, "chunk_count": int64(16), "custom": float64(1)}})
			require.NoError(t, err)
			_, err = tx.CreateNode(&Node{ID: "test:new-b"})
			require.NoError(t, err)
			require.NoError(t, tx.CreateEdge(&Edge{ID: "test:new-r", StartNode: "test:new-a", EndNode: "test:new-b", Type: "R", Properties: map[string]any{"v": float64(1)}}))
			require.NoError(t, tx.Commit())
			require.True(t, tx.AcknowledgedBuffered())
			entries, err := ReadWALEntriesFromDir(w.Config().Dir)
			require.NoError(t, err)
			crashClose(t, b, w)
			b, w = open()
			t.Cleanup(func() { _ = w.Close(); _ = b.Close() })
			if startup {
				_, err = RecoverBufferedCommits(b, w)
				require.NoError(t, err)
			} else {
				for _, entry := range entries {
					if entry.Operation == OpBufferedCommit {
						require.NoError(t, ReplayWALEntry(b, entry))
					}
				}
			}
			n, err := b.GetNode("test:new-a")
			require.NoError(t, err)
			require.Equal(t, chunks, n.ChunkEmbeddings)
			require.Equal(t, []float32{1, 2}, n.NamedEmbeddings["title"])
			require.Equal(t, float64(1), n.EmbedMeta["custom"])
			edge, err := b.GetEdge("test:new-r")
			require.NoError(t, err)
			require.Equal(t, NodeID("test:new-a"), edge.StartNode)
			require.Equal(t, NodeID("test:new-b"), edge.EndNode)
			require.Equal(t, float64(1), edge.Properties["v"])
		})
	}
}

func TestBufferedRecoveryRejectsCorruptAuthoritativeBodies(t *testing.T) {
	for _, edge := range []bool{false, true} {
		t.Run(map[bool]string{false: "node", true: "edge"}[edge], func(t *testing.T) {
			b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			data := WALBufferedCommitData{TxID: "corrupt", Namespace: "test", Writes: map[string][]byte{}}
			if edge {
				data.Ops = []Operation{{Type: OpCreateEdge, Edge: &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R"}}}
				data.Writes[string(edgeKey("test:r"))] = []byte{0xff}
			} else {
				data.Ops = []Operation{{Type: OpCreateNode, Node: &Node{ID: "test:n"}}}
				data.Writes[string(nodeKey("test:n"))] = []byte{0xff}
			}
			payload, err := json.Marshal(data)
			require.NoError(t, err)
			require.Error(t, ReplayWALEntry(b, WALEntry{Operation: OpBufferedCommit, Data: payload}))
			marker, err := b.observeBufferedCommitReceipt(bufferedReceiptRead{txID: "corrupt"})
			require.NoError(t, err)
			require.False(t, marker.present)
			_, err = b.GetNode("test:n")
			require.ErrorIs(t, err, ErrNotFound)
			_, err = b.GetEdge("test:r")
			require.ErrorIs(t, err, ErrNotFound)
		})
	}
}
