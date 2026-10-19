package storage

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Preserve the reviewer's public-path allocation probe: excluded vectors must
// not contribute payload-sized allocations to a stripped projected scan.
func TestReviewerStrippedProjectionCopyCost(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	b.writeBehind.Close()
	vectors := make([][]float32, 32)
	for i := range vectors {
		vectors[i] = make([]float32, 1536)
	}
	bufferedCreate(t, b, &Node{ID: "test:strip-cost", Labels: []string{"Doc"}, Properties: map[string]any{"keep": "ack"}, ChunkEmbeddings: vectors})
	opts := StreamNodesOptions{Projection: []string{"keep"}, StripEmbeddings: true}
	scan := func() {
		calls := 0
		err := b.StreamNodesWithOptions(context.Background(), opts, func(n *Node) error {
			calls++
			if len(n.ChunkEmbeddings) != 0 || n.Properties["keep"] != "ack" {
				t.Fatal("wrong projection")
			}
			return nil
		})
		if err != nil || calls != 1 {
			t.Fatalf("stream: calls=%d err=%v", calls, err)
		}
	}
	scan()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < 10; i++ {
		scan()
	}
	runtime.ReadMemStats(&after)
	bytesPerRead := (after.TotalAlloc - before.TotalAlloc) / 10
	t.Logf("stripped projected stream allocated %d bytes/node for 196608 bytes of vectors excluded from output", bytesPerRead)
	if bytesPerRead > 65536 {
		t.Errorf("stripped projection copies excluded vector payload: %d bytes/node", bytesPerRead)
	}
}

func TestSnapshotOverlayStrippedStreamOwnership(t *testing.T) {
	for _, reader := range []string{"engine", "transaction", "pending-new", "pending-replace", "legacy"} {
		for _, projection := range [][]string{nil, {"keep"}} {
			name := reader + "/full-properties"
			if projection != nil {
				name = reader + "/projected"
			}
			t.Run(name, func(t *testing.T) {
				var b *BadgerEngine
				var err error
				if reader == "legacy" {
					b, err = NewBadgerEngineInMemory()
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, b.Close()) })
				} else {
					b = newWriteBehindTestEngine(t, time.Hour)
					b.writeBehind.Close()
				}
				node := &Node{ID: "test:owned", Labels: []string{"Doc"}, Properties: map[string]any{"keep": "ack", "drop": "retained"},
					ChunkEmbeddings: [][]float32{{1, 2}}, NamedEmbeddings: map[string][]float32{"named": {3, 4}}, EmbedMeta: map[string]any{"chunk_count": 1, "origin": "ack"}}
				if reader == "legacy" {
					_, err = b.CreateNode(node)
					require.NoError(t, err)
				} else if reader != "pending-new" {
					bufferedCreate(t, b, node)
				}
				scan, read := b.StreamNodesWithOptions, b.GetNode
				if reader != "engine" {
					tx, err := b.BeginTransaction()
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, tx.Rollback()) })
					if reader == "pending-new" {
						_, err = tx.CreateNode(node)
						require.NoError(t, err)
					} else if reader == "pending-replace" {
						require.NoError(t, tx.UpdateNode(node))
					} else if reader == "legacy" {
						tx.snapshotTx.Discard()
						tx.snapshotTx = nil
						tx.readTS = MVCCVersion{}
					}
					scan = tx.StreamNodesWithOptions
					if reader != "legacy" {
						read = tx.GetNode
					}
				}
				calls := 0
				err = scan(context.Background(), StreamNodesOptions{Projection: projection, StripEmbeddings: true}, func(result *Node) error {
					calls++
					if len(result.ChunkEmbeddings) != 0 || len(result.NamedEmbeddings) != 0 {
						return fmt.Errorf("stripped stream retained chunk=%d named=%d vectors", len(result.ChunkEmbeddings), len(result.NamedEmbeddings))
					}
					if result.Properties["keep"] != "ack" || len(result.Labels) != 1 || result.EmbedMeta["origin"] != "ack" {
						return fmt.Errorf("stripped stream omitted retained fields")
					}
					if _, present := result.Properties["drop"]; projection != nil && present {
						return fmt.Errorf("stripped stream retained an omitted property")
					}
					result.Labels[0] = "callback"
					result.Properties["keep"] = "callback"
					result.EmbedMeta["origin"] = "callback"
					return nil
				})
				require.NoError(t, err)
				require.Equal(t, 1, calls)
				original, err := read("test:owned")
				require.NoError(t, err)
				require.Equal(t, []string{"Doc"}, original.Labels)
				require.Equal(t, "ack", original.Properties["keep"])
				require.Equal(t, "ack", original.EmbedMeta["origin"])
				require.Equal(t, [][]float32{{1, 2}}, original.ChunkEmbeddings)
			})
		}
	}
}
