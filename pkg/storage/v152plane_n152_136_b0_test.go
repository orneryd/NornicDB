package storage

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// NOR-390 corruption-policy reproduction: a stored row this engine cannot
// decode must fail the public native stream that reaches it, rather than
// returning the remaining rows as if they were the whole population. The
// canonical readers (badger_read_errors_test.go) already cover point reads and
// label streams; these cover the whole-engine node streams, the relationship
// type stream named by NOR-390 (edge_type_stream.go), the all-edge stream and
// the read-snapshot counterpart the search build reads its baseline through.
//
// Every case runs twice over the same fixture: the valid-row control proves
// the reader reaches and visits every row, and the corrupt run proves the same
// read now returns a decode error instead of a partial success. Corrupting a
// row is a deliberate mutation of this test's own temporary store through the
// package's existing write seam (withUpdate plus the key builders); no
// production code is involved.

const nor390NodeType = "L"
const nor390EdgeType = "LINK"

type nor390NodeStream struct {
	name string
	read func(*BadgerEngine, func(*Node) error) error
}

func nor390NodeStreams() []nor390NodeStream {
	return []nor390NodeStream{
		{"StreamNodesWithOptions", func(b *BadgerEngine, visit func(*Node) error) error {
			return b.StreamNodesWithOptions(context.Background(), StreamNodesOptions{WithEmbeddings: true}, visit)
		}},
		{"IterateNodes", func(b *BadgerEngine, visit func(*Node) error) error {
			return b.IterateNodes(func(node *Node) bool {
				return visit(node) == nil
			})
		}},
	}
}

type nor390EdgeStream struct {
	name string
	read func(*BadgerEngine, func(*Edge) error) error
}

func nor390EdgeStreams() []nor390EdgeStream {
	return []nor390EdgeStream{
		// The relationship type stream named by NOR-390's source line.
		{"StreamEdgesByType", func(b *BadgerEngine, visit func(*Edge) error) error {
			return b.StreamEdgesByType(context.Background(), nor390EdgeType, visit)
		}},
		{"StreamEdgesByTypeInScope", func(b *BadgerEngine, visit func(*Edge) error) error {
			return b.StreamEdgesByTypeInScope(context.Background(), "test:", nor390EdgeType, visit)
		}},
		// The canonical package-level entry the constraints and readers use.
		{"StreamEdgesByType package entry", func(b *BadgerEngine, visit func(*Edge) error) error {
			return StreamEdgesByType(context.Background(), b, nor390EdgeType, visit)
		}},
		// The stored-read variant announces it reads hidden rows too; it must
		// still fail on a row it cannot decode.
		{"StreamStoredEdgesByType", func(b *BadgerEngine, visit func(*Edge) error) error {
			return b.StreamStoredEdgesByType(context.Background(), nor390EdgeType, visit)
		}},
		{"StreamEdges", func(b *BadgerEngine, visit func(*Edge) error) error {
			return b.StreamEdges(context.Background(), visit)
		}},
	}
}

// nor390NodeFixture creates two labelled nodes.
func nor390NodeFixture(t *testing.T, b *BadgerEngine) {
	t.Helper()
	for _, id := range []NodeID{"test:a1", "test:a2"} {
		_, err := b.CreateNode(t.Context(), &Node{ID: id, Labels: []string{nor390NodeType}, Properties: map[string]any{"name": string(id)}})
		require.NoError(t, err)
	}
}

// nor390EdgeFixture creates two relationships of the corrupted type between
// the fixture nodes.
func nor390EdgeFixture(t *testing.T, b *BadgerEngine) {
	t.Helper()
	nor390NodeFixture(t, b)
	for _, id := range []EdgeID{"test:e1", "test:e2"} {
		require.NoError(t, b.CreateEdge(t.Context(), &Edge{ID: id, StartNode: "test:a1", EndNode: "test:a2", Type: nor390EdgeType, Properties: map[string]any{"name": string(id)}}))
	}
}

// nor390CorruptNodeBody replaces one stored node body with bytes the decoder
// cannot read, through the engine's write seam.
func nor390CorruptNodeBody(t *testing.T, b *BadgerEngine, id NodeID) {
	t.Helper()
	require.NoError(t, b.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
		return txn.Set(nodeKey(id), []byte{0xff})
	}))
	b.nodeCacheMu.Lock()
	clear(b.nodeCache)
	b.nodeCacheMu.Unlock()
}

// nor390CorruptEdgeBody replaces one stored relationship body with bytes the
// decoder cannot read, through the engine's write seam.
func nor390CorruptEdgeBody(t *testing.T, b *BadgerEngine, id EdgeID) {
	t.Helper()
	require.NoError(t, b.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
		return txn.Set(edgeKey(id), []byte{0xff})
	}))
	b.cacheInvalidateEdges()
	b.InvalidateEdgeTypeCache()
}

func TestNOR390CorruptNodeFailsPublicNodeStreams(t *testing.T) {
	for _, stream := range nor390NodeStreams() {
		t.Run(stream.name, func(t *testing.T) {
			for _, corrupt := range []bool{false, true} {
				state := "valid control"
				if corrupt {
					state = "corrupt row"
				}
				t.Run(state, func(t *testing.T) {
					b := newTestEngine(t)
					nor390NodeFixture(t, b)
					if corrupt {
						nor390CorruptNodeBody(t, b, "test:a1")
					}
					visited := 0
					err := stream.read(b, func(*Node) error {
						visited++
						return nil
					})
					if corrupt {
						require.Less(t, visited, 2, "a partially decoded population must not be reported as complete")
						require.ErrorContains(t, err, "scanning node \"test:a1\"")
					} else {
						require.Equal(t, 2, visited, "the read reaches both stored nodes")
						require.NoError(t, err)
					}
				})
			}
		})
	}
}

func TestNOR390CorruptEdgeFailsEdgeTypeStreams(t *testing.T) {
	for _, stream := range nor390EdgeStreams() {
		t.Run(stream.name, func(t *testing.T) {
			for _, corrupt := range []bool{false, true} {
				state := "valid control"
				if corrupt {
					state = "corrupt row"
				}
				t.Run(state, func(t *testing.T) {
					b := newTestEngine(t)
					nor390EdgeFixture(t, b)
					if corrupt {
						nor390CorruptEdgeBody(t, b, "test:e1")
					}
					visited := 0
					err := stream.read(b, func(*Edge) error {
						visited++
						return nil
					})
					if corrupt {
						require.Less(t, visited, 2, "a partially decoded population must not be reported as complete")
						// The type-index streams surface the decoder's own
						// error; the all-edge stream wraps it with the row it
						// was reading. Both name the undecodable bytes.
						require.ErrorContains(t, err, "0xff")
					} else {
						require.Equal(t, 2, visited, "the read reaches both stored relationships")
						require.NoError(t, err)
					}
				})
			}
		})
	}
}

func TestNOR390CorruptEntityFailsReadSnapshotStreams(t *testing.T) {
	t.Run("nodes", func(t *testing.T) {
		for _, corrupt := range []bool{false, true} {
			state := "valid control"
			if corrupt {
				state = "corrupt row"
			}
			t.Run(state, func(t *testing.T) {
				b := newTestEngine(t)
				nor390NodeFixture(t, b)
				if corrupt {
					nor390CorruptNodeBody(t, b, "test:a1")
				}
				snapshot, err := b.OpenReadSnapshot(context.Background())
				require.NoError(t, err)
				t.Cleanup(snapshot.Close)
				visited := 0
				err = snapshot.IterateNodes(func(*Node) bool {
					visited++
					return true
				})
				if corrupt {
					require.Less(t, visited, 2, "the snapshot must not report the rest as the whole population")
					require.ErrorContains(t, err, "scanning node \"test:a1\"")
				} else {
					require.Equal(t, 2, visited)
					require.NoError(t, err)
				}
			})
		}
	})

	t.Run("edges", func(t *testing.T) {
		for _, corrupt := range []bool{false, true} {
			state := "valid control"
			if corrupt {
				state = "corrupt row"
			}
			t.Run(state, func(t *testing.T) {
				b := newTestEngine(t)
				nor390EdgeFixture(t, b)
				if corrupt {
					nor390CorruptEdgeBody(t, b, "test:e1")
				}
				snapshot, err := b.OpenReadSnapshot(context.Background())
				require.NoError(t, err)
				t.Cleanup(snapshot.Close)
				visited := 0
				err = snapshot.StreamEdges(context.Background(), func(*Edge) error {
					visited++
					return nil
				})
				if corrupt {
					require.Less(t, visited, 2, "the snapshot must not report the rest as the whole population")
					require.ErrorContains(t, err, "reading relationship \"test:e1\"")
				} else {
					require.Equal(t, 2, visited)
					require.NoError(t, err)
				}
			})
		}
	})
}
