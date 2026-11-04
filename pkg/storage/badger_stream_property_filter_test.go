package storage

// A projected node scan with a PropertyFilter skips nodes whose projected
// properties the filter rejects, before decoding the rest of them (#824). A
// filter without a projection decodes every property and still filters before
// the node is materialized, and a record the scan cannot decode fails the scan
// instead of being passed over.

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerStreamNodesPropertyFilter(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	for _, node := range []*Node{
		{ID: "ns:a", Labels: []string{"Code"}, Properties: map[string]interface{}{"id": "a", "body": "x"}},
		{ID: "ns:b", Labels: []string{"Code"}, Properties: map[string]interface{}{"id": "b", "body": "y"}},
		{ID: "ns:c", Labels: []string{"Other"}, Properties: map[string]interface{}{"body": "z"}},
	} {
		_, err := engine.CreateNode(t.Context(), node)
		require.NoError(t, err)
	}

	stream := func(opts StreamNodesOptions) []*Node {
		t.Helper()
		var visited []*Node
		require.NoError(t, engine.StreamNodesWithOptions(context.Background(), opts, func(node *Node) error {
			visited = append(visited, node)
			return nil
		}))
		return visited
	}
	wantB := func(props map[string]interface{}) bool { return props["id"] == "b" }

	visited := stream(StreamNodesOptions{Projection: []string{"id"}, PropertyFilter: wantB})
	require.Len(t, visited, 1)
	require.Equal(t, NodeID("ns:b"), visited[0].ID)
	require.Equal(t, []string{"Code"}, visited[0].Labels)
	require.Equal(t, map[string]interface{}{"id": "b"}, visited[0].Properties)

	// The prefix scan applies it too; without a filter every node is visited.
	require.Len(t, stream(StreamNodesOptions{Prefix: "ns:", Projection: []string{"id"}, PropertyFilter: wantB}), 1)
	require.Len(t, stream(StreamNodesOptions{Projection: []string{"id"}}), 3)
}

// A filter without a projection decodes every property: the node a scan keeps
// is the whole node, and the filter sees every property it may test.
func TestBadgerStreamNodesPropertyFilterWithoutProjection(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	for _, node := range []*Node{
		{ID: "ns:a", Labels: []string{"Code"}, Properties: map[string]interface{}{"id": "a", "body": "x"}},
		{ID: "ns:b", Labels: []string{"Code"}, Properties: map[string]interface{}{"id": "b", "body": "y"}},
		{ID: "ns:n", Labels: []string{"Code"}, Properties: map[string]interface{}{"id": 7, "body": "z"}},
	} {
		_, err := engine.CreateNode(t.Context(), node)
		require.NoError(t, err)
	}

	stream := func(opts StreamNodesOptions) []*Node {
		t.Helper()
		var visited []*Node
		require.NoError(t, engine.StreamNodesWithOptions(context.Background(), opts, func(node *Node) error {
			visited = append(visited, node)
			return nil
		}))
		return visited
	}

	// The filter tests a property the scan was not asked to project.
	visited := stream(StreamNodesOptions{PropertyFilter: func(props map[string]interface{}) bool {
		return props["body"] == "y"
	}})
	require.Len(t, visited, 1)
	require.Equal(t, NodeID("ns:b"), visited[0].ID)
	require.Equal(t, map[string]interface{}{"id": "b", "body": "y"}, visited[0].Properties,
		"a kept node carries every property when the scan has no projection")

	// A required string is compared with the stored bytes: the integer 7 is
	// not the string "7", and a node without the property is left to the filter.
	require.Equal(t, []NodeID{"ns:a"}, scanIDs(stream(StreamNodesOptions{PropertyStringEquals: map[string]string{"id": "a"}})))
	require.Empty(t, stream(StreamNodesOptions{PropertyStringEquals: map[string]string{"id": "7"}}))

	// A numeric equality is the filter's, and it sees the decoded number.
	require.Equal(t, []NodeID{"ns:n"}, scanIDs(stream(StreamNodesOptions{PropertyFilter: func(props map[string]interface{}) bool {
		return props["id"] == int64(7)
	}})))
}

func scanIDs(nodes []*Node) []NodeID {
	ids := make([]NodeID, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.ID)
	}
	return ids
}

// A record the options scan cannot decode fails the scan. The records streamed
// before it are not a complete population, so the scan cannot report success.
func TestBadgerStreamNodesDecodeErrorPropagates(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	_, err = engine.CreateNode(t.Context(), &Node{ID: "ns:a", Labels: []string{"Code"}, Properties: map[string]interface{}{"id": "a"}})
	require.NoError(t, err)
	require.NoError(t, engine.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
		return txn.Set(nodeKey("ns:z-bad"), []byte("not a node body"))
	}))

	for name, opts := range map[string]StreamNodesOptions{
		"filter without projection": {PropertyFilter: func(map[string]interface{}) bool { return true }},
		"projection":                {Projection: []string{"id"}},
		"query read":                {QueryNodes: true},
		"full read":                 {},
	} {
		t.Run(name, func(t *testing.T) {
			visited := 0
			err := engine.StreamNodesWithOptions(context.Background(), opts, func(*Node) error {
				visited++
				return nil
			})
			require.ErrorContains(t, err, `scanning node "ns:z-bad"`)
			require.ErrorContains(t, err, "unexpected format byte")
			require.Equal(t, 1, visited, "only the decodable node was delivered")
		})
	}
}

// A whole-node query read applies the filter before materializing the node and
// settles the kept node's embedding metadata from the same snapshot: the scan
// succeeds although no stored chunk vector can be read, and the row carries the
// metadata instead of the vectors.
func TestBadgerStreamNodesPropertyFilterQueryMetadata(t *testing.T) {
	for _, tc := range lazyEmbeddingCases() {
		t.Run(tc.name, func(t *testing.T) {
			b := newSidecarTestBadger(t)
			id := NodeID("test:lazy")
			wantChunks := tc.create(t, b, id)
			require.Len(t, wantChunks, tc.chunks)
			corruptFirstEmbeddingChunk(t, b, id)

			var kept *Node
			require.NoError(t, b.StreamNodesWithOptions(context.Background(), StreamNodesOptions{
				QueryNodes: true,
				PropertyFilter: func(props map[string]interface{}) bool {
					return props["title"] == "t"
				},
			}, func(node *Node) error {
				kept = node
				return nil
			}))
			require.NotNil(t, kept)
			require.Equal(t, id, kept.ID)
			require.Equal(t, map[string]interface{}{"title": "t"}, kept.Properties, "the row is the whole node")
			require.True(t, kept.HasChunkEmbeddings())
			require.Equal(t, tc.chunks, chunkCountFromMeta(kept.EmbedMeta))
			require.Empty(t, kept.ChunkEmbeddings)
			require.False(t, kept.RuntimeMetadata().EmbeddingsStoredSeparately)

			// A rejected node costs no metadata settlement.
			visited := 0
			require.NoError(t, b.StreamNodesWithOptions(context.Background(), StreamNodesOptions{
				QueryNodes:     true,
				PropertyFilter: func(props map[string]interface{}) bool { return props["title"] == "other" },
			}, func(*Node) error {
				visited++
				return nil
			}))
			require.Zero(t, visited)
		})
	}
}

// Full reads retain their vector contract while filtering before sidecar work.
// A corrupt vector on a rejected body must not fail the scan; the same vector
// on a kept body must fail rather than silently becoming a metadata-only read.
func TestBadgerStreamNodesPropertyFilterFullEmbeddings(t *testing.T) {
	for _, tc := range lazyEmbeddingCases() {
		t.Run(tc.name, func(t *testing.T) {
			b := newSidecarTestBadger(t)
			id := NodeID("test:full-filter")
			want := tc.create(t, b, id)
			for _, projection := range [][]string{nil, {"title"}} {
				var kept *Node
				err := b.StreamNodesWithOptions(t.Context(), StreamNodesOptions{
					WithEmbeddings: true,
					QueryNodes:     true, // explicit full hydration takes precedence
					Projection:     projection,
					PropertyFilter: func(props map[string]interface{}) bool { return props["title"] == "t" },
				}, func(node *Node) error { kept = node; return nil })
				require.NoError(t, err)
				require.NotNil(t, kept)
				require.Equal(t, id, kept.ID)
				require.Equal(t, want, kept.ChunkEmbeddings)
				require.Equal(t, tc.chunks, chunkCountFromMeta(kept.EmbedMeta))
				require.Equal(t, map[string]interface{}{"title": "t"}, kept.Properties)
			}
			corruptFirstEmbeddingChunk(t, b, id)
			visits := 0
			err := b.StreamNodesWithOptions(t.Context(), StreamNodesOptions{
				WithEmbeddings: true,
				PropertyFilter: func(props map[string]interface{}) bool { return props["title"] == "other" },
			}, func(*Node) error { visits++; return nil })
			require.NoError(t, err)
			require.Zero(t, visits)
			err = b.StreamNodesWithOptions(t.Context(), StreamNodesOptions{
				WithEmbeddings: true,
				PropertyFilter: func(props map[string]interface{}) bool { return props["title"] == "t" },
			}, func(*Node) error { visits++; return nil })
			require.Error(t, err)
			require.Zero(t, visits, "a failed full read cannot deliver a partial node")
		})
	}
}

func TestDecodeNodeRejectsMalformedBodies(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	scan := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{Projection: []string{"id"}})
	filtered := newProjectedNodeDecoder(engine, nil, StreamNodesOptions{PropertyFilter: func(map[string]interface{}) bool { return true }})
	for name, data := range map[string][]byte{
		"empty":          {},
		"format byte":    {0x01},
		"length varint":  {nodeFormatTokenizedV1, 0xff},
		"truncated":      {nodeFormatTokenizedV1, 0x05, 0x01},
		"bad properties": {nodeFormatTokenizedV1, 0x02, 0xff, 0xff},
		"bad body":       {nodeFormatTokenizedV1, 0x01, 0x00, 0xc1},
	} {
		node, err := engine.decodeNodeProjected(nil, "ns", data, nil)
		require.Error(t, err, name)
		require.Nil(t, node, name)
		node, err = scan.decode([]byte("\x01ns:n1"), data)
		require.Error(t, err, name)
		require.Nil(t, node, name)
		node, err = filtered.decode([]byte("\x01ns:n1"), data)
		require.Error(t, err, name)
		require.Nil(t, node, name)
	}
}
