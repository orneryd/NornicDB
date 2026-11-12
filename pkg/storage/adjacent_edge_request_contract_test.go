package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAdjacentEdgeRequestDirectionsTypesAndSelfLoops(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("write-behind-%v", buffered), func(t *testing.T) {
			engine := newTestMemoryEngine(t).BadgerEngine
			if buffered {
				engine = newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
			}
			for _, id := range []NodeID{"test:a", "test:b", "test:c"} {
				_, err := engine.CreateNode(t.Context(), &Node{ID: id})
				require.NoError(t, err)
			}
			for _, edge := range []*Edge{
				{ID: "test:out", StartNode: "test:a", EndNode: "test:b", Type: "R"},
				{ID: "test:in", StartNode: "test:c", EndNode: "test:a", Type: "S"},
				{ID: "test:self", StartNode: "test:a", EndNode: "test:a", Type: "R"},
			} {
				edge.Properties = map[string]interface{}{"weight": int64(7)}
				require.NoError(t, engine.CreateEdge(t.Context(), edge))
			}
			for _, flushed := range []bool{false, true} {
				if flushed {
					if !buffered {
						continue
					}
					require.NoError(t, engine.FlushWriteBehind(t.Context()))
				}
				t.Run(fmt.Sprintf("flushed-%v", flushed), func(t *testing.T) {
					tx, err := engine.BeginTransaction(t.Context())
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, tx.Rollback()) })
					for _, read := range []struct {
						name string
						call func(context.Context, AdjacentEdgeRead) ([]*Edge, error)
					}{
						{"engine", engine.ReadAdjacentEdges},
						{"transaction", tx.ReadEdges},
					} {
						for _, headers := range []bool{false, true} {
							for _, tc := range []struct {
								direction AdjacentEdgeDirection
								types     []string
								want      []string
							}{
								{AdjacentOutgoing, []string{"R"}, []string{"test:out", "test:self"}},
								{AdjacentIncoming, []string{"S"}, []string{"test:in"}},
								{AdjacentBoth, []string{"R", "S"}, []string{"test:in", "test:out", "test:self"}},
								{AdjacentBetween, []string{"R"}, []string{"test:out"}},
								{AdjacentOutgoing, []string{"missing"}, []string{}},
							} {
								edges, err := read.call(t.Context(), AdjacentEdgeRead{Node: "test:a", Other: "test:b", Direction: tc.direction, Types: tc.types, Headers: headers})
								require.NoError(t, err, "%s headers=%v direction=%v", read.name, headers, tc.direction)
								require.Equal(t, tc.want, sortedEdgeIDStrings(edges), "self loops occur once and type alternatives are a union")
								if !headers {
									for _, edge := range edges {
										require.Equal(t, int64(7), edge.Properties["weight"])
									}
								}
							}
						}
					}
				})
			}
		})
	}
}

func TestTypedAdjacentHeadersKeepHistoricalTypeAndEndpoints(t *testing.T) {
	engine := newTestMemoryEngine(t).BadgerEngine
	for _, id := range []NodeID{"test:a", "test:b", "test:c"} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:edge", StartNode: "test:a", EndNode: "test:b", Type: "old"}))
	reader, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Rollback()) })
	request := AdjacentEdgeRead{Node: "test:a", Types: []string{"old"}, Headers: true}
	for phase := 0; phase < 2; phase++ {
		edges, err := reader.ReadEdges(t.Context(), request)
		require.NoError(t, err)
		require.Len(t, edges, 1)
		require.Equal(t, EdgeID("test:edge"), edges[0].ID)
		require.Equal(t, NodeID("test:b"), edges[0].EndNode)
		require.Equal(t, "old", edges[0].Type)
		if phase == 0 {
			before, err := engine.GetEdge(t.Context(), "test:edge")
			require.NoError(t, err)
			after := CopyEdge(before)
			after.Type, after.EndNode = "new", "test:c"
			require.NoError(t, engine.UpdateEdge(t.Context(), EdgeUpdateRequest{Before: before, After: after}))
		}
	}
	fresh, err := engine.ReadAdjacentEdges(t.Context(), AdjacentEdgeRead{Node: "test:a", Types: []string{"new"}, Headers: true})
	require.NoError(t, err)
	require.Len(t, fresh, 1)
	require.Equal(t, NodeID("test:c"), fresh[0].EndNode)
}

// Versioned reads use their registered historical cut, not the current
// write-behind overlay captured by read-committed readers.
func TestVersionedAdjacentReadKeepsHistoricalCutAcrossBufferedUpdate(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:edge", StartNode: "test:a", EndNode: "test:b", Type: "old"}))
	head, err := engine.GetEdgeCurrentHead(t.Context(), "test:edge")
	require.NoError(t, err)
	require.False(t, head.Version.IsZero())
	writer, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer func() { _ = writer.Rollback() }()
	require.NoError(t, writer.SetNamespace("test"))
	require.NoError(t, writer.SetImplicit(true))
	before, err := writer.GetEdge(t.Context(), "test:edge")
	require.NoError(t, err)
	after := CopyEdge(before)
	after.Type = "new"
	require.NoError(t, writer.UpdateEdge(t.Context(), EdgeUpdateRequest{Before: before, After: after}))
	require.NoError(t, writer.Commit())
	for _, headers := range []bool{false, true} {
		req := AdjacentEdgeRead{Node: "test:a", Types: []string{"old"}, Headers: headers}
		historical, err := engine.ReadAdjacentEdgesVisibleAt(t.Context(), req, head.Version)
		require.NoError(t, err)
		require.Equal(t, []string{"test:edge"}, sortedEdgeIDStrings(historical))
		require.Equal(t, "old", historical[0].Type)
		current, err := engine.ReadAdjacentEdges(t.Context(), req)
		require.NoError(t, err)
		require.Empty(t, current)
		req.Types = []string{"new"}
		current, err = engine.ReadAdjacentEdges(t.Context(), req)
		require.NoError(t, err)
		require.Equal(t, []string{"test:edge"}, sortedEdgeIDStrings(current))
	}
}
