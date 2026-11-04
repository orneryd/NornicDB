// Author: #1683 implementation lane (Codex), 2026-11-01; ported to release/v1.4.4.
package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type entitySnapshotForwarder struct{ Engine }

func (v entitySnapshotForwarder) GetInnerEngine() Engine { return v.Engine }
func (entitySnapshotForwarder) PreservesEventView()      {}

// Observe every field without asking a failure formatter to walk a live engine
// reached through immutable runtime provenance. Runtime record nilness and all
// of its fields are still checked; Go == also preserves owner/token identities.
// This is diagnostic test-only source, not an exclusion of private differences.
func assertSnapshotNodeImage(t *testing.T, expected, actual *Node) {
	t.Helper()
	require.NotNil(t, expected)
	require.NotNil(t, actual)
	wantState, gotState := expected.runtimeState(), actual.runtimeState()
	t.Logf("SNAPSHOT_NODE_OWNER expected_live=%t captured_live=%t same_owner=%t metadata_equal=%t revision_equal=%t attempt_same=%t publication_equal=%t expected_id=%q captured_id=%q",
		wantState.embeddingOwner != nil, gotState.embeddingOwner != nil, wantState.embeddingOwner == gotState.embeddingOwner,
		wantState.metadata == gotState.metadata, wantState.embeddingRevision == gotState.embeddingRevision,
		wantState.embeddingAttempt == gotState.embeddingAttempt, wantState.embeddingPublication == gotState.embeddingPublication,
		wantState.embeddingOwnerID, gotState.embeddingOwnerID)
	assert.Equal(t, expected.runtime == nil, actual.runtime == nil, "runtime record nilness")
	assert.True(t, wantState == gotState, "every immutable runtime field, owner and token identity must match; see bounded diagnostics above")
	wantBody, gotBody := *expected, *actual
	wantBody.runtime, gotBody.runtime = nil, nil
	require.Equal(t, wantBody, gotBody, "all remaining node fields, including vectors and embedding metadata")
}

// The point reader shares the release stream's hydration, then remains bound
// to that committed view after a later update. Forwarding must preserve IDs,
// namespace and lifetime; request-local/unsupported storage gets no substitute.
func TestEntityReadSnapshotMatchesNativeStream(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, buffered := range []bool{false, true} {
		for _, shape := range []string{"native", "namespace", "wal_namespace", "declared_forwarder"} {
			t.Run(fmt.Sprintf("buffered=%t/%s", buffered, shape), func(t *testing.T) {
				base := newReadSnapshotTestEngine(t, buffered)
				var engine Engine = base
				prefix := "test:"
				if shape != "native" {
					if shape == "wal_namespace" {
						walConfig := DefaultWALConfig()
						walConfig.SyncMode = "immediate"
						wal, err := NewWAL(t.TempDir(), walConfig)
						require.NoError(t, err)
						wrapped := NewWALEngine(engine, wal)
						t.Cleanup(func() { require.NoError(t, wrapped.Close()) })
						engine = wrapped
					}
					engine = NewNamespacedEngine(engine, "test")
					prefix = ""
				}
				// The reference must keep the native stream capability. The declared
				// forwarder intentionally hides legacy StreamingEngine: using it as
				// the reference would select AllNodes and its write-cache embedding
				// provenance instead of the native hydration being compared here.
				nativeReference := engine
				_, streams := nativeReference.(StreamingEngine)
				require.True(t, streams, "the comparison reference must be a native stream")
				if shape == "declared_forwarder" {
					engine = entitySnapshotForwarder{engine}
				}
				id, end, edgeID := NodeID(prefix+"n"), NodeID(prefix+"end"), EdgeID(prefix+"r")
				if buffered {
					bufferedWrite(t, base, func(tx *BadgerTransaction) error {
						for _, key := range []NodeID{"test:n", "test:end"} {
							if _, err := tx.CreateNode(t.Context(), &Node{ID: key, Labels: []string{"Doc"}, Properties: map[string]any{"text": "Old Hotel", "number": int64(17)}}); err != nil {
								return err
							}
						}
						return tx.CreateEdge(t.Context(), &Edge{ID: "test:r", StartNode: "test:n", EndNode: "test:end", Type: "LINK", Properties: map[string]any{"text": "Old Hotel"}})
					})
					require.Positive(t, base.writeBehind.PendingOps(), "the overlay premise must be real")
				} else {
					for _, key := range []NodeID{id, end} {
						_, err := engine.CreateNode(t.Context(), &Node{ID: key, Labels: []string{"Doc"}, Properties: map[string]any{"text": "Old Hotel", "number": int64(17)}})
						require.NoError(t, err)
					}
					require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: edgeID, StartNode: id, EndNode: end, Type: "LINK", Properties: map[string]any{"text": "Old Hotel"}}))
				}
				var expectedNode *Node
				require.NoError(t, StreamNodesWithFallback(t.Context(), nativeReference, StreamChunkSize, func(node *Node) error {
					if node.ID == id {
						expectedNode = node
					}
					return nil
				}))
				var expectedEdge *Edge
				require.NoError(t, StreamEdgesWithFallback(t.Context(), nativeReference, StreamChunkSize, func(edge *Edge) error {
					if edge.ID == edgeID {
						expectedEdge = edge
					}
					return nil
				}))
				nodes, err := OpenEntityReadSnapshot(t.Context(), engine, false)
				require.NoError(t, err)
				defer nodes.Close()
				edges, err := OpenEntityReadSnapshot(t.Context(), engine, true)
				require.NoError(t, err)
				defer edges.Close()
				if shape == "native" {
					require.Nil(t, nodes.(*badgerReadSnapshot).overlayEdges, "node capture must not construct relationship images")
					require.Nil(t, edges.(*badgerReadSnapshot).overlayNodes, "relationship capture must not construct node images")
				}
				actualNode, err := nodes.ReadNode(t.Context(), id)
				require.NoError(t, err)
				assertSnapshotNodeImage(t, expectedNode, actualNode)
				actualEdge, err := edges.ReadEdge(t.Context(), edgeID)
				require.NoError(t, err)
				require.Equal(t, expectedEdge, actualEdge)
				require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: id, Labels: []string{"Changed"}, Properties: map[string]any{"text": "new"}}}))
				require.NoError(t, engine.UpdateEdge(t.Context(), EdgeUpdateRequest{After: &Edge{ID: edgeID, StartNode: id, EndNode: end, Type: "LINK", Properties: map[string]any{"text": "new"}}}))
				actualNode, err = nodes.ReadNode(t.Context(), id)
				require.NoError(t, err)
				assertSnapshotNodeImage(t, expectedNode, actualNode)
				actualEdge, err = edges.ReadEdge(t.Context(), edgeID)
				require.NoError(t, err)
				require.Equal(t, expectedEdge, actualEdge)
				cancelled, cancel := context.WithCancel(t.Context())
				cancel()
				_, err = nodes.ReadNode(cancelled, id)
				require.ErrorIs(t, err, context.Canceled)
				nodes.Close()
				_, err = nodes.ReadNode(t.Context(), id)
				require.ErrorIs(t, err, ErrStorageClosed)
			})
		}
	}
}
