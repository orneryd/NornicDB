// NOR-312 sidecar maintenance contract: UpdateNodeEmbeddingSidecar belongs to
// the composite capability file's maintenance class. It writes derived
// embedding state through the readable constituent that holds the node and
// never mutates the node record, so it is not admitted by the constituent's
// configured write role. The entity-mutation routes keep that writer admission
// and still refuse a read-only constituent. Both directions are pinned here so
// converging the composite write routes cannot silently move one admission into
// the other.
package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompositeSidecarMaintenanceWritesThroughReadableConstituent(t *testing.T) {
	ctx := context.Background()
	composite, readOnly, writable := newTestCompositeEngine(t, "read")

	_, err := readOnly.CreateNode(ctx, &Node{ID: "held", Labels: []string{"Doc"}, Properties: map[string]any{"text": "derived work"}})
	require.NoError(t, err)

	node, err := composite.GetNode(ctx, "held")
	require.NoError(t, err)
	node.ChunkEmbeddings = [][]float32{{0.25, 0.5}}
	node.EmbedMeta = map[string]any{"embedded": true, "chunk_count": 1}

	require.NoError(t, composite.UpdateNodeEmbeddingSidecar(ctx, node),
		"derived embedding maintenance is admitted by the readable participant that holds the node")

	stored, err := readOnly.GetNode(ctx, "held")
	require.NoError(t, err)
	require.Equal(t, [][]float32{{0.25, 0.5}}, stored.ChunkEmbeddings)
	require.Equal(t, "derived work", stored.Properties["text"], "the node record itself is unchanged")

	_, err = writable.GetNode(ctx, "held")
	require.ErrorIs(t, err, ErrNotFound, "no other participant receives the maintenance write")
}

// sidecarLessEngine is an Engine without the optional embedding sidecar
// capability, standing in for a participant that cannot persist derived
// embeddings (a remote constituent reports the same capability gap).
type sidecarLessEngine struct{ Engine }

func TestCompositeSidecarMaintenanceReportsMissingCapability(t *testing.T) {
	ctx := context.Background()
	composite, readOnly, writable := newTestCompositeEngine(t)

	_, err := readOnly.CreateNode(ctx, &Node{ID: "local", Labels: []string{"Doc"}})
	require.NoError(t, err)
	_, err = writable.CreateNode(ctx, &Node{ID: "remote", Labels: []string{"Doc"}})
	require.NoError(t, err)

	withoutCapability := NewCompositeEngine(
		map[string]Engine{"a": readOnly, "b": &sidecarLessEngine{Engine: writable}},
		map[string]string{"a": "a", "b": "b"},
		map[string]string{"a": "read_write", "b": "read_write"},
	)
	node, err := composite.GetNode(ctx, "remote")
	require.NoError(t, err)
	node.ChunkEmbeddings = [][]float32{{1, 2}}

	err = withoutCapability.UpdateNodeEmbeddingSidecar(ctx, node)
	require.ErrorContains(t, err, "does not support embedding sidecar writes")
	require.ErrorContains(t, err, "sidecarLessEngine", "the error names the participant that lacks the capability")

	stored, err := writable.GetNode(ctx, "remote")
	require.NoError(t, err)
	require.Empty(t, stored.ChunkEmbeddings, "no participant receives an unsupported maintenance write")
}

func TestCompositeEntityMutationStillRefusesReadOnlyConstituent(t *testing.T) {
	ctx := context.Background()
	composite, readOnly, _ := newTestCompositeEngine(t, "read")
	_, err := readOnly.CreateNode(ctx, &Node{ID: "held", Labels: []string{"Doc"}})
	require.NoError(t, err)

	after := &Node{ID: "held", Labels: []string{"Doc"}, Properties: map[string]any{"text": "changed"}}
	err = composite.UpdateNode(ctx, NodeUpdateRequest{After: after})
	require.ErrorContains(t, err, "is read-only",
		"entity mutation keeps writer admission and does not fall back to the readable participant")

	stored, err := readOnly.GetNode(ctx, "held")
	require.NoError(t, err)
	require.NotEqual(t, "changed", stored.Properties["text"])
}
