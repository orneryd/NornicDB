package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNOR178ReadSnapshotRetainsBodiesAcrossDropRecreate(t *testing.T) {
	b := newReadSnapshotTestEngine(t, false)
	for id, value := range map[NodeID]string{"d7:n": "Old", "peer:n": "Peer"} {
		_, err := b.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": value}})
		require.NoError(t, err)
	}
	old, err := b.OpenReadSnapshot(t.Context())
	require.NoError(t, err)
	defer old.Close()
	want := map[NodeID]string{"d7:n": "Old", "peer:n": "Peer"}
	require.Equal(t, want, snapshotNodeText(t, old))
	pr1667DropPrefix(t, b, "d7:")
	require.Equal(t, want, snapshotNodeText(t, old), "drop must preserve the old view")
	_, err = b.CreateNode(t.Context(), &Node{ID: "d7:n", Labels: []string{"Replacement"}, Properties: map[string]any{"text": "New"}})
	require.NoError(t, err)
	require.Equal(t, want, snapshotNodeText(t, old), "recreation must not replace or omit the old body")
	current, err := b.OpenReadSnapshot(t.Context())
	require.NoError(t, err)
	defer current.Close()
	require.Equal(t, map[NodeID]string{"d7:n": "New", "peer:n": "Peer"}, snapshotNodeText(t, current))
}
