package storage

import (
	"bytes"
	"context"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// A snapshot encodes the entity image storage holds, and the version-keyed
// body cache must return that same image when it substitutes for the decode it
// caches. A copy that turned an absent label list into an empty one made the
// second snapshot of an unchanged store differ from the first: the first wrote
// the label-less node's nil labels, the cache-fed second wrote an empty list.
// Recovery streams and the stored-body transcoder comparison both read those
// frames, so the image a snapshot writes must not depend on which read served
// the node.
func TestSnapshotNodeFramesAreCacheIndependent(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:bare"})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &Node{ID: "test:labelled", Labels: []string{"Doc"}})
	require.NoError(t, err)

	write := func() map[string][]byte {
		var output bytes.Buffer
		require.NoError(t, WriteSnapshot(context.Background(), engine, &output, SnapshotOptions{Sequence: 11}))
		return snapshotFramesByID(t, output.Bytes())
	}
	cold := write()
	warm := write()
	require.Equal(t, cold, warm, "a body-cache hit encodes the same entity image as the decode it replaces")

	decodeNode := func(frames map[string][]byte, key string) *Node {
		t.Helper()
		frame, ok := frames["node:"+key]
		require.True(t, ok, key)
		kind, payload, err := readSnapshotFrame(bytes.NewReader(frame))
		require.NoError(t, err)
		require.Equal(t, streamSnapshotNode, kind)
		var node Node
		require.NoError(t, decodeRecoveryPayload(payload, &node))
		return &node
	}
	bare := decodeNode(cold, "test:bare")
	require.Nil(t, bare.Labels, "an exported node keeps the label list storage holds, absent stays absent")
	require.Equal(t, []string{"Doc"}, decodeNode(cold, "test:labelled").Labels)
}
