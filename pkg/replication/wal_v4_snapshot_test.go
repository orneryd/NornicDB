package replication

import (
	"bytes"
	"encoding/gob"
	"io"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestV4ReplicationSnapshotConversionRestoresHistoricalData(t *testing.T) {
	created := time.Date(2026, 3, 4, 5, 6, 7, 123456789, time.UTC)
	fixture := snapshotPayload{
		WALPosition: 42,
		Nodes:       []*storage.Node{transportContractNode(created, created.Add(time.Second)), {ID: "test:contract-node-2"}},
		Edges:       []*storage.Edge{transportContractEdge(created, created.Add(time.Minute))},
	}
	want := gobSnapshotBaseline(t, fixture)
	var historical bytes.Buffer
	require.NoError(t, gob.NewEncoder(&historical).Encode(fixture))
	var converted bytes.Buffer
	require.NoError(t, TranscodeV4ReplicationSnapshot(bytes.NewReader(historical.Bytes()), &converted))
	ok, err, trailing := singleMessagePackValue(converted.Bytes())
	require.NoError(t, err)
	require.Zero(t, trailing)
	require.True(t, ok)
	var decoded storage.ReplicationSnapshot
	require.NoError(t, storage.DecodeCommandPayload(converted.Bytes(), &decoded))
	require.Equal(t, want, decoded)

	baseline := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	for _, item := range want.Nodes {
		_, err := baseline.CreateNode(item)
		require.NoError(t, err)
	}
	for _, item := range want.Edges {
		require.NoError(t, baseline.CreateEdge(item))
	}
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	adapter, err := NewStorageAdapterWithWAL(engine, t.TempDir())
	require.NoError(t, err)
	fixtureowner.OwnChecked(t, adapter)
	require.NoError(t, adapter.RestoreSnapshot(bytes.NewReader(converted.Bytes())))
	position, err := adapter.GetWALPosition()
	require.NoError(t, err)
	require.Equal(t, uint64(42), position)
	node, err := engine.GetNode(want.Nodes[0].ID)
	require.NoError(t, err)
	wantNode, err := baseline.GetNode(want.Nodes[0].ID)
	require.NoError(t, err)
	// Both engines apply their ordinary stored-value normalization after the
	// exact historical snapshot has been decoded above.
	require.Equal(t, wantNode.Properties, node.Properties)
	require.Equal(t, wantNode.NamedEmbeddings, node.NamedEmbeddings)
	edge, err := engine.GetEdge(want.Edges[0].ID)
	require.NoError(t, err)
	require.Equal(t, want.Edges[0].Properties, edge.Properties)
	require.Equal(t, want.Edges[0].VisibilitySuppressed, edge.VisibilitySuppressed)
}

type shortSnapshotWriter struct{}

func (shortSnapshotWriter) Write(data []byte) (int, error) { return len(data) - 1, nil }

func TestV4ReplicationSnapshotConversionRejectsInvalidInputAndShortWrites(t *testing.T) {
	var valid bytes.Buffer
	require.NoError(t, gob.NewEncoder(&valid).Encode(snapshotPayload{WALPosition: 1}))
	for _, input := range [][]byte{[]byte("invalid"), append(bytes.Clone(valid.Bytes()), 0xff), append(bytes.Clone(valid.Bytes()), 0)} {
		var output bytes.Buffer
		err := TranscodeV4ReplicationSnapshot(bytes.NewReader(input), &output)
		require.Error(t, err)
		require.Empty(t, output.Bytes())
	}
	require.ErrorIs(t, TranscodeV4ReplicationSnapshot(bytes.NewReader(valid.Bytes()), shortSnapshotWriter{}), io.ErrShortWrite)
}
