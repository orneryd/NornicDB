// Author: Lane #1644 (Codex). Public protobuf service outcomes distinguish
// absent points from failed required storage; completed updates require a write.
package qdrantgrpc

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

type foundationPointFault struct {
	storage.Engine
	failed            storage.NodeID
	readErr, writeErr error
	reads, writes     int
}

func (v *foundationPointFault) GetNode(id storage.NodeID) (*storage.Node, error) {
	if id == v.failed && v.readErr != nil {
		v.reads++
		return nil, v.readErr
	}
	return v.Engine.GetNode(id)
}
func (v *foundationPointFault) UpdateNode(n *storage.Node) error {
	if v.writeErr != nil {
		v.writes++
		return v.writeErr
	}
	return v.Engine.UpdateNode(n)
}

type foundationPointCollections struct {
	CollectionStore
	view storage.Engine
}

func (v foundationPointCollections) Open(ctx context.Context, name string) (storage.Engine, *CollectionMeta, error) {
	_, meta, err := v.CollectionStore.Open(ctx, name)
	return v.view, meta, err
}
func TestFoundationPublicPointOutcome(t *testing.T) {
	ctx := context.Background()
	_, collections, _ := newPointsServiceFixture(t, pointsFixtureEmptyVectors)
	engine, _, err := collections.Open(ctx, "test_vectors")
	require.NoError(t, err)
	id := &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p1"}}
	_, err = engine.CreateNode(&storage.Node{ID: pointIDToNodeID(id), Labels: []string{QdrantPointLabel}, Properties: map[string]any{"tag": "first"}, NamedEmbeddings: map[string][]float32{"default": {1, 0, 0, 0}}})
	require.NoError(t, err)
	view := &foundationPointFault{Engine: engine, failed: pointIDToNodeID(id)}
	service := NewPointsService(DefaultConfig(), foundationPointCollections{collections, view}, nil, nil)
	get := func() error {
		_, err := service.Get(ctx, &qpb.GetPoints{CollectionName: "test_vectors", Ids: []*qpb.PointId{id}})
		return err
	}
	set := func() error {
		_, err := service.SetPayload(ctx, &qpb.SetPayloadPoints{CollectionName: "test_vectors", PointsSelector: &qpb.PointsSelector{PointsSelectorOneOf: &qpb.PointsSelector_Points{Points: &qpb.PointsIdsList{Ids: []*qpb.PointId{id}}}}, Payload: map[string]*qpb.Value{"tag": {Kind: &qpb.Value_StringValue{StringValue: "updated"}}}})
		return err
	}
	healthy, err := service.Get(ctx, &qpb.GetPoints{CollectionName: "test_vectors", Ids: []*qpb.PointId{id}})
	require.NoError(t, err)
	require.Len(t, healthy.Result, 1)
	require.NoError(t, set())
	for _, tc := range []struct {
		name  string
		write bool
		call  func() error
	}{{"get-read", false, get}, {"payload-read", false, set}, {"payload-write", true, set}} {
		t.Run(tc.name, func(t *testing.T) {
			cause := errors.New("unrecovered point operation failed")
			view.readErr, view.writeErr = nil, nil
			if tc.write {
				view.writeErr = cause
			} else {
				view.readErr = cause
			}
			err := tc.call()
			if tc.write {
				require.Positive(t, view.writes)
			} else {
				require.Positive(t, view.reads)
			}
			require.Equal(t, codes.Internal, status.Code(err))
			require.ErrorContains(t, err, cause.Error())
		})
	}
	view.readErr, view.writeErr = nil, nil
	missing, err := service.Get(ctx, &qpb.GetPoints{CollectionName: "test_vectors", Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "missing"}}}})
	require.NoError(t, err)
	require.Empty(t, missing.Result)
}
