package qdrantgrpc

import (
	"context"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPR1655PointMutationDoesNotAcknowledgeFailedObservation(t *testing.T) {
	for _, closed := range []bool{false, true} {
		for _, operation := range []string{"set payload", "overwrite payload", "delete payload", "clear payload", "update vectors", "delete vectors"} {
			name := operation + "/missing"
			if closed {
				name = operation + "/storage unavailable"
			}
			t.Run(name, func(t *testing.T) {
				engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
				t.Cleanup(func() { require.NoError(t, engine.Close()) })
				if closed {
					require.NoError(t, engine.Close())
				}
				// Collection routing has succeeded, then its storage becomes unavailable.
				collections := &fakeCollectionStore{engine: engine, meta: &CollectionMeta{Name: "test", Dimensions: 4, Distance: qpb.Distance_Cosine}}
				config := DefaultConfig()
				config.AllowVectorMutations = true
				service := NewPointsService(config, collections, nil, nil)
				id := &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: 7}}
				selector := &qpb.PointsSelector{PointsSelectorOneOf: &qpb.PointsSelector_Points{Points: &qpb.PointsIdsList{Ids: []*qpb.PointId{id}}}}
				payload := &qpb.SetPayloadPoints{CollectionName: "test", PointsSelector: selector, Payload: map[string]*qpb.Value{"value": {Kind: &qpb.Value_StringValue{StringValue: "new"}}}}
				var response *qpb.PointsOperationResponse
				var err error
				switch operation {
				case "set payload":
					response, err = service.SetPayload(context.Background(), payload)
				case "overwrite payload":
					response, err = service.OverwritePayload(context.Background(), payload)
				case "delete payload":
					response, err = service.DeletePayload(context.Background(), &qpb.DeletePayloadPoints{CollectionName: "test", PointsSelector: selector, Keys: []string{"value"}})
				case "clear payload":
					response, err = service.ClearPayload(context.Background(), &qpb.ClearPayloadPoints{CollectionName: "test", Points: selector})
				case "update vectors":
					response, err = service.UpdateVectors(context.Background(), &qpb.UpdatePointVectors{CollectionName: "test", Points: []*qpb.PointVectors{{Id: id, Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vector{Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}}}}}}})
				case "delete vectors":
					response, err = service.DeleteVectors(context.Background(), &qpb.DeletePointVectors{CollectionName: "test", PointsSelector: selector, Vectors: &qpb.VectorsSelector{Names: []string{"default"}}})
				}
				if !closed {
					require.NoError(t, err, "missing points preserve their no-op mutation contract")
					require.NotNil(t, response)
					require.Equal(t, qpb.UpdateStatus_Completed, response.GetResult().GetStatus())
					return
				}
				require.Error(t, err, "failed storage reads must not be acknowledged as Completed")
				require.Equal(t, codes.Internal, status.Code(err))
				require.Nil(t, response)
			})
		}
	}
}
