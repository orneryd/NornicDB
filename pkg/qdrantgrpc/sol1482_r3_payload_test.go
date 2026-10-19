package qdrantgrpc

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
)

func TestSol1482R3PayloadMutationsRefreshSearch(t *testing.T) {
	for _, operation := range []string{"set", "overwrite", "delete", "clear"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			points, _, svc, store := setupConsumerPoints(t)
			pid := &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: 81}}
			initial := consumerUpsert(pid, []float32{1, 0, 0, 0})
			initial.Points[0].Payload = map[string]*qpb.Value{"content": {Kind: &qpb.Value_StringValue{StringValue: "oldword"}}}
			_, err := points.Upsert(ctx, initial)
			require.NoError(t, err)
			selector := &qpb.PointsSelector{PointsSelectorOneOf: &qpb.PointsSelector_Points{Points: &qpb.PointsIdsList{Ids: []*qpb.PointId{pid}}}}
			opts := search.DefaultSearchOptions()
			opts.MinRRFScore = 0
			control, err := svc.Search(ctx, "oldword", nil, opts)
			require.NoError(t, err)
			require.Len(t, control.Results, 1)
			request := &qpb.SetPayloadPoints{CollectionName: "test_vectors", Payload: map[string]*qpb.Value{"content": {Kind: &qpb.Value_StringValue{StringValue: "newword"}}}, PointsSelector: selector}
			switch operation {
			case "set":
				_, err = points.SetPayload(ctx, request)
			case "overwrite":
				_, err = points.OverwritePayload(ctx, request)
			case "delete":
				_, err = points.DeletePayload(ctx, &qpb.DeletePayloadPoints{CollectionName: "test_vectors", Keys: []string{"content"}, PointsSelector: selector})
			case "clear":
				_, err = points.ClearPayload(ctx, &qpb.ClearPayloadPoints{CollectionName: "test_vectors", Points: selector})
			}
			require.NoError(t, err)
			stored, err := store.GetNode(pointIDToNodeID(pid))
			require.NoError(t, err)
			if operation == "set" || operation == "overwrite" {
				require.Equal(t, "newword", stored.Properties["content"])
			} else {
				require.NotContains(t, stored.Properties, "content")
			}
			response, err := svc.Search(ctx, "oldword", nil, opts)
			require.NoError(t, err)
			require.Empty(t, response.Results, "payload operation left the old searchable text indexed")
		})
	}
}
