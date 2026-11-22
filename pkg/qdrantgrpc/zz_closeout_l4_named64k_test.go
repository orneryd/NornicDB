package qdrantgrpc

// v1.5.2 close-out L4, CL4-NEW-NAMED-64K: a Qdrant gRPC Upsert of one point
// with k named vectors of dimension d (the public path that writes named
// embeddings). For each dimension the sweep reports what the client sees for
// every k and the smallest k that fails. Test file only; not for merge.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"
)

func TestCloseoutL4Named64KQdrantUpsert(t *testing.T) {
	for _, dims := range []int{768, 1024, 1536, 2048, 3072, 4096} {
		ctx := context.Background()
		base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
		dbm, err := multidb.NewDatabaseManager(t.Context(), base, nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = dbm.Close() })
		collections, err := NewDatabaseCollectionStore(dbm, nil)
		require.NoError(t, err)
		cfg := DefaultConfig()
		cfg.AllowVectorMutations = true
		name := fmt.Sprintf("l4_named_%d", dims)
		require.NoError(t, collections.Create(ctx, name, dims, qpb.Distance_Cosine))
		svc := NewPointsService(cfg, collections, sharedSearchProvider(), nil)
		firstFail := 0
		outcomes := []string{}
		for k := 1; k <= 16; k++ {
			vectors := map[string]*qpb.Vector{}
			for i := 0; i < k; i++ {
				data := make([]float32, dims)
				for j := range data {
					data[j] = float32((i+1)*(j+3)%97) / 97
				}
				vectors[fmt.Sprintf("v%d", i)] = &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: data}}}
			}
			_, err := svc.Upsert(ctx, &qpb.UpsertPoints{
				CollectionName: name,
				Points: []*qpb.PointStruct{{
					Id:      &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: fmt.Sprintf("p%d", k)}},
					Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vectors{Vectors: &qpb.NamedVectors{Vectors: vectors}}},
					Payload: map[string]*qpb.Value{"title": {Kind: &qpb.Value_StringValue{StringValue: "doc"}}},
				}},
			})
			if err == nil {
				outcomes = append(outcomes, fmt.Sprintf("k=%d ok", k))
				continue
			}
			st, _ := status.FromError(err)
			msg := st.Message()
			if len(msg) > 160 {
				msg = msg[:160]
			}
			outcomes = append(outcomes, fmt.Sprintf("k=%d %s: %s", k, st.Code(), msg))
			if firstFail == 0 {
				firstFail = k
			}
			if k >= firstFail+1 {
				break
			}
		}
		data, _ := json.Marshal(map[string]any{"item": "CL4-NEW-NAMED-64K", "check": fmt.Sprintf("qdrant-upsert/d=%d", dims), "pass": true,
			"dims": dims, "bytesPerVector": dims * 4, "firstFailingCount": firstFail, "outcomes": outcomes})
		fmt.Fprintf(os.Stdout, "L4_RESULT %s\n", data)
	}
}
