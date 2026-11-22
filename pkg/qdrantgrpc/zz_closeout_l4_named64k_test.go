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
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"
)

func TestCloseoutL4Named64KQdrantUpsert(t *testing.T) {
	for _, dims := range []int{768, 1024, 1536, 2048, 3072, 4096} {
		ctx := context.Background()
		base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
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

// A batch whose first points are small and whose last point crosses the
// limit: after the error, are the earlier points written or rolled back?
// Read back by gRPC Get, gRPC Count and a Cypher count over the collection's
// database.
func TestCloseoutL4Named64KQdrantBatchAfterError(t *testing.T) {
	const dims = 2048
	ctx := context.Background()
	base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	dbm, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbm.Close() })
	collections, err := NewDatabaseCollectionStore(dbm, nil)
	require.NoError(t, err)
	cfg := DefaultConfig()
	cfg.AllowVectorMutations = true
	const name = "l4_batch"
	require.NoError(t, collections.Create(ctx, name, dims, qpb.Distance_Cosine))
	svc := NewPointsService(cfg, collections, sharedSearchProvider(), nil)
	point := func(id string, k int) *qpb.PointStruct {
		vectors := map[string]*qpb.Vector{}
		for i := 0; i < k; i++ {
			data := make([]float32, dims)
			for j := range data {
				data[j] = float32((i+1)*(j+3)%97) / 97
			}
			vectors[fmt.Sprintf("v%d", i)] = &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: data}}}
		}
		return &qpb.PointStruct{Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: id}},
			Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vectors{Vectors: &qpb.NamedVectors{Vectors: vectors}}}}
	}
	_, upsertErr := svc.Upsert(ctx, &qpb.UpsertPoints{CollectionName: name, Points: []*qpb.PointStruct{point("small1", 1), point("small2", 1), point("small3", 1), point("big", 7)}})
	ids := []*qpb.PointId{}
	for _, id := range []string{"small1", "small2", "small3", "big"} {
		ids = append(ids, &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: id}})
	}
	got, getErr := svc.Get(ctx, &qpb.GetPoints{CollectionName: name, Ids: ids})
	found := []string{}
	if got != nil {
		for _, p := range got.GetResult() {
			found = append(found, p.GetId().GetUuid())
		}
	}
	exact := true
	counted, countErr := svc.Count(ctx, &qpb.CountPoints{CollectionName: name, Exact: &exact})
	var count uint64
	if counted != nil && counted.GetResult() != nil {
		count = counted.GetResult().GetCount()
	}
	exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(base, name))
	cy, cyErr := exec.Execute(ctx, "MATCH (n) RETURN count(n)", nil)
	cyCount := "?"
	if cy != nil && len(cy.Rows) == 1 {
		cyCount = fmt.Sprint(cy.Rows[0][0])
	}
	st, _ := status.FromError(upsertErr)
	msg := st.Message()
	if len(msg) > 160 {
		msg = msg[:160]
	}
	data, _ := json.Marshal(map[string]any{"item": "CL4-NEW-NAMED-64K", "check": "qdrant-batch-after-error", "pass": true,
		"upsertCode": st.Code().String(), "upsertError": msg, "getFound": found, "getError": fmt.Sprint(getErr),
		"grpcCount": count, "countError": fmt.Sprint(countErr), "cypherCount": cyCount, "cypherError": fmt.Sprint(cyErr)})
	fmt.Fprintf(os.Stdout, "L4_RESULT %s\n", data)
}
