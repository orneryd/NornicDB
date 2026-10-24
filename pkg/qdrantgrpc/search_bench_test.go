package qdrantgrpc

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"net"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type benchServer struct {
	srv   *Server
	conn  *grpc.ClientConn
	colls qpb.CollectionsClient
	pts   qpb.PointsClient
}

func startBenchServer(b testing.TB) *benchServer {
	b.Helper()

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.EnableReflection = false
	cfg.AllowVectorMutations = true

	base := runtimeFixtureOwner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	b.Cleanup(func() { _ = base.Close() })

	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(b, err)

	srv, err := NewServerWithDatabaseManager(cfg, dbm, base, nil, nil)
	require.NoError(b, err)
	require.NoError(b, srv.Start())
	b.Cleanup(srv.Stop)

	// grpc.NewClient is used elsewhere in repo; keep consistent.
	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(b, err)
	b.Cleanup(func() { _ = conn.Close() })

	return &benchServer{
		srv:   srv,
		conn:  conn,
		colls: qpb.NewCollectionsClient(conn),
		pts:   qpb.NewPointsClient(conn),
	}
}

func loadBenchCollection(b testing.TB, bs *benchServer, collection string, dim int, points int) {
	b.Helper()

	ctx := context.Background()

	_, err := bs.colls.Create(ctx, &qpb.CreateCollection{
		CollectionName: collection,
		VectorsConfig: &qpb.VectorsConfig{
			Config: &qpb.VectorsConfig_Params{
				Params: &qpb.VectorParams{Size: uint64(dim), Distance: qpb.Distance_Cosine},
			},
		},
	})
	require.NoError(b, err)

	const batchSize = 1000
	for start := 0; start < points; start += batchSize {
		end := start + batchSize
		if end > points {
			end = points
		}

		batch := make([]*qpb.PointStruct, 0, end-start)
		for i := start; i < end; i++ {
			vec := make([]float32, dim)
			// Deterministic sparse-ish vector to keep allocations simple and stable.
			vec[i%dim] = 1
			vec[(i*7)%dim] = 0.5

			batch = append(batch, &qpb.PointStruct{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: fmt.Sprintf("p%d", i)}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{
							Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: vec}},
						},
					},
				},
				Payload: map[string]*qpb.Value{
					"i": {Kind: &qpb.Value_IntegerValue{IntegerValue: int64(i)}},
				},
			})
		}

		_, err := bs.pts.Upsert(ctx, &qpb.UpsertPoints{
			CollectionName: collection,
			Points:         batch,
		})
		require.NoError(b, err)
	}
}

func BenchmarkGRPCSearch_WithPayload(b *testing.B) {
	bs := startBenchServer(b)

	const (
		collection = "bench_col"
		dim        = 128
		points     = 20_000
		k          = 10
	)
	loadBenchCollection(b, bs, collection, dim, points)

	ctx := context.Background()
	query := make([]float32, dim)
	query[0] = 1
	query[7] = 0.5

	req := &qpb.SearchPoints{
		CollectionName: collection,
		Vector:         query,
		Limit:          k,
		WithPayload:    &qpb.WithPayloadSelector{SelectorOptions: &qpb.WithPayloadSelector_Enable{Enable: true}},
	}

	// Avoid measuring client-side connection setup.
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := bs.pts.Search(ctx, req)
			if err != nil {
				b.Fatal(err)
			}
			if len(resp.GetResult()) == 0 {
				b.Fatal("empty result")
			}
		}
	})
}

// Ensure port binding errors are surfaced early on platforms with aggressive reuse.
func TestBenchServer_Starts(t *testing.T) {
	// Quick sanity: can bind an ephemeral port.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	_ = l.Close()
}
