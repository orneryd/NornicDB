package qdrantgrpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// sharedSearchProvider hands out one search service per database, built from
// what the database already stores, as the server's provider does
// (pkg/server/server_qdrantgrpc.go via DB.GetOrCreateSearchService): Cypher and
// the Qdrant API of a database search the same index.
func sharedSearchProvider() SearchServiceProvider {
	var mu sync.Mutex
	services := map[string]*search.Service{}
	return func(database string, store storage.Engine) (*search.Service, error) {
		mu.Lock()
		defer mu.Unlock()
		svc, ok := services[database]
		if !ok {
			svc = search.NewService(store)
			if err := svc.BuildIndexes(context.Background()); err != nil {
				return nil, err
			}
			services[database] = svc
		}
		return svc, nil
	}
}

type sharedIndexServer struct {
	points      qpb.PointsClient
	collections qpb.CollectionsClient
	stop        func()
}

func startSharedIndexServer(t *testing.T, dbm *multidb.DatabaseManager, base storage.Engine, provider SearchServiceProvider) sharedIndexServer {
	t.Helper()
	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.EnableReflection = false
	cfg.AllowVectorMutations = true
	srv, err := NewServerWithDatabaseManager(cfg, dbm, base, provider, nil)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	stop := func() {
		_ = conn.Close()
		srv.Stop()
	}
	t.Cleanup(stop)
	return sharedIndexServer{points: qpb.NewPointsClient(conn), collections: qpb.NewCollectionsClient(conn), stop: stop}
}

func unitVector(dims, axis int) []float32 {
	v := make([]float32, dims)
	v[axis%dims] = 1
	return v
}

func upsertPoint(t *testing.T, ctx context.Context, points qpb.PointsClient, collection string, id uint64, vec []float32, payload map[string]*qpb.Value) {
	t.Helper()
	_, err := points.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: collection,
		Points: []*qpb.PointStruct{{
			Id:      &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: id}},
			Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vector{Vector: &qpb.Vector{Data: vec}}},
			Payload: payload,
		}},
	})
	require.NoError(t, err)
}

func createCosineCollection(t *testing.T, ctx context.Context, collections qpb.CollectionsClient, name string, dims uint64) {
	t.Helper()
	_, err := collections.Create(ctx, &qpb.CreateCollection{
		CollectionName: name,
		VectorsConfig: &qpb.VectorsConfig{Config: &qpb.VectorsConfig_Params{
			Params: &qpb.VectorParams{Size: dims, Distance: qpb.Distance_Cosine},
		}},
	})
	require.NoError(t, err)
}

func searchIDs(t *testing.T, ctx context.Context, points qpb.PointsClient, req *qpb.SearchPoints) []uint64 {
	t.Helper()
	resp, err := points.Search(ctx, req)
	require.NoError(t, err)
	ids := make([]uint64, 0, len(resp.GetResult()))
	for _, p := range resp.GetResult() {
		ids = append(ids, p.GetId().GetNum())
	}
	return ids
}

// Points upserted through one server instance stay searchable after another
// instance starts: the database's search index holds them, not memory owned by
// the gRPC server (#1370).
func TestSearch_SharedIndex_PointsSurviveServerRestart(t *testing.T) {
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	provider := sharedSearchProvider()

	first := startSharedIndexServer(t, dbm, base, provider)
	createCosineCollection(t, ctx, first.collections, "restart", 4)
	upsertPoint(t, ctx, first.points, "restart", 1, unitVector(4, 0), nil)
	upsertPoint(t, ctx, first.points, "restart", 2, unitVector(4, 1), nil)
	first.stop()

	second := startSharedIndexServer(t, dbm, base, provider)
	upsertPoint(t, ctx, second.points, "restart", 3, unitVector(4, 2), nil)

	ids := searchIDs(t, ctx, second.points, &qpb.SearchPoints{CollectionName: "restart", Vector: unitVector(4, 0), Limit: 1})
	require.Equal(t, []uint64{1}, ids, "point 1 was upserted before the restart and is the exact match")
}

// A payload filter that admits a single point far from the query still finds
// it: candidates are filtered inside the search's widening loop, not cut to a
// fixed overfetch first (#1370).
func TestSearch_SharedIndex_SelectiveFilterFindsDistantMatch(t *testing.T) {
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	srv := startSharedIndexServer(t, dbm, base, sharedSearchProvider())

	const dims = 8
	createCosineCollection(t, ctx, srv.collections, "filtered", dims)
	query := unitVector(dims, 0)
	for i := uint64(1); i <= 300; i++ {
		vec := unitVector(dims, 0)
		vec[1] = float32(i) / 1000 // all close to the query
		upsertPoint(t, ctx, srv.points, "filtered", i, vec, map[string]*qpb.Value{"tag": {Kind: &qpb.Value_StringValue{StringValue: "common"}}})
	}
	// The only "rare" point points away from the query.
	upsertPoint(t, ctx, srv.points, "filtered", 1000, unitVector(dims, 5), map[string]*qpb.Value{"tag": {Kind: &qpb.Value_StringValue{StringValue: "rare"}}})

	ids := searchIDs(t, ctx, srv.points, &qpb.SearchPoints{
		CollectionName: "filtered",
		Vector:         query,
		Limit:          1,
		Filter: &qpb.Filter{Must: []*qpb.Condition{{ConditionOneOf: &qpb.Condition_Field{Field: &qpb.FieldCondition{
			Key: "tag", Match: &qpb.Match{MatchValue: &qpb.Match_Keyword{Keyword: "rare"}},
		}}}}},
	})
	require.Equal(t, []uint64{1000}, ids)
}

// A database whose vector search is off answers a Qdrant search the way it
// answers Cypher: with no hits. Nothing outside the search service keeps
// serving the vectors (#1370).
func TestSearch_SharedIndex_VectorSearchDisabledReturnsNoHits(t *testing.T) {
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	enabled := sharedSearchProvider()
	var disabled sync.Map
	provider := func(database string, store storage.Engine) (*search.Service, error) {
		if _, off := disabled.Load(database); off {
			return nil, errors.New("vector search is disabled for database " + database)
		}
		return enabled(database, store)
	}
	srv := startSharedIndexServer(t, dbm, base, provider)
	createCosineCollection(t, ctx, srv.collections, "off", 4)
	for i := uint64(1); i <= 3; i++ {
		upsertPoint(t, ctx, srv.points, "off", i, unitVector(4, int(i)), nil)
	}
	disabled.Store("off", true)

	ids := searchIDs(t, ctx, srv.points, &qpb.SearchPoints{CollectionName: "off", Vector: unitVector(4, 1), Limit: 3})
	require.Empty(t, ids, fmt.Sprintf("vector search is off for the database, got %v", ids))
}

func mustSearchCollection(t *testing.T, ctx context.Context, svc *search.Service, dist qpb.Distance, vectorName string, query []float32, filter *qpb.Filter, limit, offset int, minScore float64, ef int) []searchResult {
	t.Helper()
	results, err := searchCollection(ctx, svc, dist, vectorName, query, filter, limit, offset, minScore, ef)
	require.NoError(t, err)
	return results
}

// A Qdrant search's ef is its hnsw_ef or, without one, the configured preset's
// efSearch, as Qdrant defaults it; never less than the results it returns.
func TestQdrantSearchEf(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "balanced") // efSearch 100
	t.Setenv("NORNICDB_VECTOR_HNSW_EF_SEARCH", "")
	require.Equal(t, 100, qdrantSearchEf(0, 10), "no hnsw_ef: the preset's efSearch")
	require.Equal(t, 32, qdrantSearchEf(32, 10), "the request's hnsw_ef")
	require.Equal(t, 40, qdrantSearchEf(16, 40), "at least the results to return")
	require.Equal(t, 500, qdrantSearchEf(0, 500))
}
