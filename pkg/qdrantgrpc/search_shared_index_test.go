package qdrantgrpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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
			return nil, nil
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
	results, err := searchCollection(ctx, svc, dist, vectorName, query, filter, limit, offset, minScore, ef, false, nil)
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

func TestSearchSharedIndexDefaultDropRecreateAndIsolation(t *testing.T) {
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	srv := startSharedIndexServer(t, dbm, base, sharedSearchProvider())
	for _, name := range []string{"replace", "keep"} {
		createCosineCollection(t, ctx, srv.collections, name, 4)
	}
	upsertPoint(t, ctx, srv.points, "replace", 1, unitVector(4, 0), nil)
	upsertPoint(t, ctx, srv.points, "keep", 2, unitVector(4, 0), nil)
	_, err = srv.collections.Delete(ctx, &qpb.DeleteCollection{CollectionName: "replace"})
	require.NoError(t, err)
	createCosineCollection(t, ctx, srv.collections, "replace", 4)
	upsertPoint(t, ctx, srv.points, "replace", 3, unitVector(4, 1), nil)
	require.Equal(t, []uint64{3}, searchIDs(t, ctx, srv.points, &qpb.SearchPoints{CollectionName: "replace", Vector: unitVector(4, 0), Limit: 10}))
	require.Equal(t, []uint64{2}, searchIDs(t, ctx, srv.points, &qpb.SearchPoints{CollectionName: "keep", Vector: unitVector(4, 0), Limit: 10}))
}

func TestSharedIndexProviderFailureIsNotAcknowledged(t *testing.T) {
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	srv := startSharedIndexServer(t, dbm, base, func(string, storage.Engine) (*search.Service, error) { return nil, errors.New("index unavailable") })
	createCosineCollection(t, ctx, srv.collections, "unavailable", 4)
	_, err = srv.points.Upsert(ctx, &qpb.UpsertPoints{CollectionName: "unavailable", Points: []*qpb.PointStruct{{Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: 1}}, Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vector{Vector: &qpb.Vector{Data: unitVector(4, 0)}}}}}})
	require.Error(t, err, "an unavailable index cannot acknowledge indexed points")
	_, err = srv.points.Search(ctx, &qpb.SearchPoints{CollectionName: "unavailable", Vector: unitVector(4, 0), Limit: 1})
	require.Error(t, err, "an unavailable index cannot report a successful empty search")
}

func TestSharedIndexColdDatabaseAndServiceReconstruction(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	base, err := storage.NewBadgerEngine(path)
	require.NoError(t, err)
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	first := startSharedIndexServer(t, dbm, base, sharedSearchProvider())
	createCosineCollection(t, ctx, first.collections, "cold", 4)
	upsertPoint(t, ctx, first.points, "cold", 1, unitVector(4, 0), nil)
	first.stop()
	require.NoError(t, dbm.Close())
	base, err = storage.NewBadgerEngine(path)
	require.NoError(t, err)
	dbm, err = multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbm.Close()) })
	// Neither the database manager nor the provider's service cache survives.
	second := startSharedIndexServer(t, dbm, base, sharedSearchProvider())
	upsertPoint(t, ctx, second.points, "cold", 2, unitVector(4, 1), nil)
	require.Equal(t, []uint64{1}, searchIDs(t, ctx, second.points, &qpb.SearchPoints{CollectionName: "cold", Vector: unitVector(4, 0), Limit: 1}))
}

func TestSharedIndexFixedProviderDoesNotAcknowledgeLostVectors(t *testing.T) {
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	var svc *search.Service
	srv := startSharedIndexServer(t, dbm, base, func(_ string, store storage.Engine) (*search.Service, error) {
		if svc == nil {
			svc = search.NewServiceWithDimensions(store, 8)
			svc.FixVectorDimensions()
		}
		return svc, nil
	})
	createCosineCollection(t, ctx, srv.collections, "fixed", 4)
	_, err = srv.points.Upsert(ctx, &qpb.UpsertPoints{CollectionName: "fixed", Points: []*qpb.PointStruct{{Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: 1}}, Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vector{Vector: &qpb.Vector{Data: unitVector(4, 0)}}}}}})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
	require.ErrorContains(t, err, search.ErrDimensionMismatch.Error())
	// The rejected point was not stored: nothing is left that the index lacks.
	count, err := srv.points.Count(ctx, &qpb.CountPoints{CollectionName: "fixed"})
	require.NoError(t, err)
	require.Zero(t, count.GetResult().GetCount())
}

// A vector write to a collection whose dimension the search service can't
// index is rejected before anything is stored, for Upsert and UpdateVectors
// alike: an existing point keeps its stored vector, and an Upsert of a new
// point stores nothing.
func TestSharedIndexFixedProviderRejectsVectorWritesBeforeStoring(t *testing.T) {
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	var fixed atomic.Bool
	var svc *search.Service
	srv := startSharedIndexServer(t, dbm, base, func(_ string, store storage.Engine) (*search.Service, error) {
		if !fixed.Load() {
			// No search service yet: the point is stored and not indexed.
			return nil, nil
		}
		if svc == nil {
			svc = search.NewServiceWithDimensions(store, 8)
			svc.FixVectorDimensions()
		}
		return svc, nil
	})
	createCosineCollection(t, ctx, srv.collections, "fixed", 4)
	upsertPoint(t, ctx, srv.points, "fixed", 1, unitVector(4, 0), nil)
	fixed.Store(true)

	pointID := func(n uint64) *qpb.PointId { return &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: n}} }
	_, err = srv.points.UpdateVectors(ctx, &qpb.UpdatePointVectors{CollectionName: "fixed", Points: []*qpb.PointVectors{{
		Id:      pointID(1),
		Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vector{Vector: &qpb.Vector{Data: unitVector(4, 1)}}},
	}}})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
	_, err = srv.points.Upsert(ctx, &qpb.UpsertPoints{CollectionName: "fixed", Points: []*qpb.PointStruct{
		{Id: pointID(1), Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vector{Vector: &qpb.Vector{Data: unitVector(4, 2)}}}},
		{Id: pointID(2), Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vector{Vector: &qpb.Vector{Data: unitVector(4, 3)}}}},
	}})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)

	got, err := srv.points.Get(ctx, &qpb.GetPoints{
		CollectionName: "fixed",
		Ids:            []*qpb.PointId{pointID(1), pointID(2)},
		WithVectors:    &qpb.WithVectorsSelector{SelectorOptions: &qpb.WithVectorsSelector_Enable{Enable: true}},
	})
	require.NoError(t, err)
	require.Len(t, got.GetResult(), 1, "the rejected Upsert must not store the new point")
	require.Equal(t, uint64(1), got.GetResult()[0].GetId().GetNum())
	require.Equal(t, unitVector(4, 0), got.GetResult()[0].GetVectors().GetVector().GetDense().GetData(), "the rejected writes must not replace the stored vector")
	require.Zero(t, svc.EmbeddingCount())
}

func TestSharedIndexLazySearchAndQueryWarmBeforeAnswering(t *testing.T) {
	for _, useQuery := range []bool{false, true} {
		t.Run(fmt.Sprint("query=", useQuery), func(t *testing.T) {
			ctx := context.Background()
			base := storage.NewMemoryEngine()
			dbm, err := multidb.NewDatabaseManager(base, nil)
			require.NoError(t, err)
			var svc *search.Service
			var warmed atomic.Bool
			srv := startSharedIndexServer(t, dbm, base, func(_ string, store storage.Engine) (*search.Service, error) {
				if svc == nil {
					svc = search.NewServiceWithDimensions(store, 4)
					svc.SetLazyWarming(true, func() { require.NoError(t, svc.BuildIndexes(context.Background())); warmed.Store(true) })
				}
				return svc, nil
			})
			createCosineCollection(t, ctx, srv.collections, "lazy", 4)
			// Populate storage directly to model an existing database on first read.
			store, err := dbm.GetStorage("lazy")
			require.NoError(t, err)
			_, err = store.CreateNode(&storage.Node{ID: pointIDToNodeID(&qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: 1}}), Labels: []string{QdrantPointLabel}, NamedEmbeddings: map[string][]float32{"default": unitVector(4, 0)}})
			require.NoError(t, err)
			exact := true
			if useQuery {
				response, err := srv.points.Query(ctx, &qpb.QueryPoints{CollectionName: "lazy", Query: &qpb.Query{Variant: &qpb.Query_Nearest{Nearest: &qpb.VectorInput{Variant: &qpb.VectorInput_Dense{Dense: &qpb.DenseVector{Data: unitVector(4, 0)}}}}}, Params: &qpb.SearchParams{Exact: &exact}})
				require.NoError(t, err)
				require.Len(t, response.Result, 1)
				require.Equal(t, uint64(1), response.Result[0].Id.GetNum())
			} else {
				require.Equal(t, []uint64{1}, searchIDs(t, ctx, srv.points, &qpb.SearchPoints{CollectionName: "lazy", Vector: unitVector(4, 0), Limit: 1, Params: &qpb.SearchParams{Exact: &exact}}))
			}
			require.True(t, warmed.Load())
		})
	}
}

func TestSharedIndexDefaultDropRemovesPersistedCoordinates(t *testing.T) {
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	path := t.TempDir() + "/vectors"
	provider := sharedSearchProvider()
	var svc *search.Service
	srv := startSharedIndexServer(t, dbm, base, func(name string, store storage.Engine) (*search.Service, error) {
		var err error
		svc, err = provider(name, store)
		if err == nil {
			svc.SetVectorIndexPath(path)
			svc.SetPersistenceEnabled(true)
		}
		return svc, err
	})
	t.Cleanup(func() {
		if svc != nil {
			require.NoError(t, svc.Close())
		}
	})
	createCosineCollection(t, ctx, srv.collections, "persisted", 4)
	upsertPoint(t, ctx, srv.points, "persisted", 1, unitVector(4, 0), nil)
	svc.PersistIndexesToDisk()
	require.FileExists(t, path+".vec")
	_, err = srv.collections.Delete(ctx, &qpb.DeleteCollection{CollectionName: "persisted"})
	require.NoError(t, err)
	require.NoFileExists(t, path+".vec")
	require.NoFileExists(t, path+".meta")
	createCosineCollection(t, ctx, srv.collections, "persisted", 4)
	upsertPoint(t, ctx, srv.points, "persisted", 2, unitVector(4, 1), nil)
	require.Equal(t, []uint64{2}, searchIDs(t, ctx, srv.points, &qpb.SearchPoints{CollectionName: "persisted", Vector: unitVector(4, 0), Limit: 10}))
}

func TestSharedIndexPersistedScoresUseOriginalMagnitudes(t *testing.T) {
	for _, metric := range []struct {
		distance qpb.Distance
		score    float32
	}{{qpb.Distance_Dot, 25}, {qpb.Distance_Euclid, 1}} {
		t.Run(metric.distance.String(), func(t *testing.T) {
			ctx := context.Background()
			base := storage.NewMemoryEngine()
			dbm, err := multidb.NewDatabaseManager(base, nil)
			require.NoError(t, err)
			var svc *search.Service
			path := t.TempDir() + "/vectors"
			srv := startSharedIndexServer(t, dbm, base, func(_ string, store storage.Engine) (*search.Service, error) {
				if svc == nil {
					svc = search.NewServiceWithDimensions(store, 4)
					svc.SetVectorIndexPath(path)
					svc.SetPersistenceEnabled(true)
					return svc, svc.BuildIndexes(ctx)
				}
				return svc, nil
			})
			t.Cleanup(func() {
				if svc != nil {
					require.NoError(t, svc.Close())
				}
			})
			_, err = srv.collections.Create(ctx, &qpb.CreateCollection{CollectionName: "scores", VectorsConfig: &qpb.VectorsConfig{Config: &qpb.VectorsConfig_Params{Params: &qpb.VectorParams{Size: 4, Distance: metric.distance}}}})
			require.NoError(t, err)
			upsertPoint(t, ctx, srv.points, "scores", 1, []float32{3, 4, 0, 0}, nil)
			require.FileExists(t, path+".vec")
			response, err := srv.points.Search(ctx, &qpb.SearchPoints{CollectionName: "scores", Vector: []float32{3, 4, 0, 0}, Limit: 1})
			require.NoError(t, err)
			require.Len(t, response.Result, 1)
			require.InDelta(t, metric.score, response.Result[0].Score, 1e-6)
		})
	}
}

func TestSharedIndexDropRecreateRetiresCapacityAndText(t *testing.T) {
	for _, lower := range []bool{false, true} {
		t.Run(fmt.Sprint(lower), func(t *testing.T) {
			ctx := context.Background()
			base := storage.NewMemoryEngine()
			dbm, err := multidb.NewDatabaseManager(base, nil)
			require.NoError(t, err)
			var svc *search.Service
			provider := func(_ string, store storage.Engine) (*search.Service, error) {
				if svc == nil {
					svc = search.NewServiceWithDimensionsAndBM25EngineAndOptions(store, 4, "v2", &search.ServiceOptions{VectorMemoryMaxBytes: 32})
					require.NoError(t, svc.BuildIndexes(ctx))
				}
				return svc, nil
			}
			cfg := DefaultConfig()
			cfg.ListenAddr = "127.0.0.1:0"
			cfg.AllowVectorMutations = true
			var srv *Server
			if lower {
				collections, e := NewDatabaseCollectionStore(dbm, nil)
				require.NoError(t, e)
				srv, err = NewServer(cfg, collections, base, provider, nil)
			} else {
				srv, err = NewServerWithDatabaseManager(cfg, dbm, base, provider, nil)
			}
			require.NoError(t, err)
			require.NoError(t, srv.Start())
			defer srv.Stop()
			conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer conn.Close()
			collections := qpb.NewCollectionsClient(conn)
			points := qpb.NewPointsClient(conn)
			createCosineCollection(t, ctx, collections, "retire", 4)
			upsertPoint(t, ctx, points, "retire", 1, unitVector(4, 0), map[string]*qpb.Value{"text": {Kind: &qpb.Value_StringValue{StringValue: "obsoleteword"}}})
			_, err = collections.Delete(ctx, &qpb.DeleteCollection{CollectionName: "retire"})
			require.NoError(t, err)
			createCosineCollection(t, ctx, collections, "retire", 4)
			upsertPoint(t, ctx, points, "retire", 2, unitVector(4, 1), nil)
			require.Equal(t, []uint64{2}, searchIDs(t, ctx, points, &qpb.SearchPoints{CollectionName: "retire", Vector: unitVector(4, 1), Limit: 10}))
			results, err := svc.Search(ctx, "obsoleteword", nil, &search.SearchOptions{Limit: 10})
			require.NoError(t, err)
			require.Empty(t, results.Results)
		})
	}
}
