package qdrantgrpc

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"math"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Quantized scores belong to the published graph. First-use requests may
// legitimately return exact scores while the service-owned graph is building.
// Base readiness can precede that graph, so wait for the active strategy too.
func requireCollectionHNSWReady(t *testing.T, ctx context.Context, dbm *multidb.DatabaseManager, provider SearchServiceProvider, points qpb.PointsClient, name string, vector []float32) {
	t.Helper()
	store, err := dbm.GetStorage(name)
	require.NoError(t, err)
	svc, err := provider(name, store)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err := points.Search(ctx, &qpb.SearchPoints{CollectionName: name, Vector: vector, Limit: 1})
		require.NoError(c, err)
		require.True(c, svc.IsReady())
		require.Equal(c, "HNSW", svc.CurrentStrategy())
	}, 10*time.Second, time.Millisecond, "collection must publish HNSW before quantized scores are asserted")
}

func TestQuantizationControlsReachPublicSearchAndQuery(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", "binary")
	ctx := context.Background()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	provider := sharedSearchProvider()
	srv := startSharedIndexServer(t, dbm, base, provider)
	createCosineCollection(t, ctx, srv.collections, "quantized", 2)
	upsertPoint(t, ctx, srv.points, "quantized", 1, []float32{1, 0.1}, nil)
	upsertPoint(t, ctx, srv.points, "quantized", 2, []float32{-1, -0.1}, nil)
	requireCollectionHNSWReady(t, ctx, dbm, provider, srv.points, "quantized", []float32{1, 1})
	yes, no := true, false
	over := 2.0
	exactScore := float64(1.1) / math.Sqrt(2*1.01)
	for _, queryAPI := range []bool{false, true} {
		for _, tc := range []struct {
			name   string
			params *qpb.SearchParams
			want   float64
		}{
			{"rescore", &qpb.SearchParams{Quantization: &qpb.QuantizationSearchParams{Rescore: &yes, Oversampling: &over}}, exactScore},
			{"approximate", &qpb.SearchParams{Quantization: &qpb.QuantizationSearchParams{Rescore: &no}}, 1},
			{"ignore", &qpb.SearchParams{Quantization: &qpb.QuantizationSearchParams{Ignore: &yes, Rescore: &no}}, exactScore},
			{"exact", &qpb.SearchParams{Exact: &yes, Quantization: &qpb.QuantizationSearchParams{Rescore: &no}}, exactScore},
		} {
			t.Run(tc.name+map[bool]string{false: "/Search", true: "/Query"}[queryAPI], func(t *testing.T) {
				var result []*qpb.ScoredPoint
				if queryAPI {
					response, e := srv.points.Query(ctx, &qpb.QueryPoints{CollectionName: "quantized", Limit: ptrU64(1), Params: tc.params, Query: &qpb.Query{Variant: &qpb.Query_Nearest{Nearest: &qpb.VectorInput{Variant: &qpb.VectorInput_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 1}}}}}}})
					require.NoError(t, e)
					result = response.Result
				} else {
					response, e := srv.points.Search(ctx, &qpb.SearchPoints{CollectionName: "quantized", Vector: []float32{1, 1}, Limit: 1, Params: tc.params})
					require.NoError(t, e)
					result = response.Result
				}
				require.Len(t, result, 1)
				require.Equal(t, uint64(1), result[0].Id.GetNum())
				require.InDelta(t, tc.want, result[0].Score, 1e-6)
			})
		}
	}
	for _, value := range []float64{0, 0.5, -1, math.NaN(), math.Inf(1)} {
		_, err := srv.points.Search(ctx, &qpb.SearchPoints{CollectionName: "quantized", Vector: []float32{1, 1}, Limit: 1, Params: &qpb.SearchParams{Quantization: &qpb.QuantizationSearchParams{Oversampling: &value}}})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}
}

func TestQuantizedCollectionConfigSurvivesServerRestart(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", "none")
	ctx := context.Background()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	first := startSharedIndexServer(t, dbm, base, sharedSearchProvider())
	config := &qpb.QuantizationConfig{Quantization: &qpb.QuantizationConfig_Binary{Binary: &qpb.BinaryQuantization{QueryEncoding: &qpb.BinaryQuantizationQueryEncoding{Variant: &qpb.BinaryQuantizationQueryEncoding_Setting_{Setting: qpb.BinaryQuantizationQueryEncoding_Scalar8Bits}}}}}
	_, err = first.collections.Create(ctx, &qpb.CreateCollection{CollectionName: "compressed", QuantizationConfig: config,
		VectorsConfig: &qpb.VectorsConfig{Config: &qpb.VectorsConfig_Params{Params: &qpb.VectorParams{Size: 2, Distance: qpb.Distance_Cosine}}}})
	require.NoError(t, err)
	upsertPoint(t, ctx, first.points, "compressed", 1, []float32{1, .1}, nil)
	upsertPoint(t, ctx, first.points, "compressed", 2, []float32{-1, -.1}, nil)
	first.stop()
	second := startSharedIndexServer(t, dbm, base, sharedSearchProvider())
	info, err := second.collections.Get(ctx, &qpb.GetCollectionInfoRequest{CollectionName: "compressed"})
	require.NoError(t, err)
	require.NotNil(t, info.Result.Config.QuantizationConfig.GetBinary())
	no := false
	hits, err := second.points.Search(ctx, &qpb.SearchPoints{CollectionName: "compressed", Vector: []float32{.9, .1}, Limit: 1, Params: &qpb.SearchParams{Quantization: &qpb.QuantizationSearchParams{Rescore: &no}}})
	require.NoError(t, err)
	require.Len(t, hits.Result, 1)
	require.True(t, proto.Equal(config, info.Result.Config.QuantizationConfig))
	want := float64(141) * (.9 / math.Sqrt(.82)) / 127 / math.Sqrt(2)
	require.InDelta(t, want, hits.Result[0].Score, 1e-6, "persisted collection configuration must activate binary scoring with the global mode disabled")
	// Updates must not acknowledge an unapplied compression change.
	_, err = second.collections.Update(ctx, &qpb.UpdateCollection{CollectionName: "compressed", QuantizationConfig: &qpb.QuantizationConfigDiff{Quantization: &qpb.QuantizationConfigDiff_Scalar{Scalar: &qpb.ScalarQuantization{Type: qpb.QuantizationType_Int8}}}})
	require.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestCollectionQuantizationModesAndValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config *qpb.QuantizationConfig
		mode   string
		ratio  int
	}{
		{"scalar", &qpb.QuantizationConfig{Quantization: &qpb.QuantizationConfig_Scalar{Scalar: &qpb.ScalarQuantization{Type: qpb.QuantizationType_Int8}}}, "scalar", 0},
		{"binary", &qpb.QuantizationConfig{Quantization: &qpb.QuantizationConfig_Binary{Binary: &qpb.BinaryQuantization{}}}, "binary", 0},
		{"pq", &qpb.QuantizationConfig{Quantization: &qpb.QuantizationConfig_Product{Product: &qpb.ProductQuantization{Compression: qpb.CompressionRatio_x16}}}, "pq", 16},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := searchQuantizationConfig(tc.config)
			require.NoError(t, err)
			require.Equal(t, tc.mode, string(cfg.Mode))
			require.Equal(t, tc.ratio, cfg.PQCompression)
			base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			dbm, err := multidb.NewDatabaseManager(base, nil)
			require.NoError(t, err)
			store, err := NewDatabaseCollectionStore(dbm, nil)
			require.NoError(t, err)
			require.NoError(t, store.(quantizedCollectionStore).CreateWithQuantization(context.Background(), tc.name, 1024, qpb.Distance_Cosine, tc.config))
			reopened, err := NewDatabaseCollectionStore(dbm, nil)
			require.NoError(t, err)
			meta, err := reopened.GetMeta(context.Background(), tc.name)
			require.NoError(t, err)
			require.True(t, proto.Equal(tc.config, meta.Quantization))
		})
	}
	quantile := float32(.99)
	_, err := searchQuantizationConfig(&qpb.QuantizationConfig{Quantization: &qpb.QuantizationConfig_Scalar{Scalar: &qpb.ScalarQuantization{Type: qpb.QuantizationType_Int8, Quantile: &quantile}}})
	require.Error(t, err)
	_, err = searchQuantizationConfig(&qpb.QuantizationConfig{})
	require.Error(t, err)
	_, err = searchQuantizationConfig(&qpb.QuantizationConfig{Quantization: &qpb.QuantizationConfig_Product{Product: &qpb.ProductQuantization{Compression: qpb.CompressionRatio(999)}}})
	require.Error(t, err)
	_, err = collectionQuantizationConfig(&qpb.CreateCollection{VectorsConfig: &qpb.VectorsConfig{Config: &qpb.VectorsConfig_ParamsMap{ParamsMap: &qpb.VectorParamsMap{Map: map[string]*qpb.VectorParams{
		"one": {QuantizationConfig: &qpb.QuantizationConfig{Quantization: &qpb.QuantizationConfig_Binary{Binary: &qpb.BinaryQuantization{}}}}, "two": {},
	}}}}})
	require.Error(t, err)
}

func TestBinaryUnsupportedStorageOrQueryEncodingFailsExplicitly(t *testing.T) {
	twoBits := qpb.BinaryQuantizationEncoding_TwoBits
	for _, binary := range []*qpb.BinaryQuantization{
		{Encoding: &twoBits},
		{QueryEncoding: &qpb.BinaryQuantizationQueryEncoding{Variant: &qpb.BinaryQuantizationQueryEncoding_Setting_{Setting: qpb.BinaryQuantizationQueryEncoding_Setting(999)}}},
	} {
		_, err := searchQuantizationConfig(&qpb.QuantizationConfig{Quantization: &qpb.QuantizationConfig_Binary{Binary: binary}})
		require.Error(t, err)
	}
}

func TestCollectionQuantizationProtocolPrecisionAndOverrides(t *testing.T) {
	for _, tc := range []struct {
		ratio qpb.CompressionRatio
		want  int
	}{{qpb.CompressionRatio_x4, 4}, {qpb.CompressionRatio_x8, 8}, {qpb.CompressionRatio_x16, 16}, {qpb.CompressionRatio_x32, 32}, {qpb.CompressionRatio_x64, 64}} {
		cfg, err := searchQuantizationConfig(&qpb.QuantizationConfig{Quantization: &qpb.QuantizationConfig_Product{Product: &qpb.ProductQuantization{Compression: tc.ratio}}})
		require.NoError(t, err)
		require.Equal(t, tc.want, cfg.PQCompression)
	}
	for _, tc := range []struct {
		encoding qpb.BinaryQuantizationQueryEncoding_Setting
		want     int
	}{{qpb.BinaryQuantizationQueryEncoding_Default, 1}, {qpb.BinaryQuantizationQueryEncoding_Binary, 1}, {qpb.BinaryQuantizationQueryEncoding_Scalar4Bits, 4}, {qpb.BinaryQuantizationQueryEncoding_Scalar8Bits, 8}} {
		cfg, err := searchQuantizationConfig(&qpb.QuantizationConfig{Quantization: &qpb.QuantizationConfig_Binary{Binary: &qpb.BinaryQuantization{QueryEncoding: &qpb.BinaryQuantizationQueryEncoding{Variant: &qpb.BinaryQuantizationQueryEncoding_Setting_{Setting: tc.encoding}}}}})
		require.NoError(t, err)
		require.Equal(t, tc.want, cfg.BinaryQueryBits)
	}
	scalar := &qpb.QuantizationConfig{Quantization: &qpb.QuantizationConfig_Scalar{Scalar: &qpb.ScalarQuantization{Type: qpb.QuantizationType_Int8}}}
	binary := &qpb.QuantizationConfig{Quantization: &qpb.QuantizationConfig_Binary{Binary: &qpb.BinaryQuantization{}}}
	cfg, err := collectionQuantizationConfig(&qpb.CreateCollection{QuantizationConfig: binary, VectorsConfig: &qpb.VectorsConfig{Config: &qpb.VectorsConfig_Params{Params: &qpb.VectorParams{QuantizationConfig: scalar}}}})
	require.NoError(t, err)
	require.True(t, proto.Equal(scalar, cfg))
	cfg, err = collectionQuantizationConfig(&qpb.CreateCollection{QuantizationConfig: scalar, VectorsConfig: &qpb.VectorsConfig{Config: &qpb.VectorsConfig_ParamsMap{ParamsMap: &qpb.VectorParamsMap{Map: map[string]*qpb.VectorParams{"one": {}, "two": {QuantizationConfig: scalar}}}}}})
	require.NoError(t, err)
	require.True(t, proto.Equal(scalar, cfg))
	for _, cfg := range []*qpb.QuantizationConfig{
		{Quantization: &qpb.QuantizationConfig_Scalar{}},
		{Quantization: &qpb.QuantizationConfig_Scalar{Scalar: &qpb.ScalarQuantization{Type: qpb.QuantizationType(999)}}},
		{Quantization: &qpb.QuantizationConfig_Binary{}},
		{Quantization: &qpb.QuantizationConfig_Product{}},
	} {
		_, err := searchQuantizationConfig(cfg)
		require.Error(t, err)
	}
}

func TestQuantizedDropRecreateUsesDefaultAndPreservesOtherCollection(t *testing.T) {
	for _, global := range []string{"none", "scalar"} {
		t.Run(global, func(t *testing.T) {
			t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
			t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
			t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
			t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", global)
			ctx := context.Background()
			base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			dbm, err := multidb.NewDatabaseManager(base, nil)
			require.NoError(t, err)
			provider := sharedSearchProvider()
			srv := startSharedIndexServer(t, dbm, base, provider)
			for _, name := range []string{"replace", "keep"} {
				_, err := srv.collections.Create(ctx, &qpb.CreateCollection{CollectionName: name,
					QuantizationConfig: &qpb.QuantizationConfig{Quantization: &qpb.QuantizationConfig_Binary{Binary: &qpb.BinaryQuantization{}}},
					VectorsConfig:      &qpb.VectorsConfig{Config: &qpb.VectorsConfig_Params{Params: &qpb.VectorParams{Size: 4, Distance: qpb.Distance_Cosine}}}})
				require.NoError(t, err)
			}
			upsertPoint(t, ctx, srv.points, "replace", 1, []float32{1, 0, 0, 0}, nil)
			upsertPoint(t, ctx, srv.points, "keep", 2, []float32{1, 0, 0, 0}, nil)
			for _, name := range []string{"replace", "keep"} {
				requireCollectionHNSWReady(t, ctx, dbm, provider, srv.points, name, []float32{1, 1, 0, 0})
			}
			no := false
			score := func(name string, id uint64, want float64) {
				response, err := srv.points.Search(ctx, &qpb.SearchPoints{CollectionName: name, Vector: []float32{1, 1, 0, 0}, Limit: 1,
					Params: &qpb.SearchParams{Quantization: &qpb.QuantizationSearchParams{Rescore: &no}}})
				require.NoError(t, err)
				require.Len(t, response.Result, 1)
				require.Equal(t, id, response.Result[0].Id.GetNum())
				require.InDelta(t, want, response.Result[0].Score, 1e-6)
			}
			score("replace", 1, .5)
			score("keep", 2, .5)
			store, err := dbm.GetStorage("replace")
			require.NoError(t, err)
			before, err := provider("replace", store)
			require.NoError(t, err)
			_, err = srv.collections.Delete(ctx, &qpb.DeleteCollection{CollectionName: "replace"})
			require.NoError(t, err)
			createCosineCollection(t, ctx, srv.collections, "replace", 4)
			upsertPoint(t, ctx, srv.points, "replace", 3, []float32{1, 0, 0, 0}, nil)
			requireCollectionHNSWReady(t, ctx, dbm, provider, srv.points, "replace", []float32{1, 1, 0, 0})
			store, err = dbm.GetStorage("replace")
			require.NoError(t, err)
			after, err := provider("replace", store)
			require.NoError(t, err)
			require.Same(t, before, after, "exercise provider reuse rather than production eviction")
			info, err := srv.collections.Get(ctx, &qpb.GetCollectionInfoRequest{CollectionName: "replace"})
			require.NoError(t, err)
			require.Nil(t, info.Result.Config.QuantizationConfig)
			score("replace", 3, 1/math.Sqrt(2))
			score("keep", 2, .5)
			keep, err := srv.collections.Get(ctx, &qpb.GetCollectionInfoRequest{CollectionName: "keep"})
			require.NoError(t, err)
			require.NotNil(t, keep.Result.Config.QuantizationConfig.GetBinary())
		})
	}
}

// A restarted server reloads an explicit collection compression policy from
// its durable configuration while another collection without one follows the
// global default, in the same server and across fresh search services.
func TestMixedCompressionPoliciesSurviveRestart(t *testing.T) {
	for _, global := range []string{"", "none"} {
		t.Run("global="+global, func(t *testing.T) {
			t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
			t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
			t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
			t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", global)
			ctx := context.Background()
			base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			dbm, err := multidb.NewDatabaseManager(base, nil)
			require.NoError(t, err)
			firstProvider := sharedSearchProvider()
			first := startSharedIndexServer(t, dbm, base, firstProvider)
			_, err = first.collections.Create(ctx, &qpb.CreateCollection{CollectionName: "override",
				QuantizationConfig: &qpb.QuantizationConfig{Quantization: &qpb.QuantizationConfig_Binary{Binary: &qpb.BinaryQuantization{}}},
				VectorsConfig:      &qpb.VectorsConfig{Config: &qpb.VectorsConfig_Params{Params: &qpb.VectorParams{Size: 4, Distance: qpb.Distance_Cosine}}}})
			require.NoError(t, err)
			createCosineCollection(t, ctx, first.collections, "default", 4)
			for _, name := range []string{"override", "default"} {
				upsertPoint(t, ctx, first.points, name, 1, []float32{.3, .7, .1, .5}, nil)
			}
			exact := (.3 + .7) / math.Sqrt(.84) / math.Sqrt(2)
			// Rescore=false returns the score of the coordinates the graph traverses.
			approximate := func(srv sharedIndexServer, name string) float64 {
				no := false
				response, err := srv.points.Search(ctx, &qpb.SearchPoints{CollectionName: name, Vector: []float32{1, 1, 0, 0}, Limit: 1,
					Params: &qpb.SearchParams{Quantization: &qpb.QuantizationSearchParams{Rescore: &no}}})
				require.NoError(t, err)
				require.Len(t, response.Result, 1)
				require.Equal(t, uint64(1), response.Result[0].Id.GetNum())
				return float64(response.Result[0].Score)
			}
			check := func(srv sharedIndexServer, provider SearchServiceProvider) (float64, float64) {
				for _, name := range []string{"override", "default"} {
					requireCollectionHNSWReady(t, ctx, dbm, provider, srv.points, name, []float32{1, 1, 0, 0})
				}
				override, defaulted := approximate(srv, "override"), approximate(srv, "default")
				require.Greater(t, math.Abs(override-exact), 1e-3, "the binary override scores sign bits")
				if global == "none" {
					require.InDelta(t, exact, defaulted, 1e-6, "no global compression: float coordinates")
				} else {
					require.InDelta(t, exact, defaulted, 0.02, "scalar default: int8 coordinates near the exact score")
					require.Greater(t, math.Abs(defaulted-exact), 1e-6, "scalar default: not the float score")
				}
				info, err := srv.collections.Get(ctx, &qpb.GetCollectionInfoRequest{CollectionName: "override"})
				require.NoError(t, err)
				require.NotNil(t, info.Result.Config.QuantizationConfig.GetBinary())
				info, err = srv.collections.Get(ctx, &qpb.GetCollectionInfoRequest{CollectionName: "default"})
				require.NoError(t, err)
				require.Nil(t, info.Result.Config.QuantizationConfig)
				return override, defaulted
			}
			override, defaulted := check(first, firstProvider)
			first.stop()
			secondProvider := sharedSearchProvider()
			second := startSharedIndexServer(t, dbm, base, secondProvider)
			restartedOverride, restartedDefault := check(second, secondProvider)
			require.InDelta(t, override, restartedOverride, 1e-6)
			require.InDelta(t, defaulted, restartedDefault, 1e-6)
		})
	}
}
