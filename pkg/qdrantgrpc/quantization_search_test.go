package qdrantgrpc

import (
	"context"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestQuantizationControlsReachPublicSearchAndQuery(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", "binary")
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	srv := startSharedIndexServer(t, dbm, base, sharedSearchProvider())
	createCosineCollection(t, ctx, srv.collections, "quantized", 2)
	upsertPoint(t, ctx, srv.points, "quantized", 1, []float32{1, 0.1}, nil)
	upsertPoint(t, ctx, srv.points, "quantized", 2, []float32{-1, -0.1}, nil)
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
	base := storage.NewMemoryEngine()
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
			base := storage.NewMemoryEngine()
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
