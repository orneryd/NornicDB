package qdrantgrpc

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/search"
	qpb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/protobuf/proto"
)

// A custom CollectionStore must explicitly support durable quantization
// configuration before Create can acknowledge a quantized collection.
type quantizedCollectionStore interface {
	CreateWithQuantization(context.Context, string, int, qpb.Distance, *qpb.QuantizationConfig) error
}

func collectionQuantizationConfig(req *qpb.CreateCollection) (*qpb.QuantizationConfig, error) {
	cfg := req.QuantizationConfig
	if single := req.GetVectorsConfig().GetParams(); single != nil && single.QuantizationConfig != nil {
		cfg = single.QuantizationConfig
	}
	if named := req.GetVectorsConfig().GetParamsMap(); named != nil {
		first := true
		for _, params := range named.Map {
			candidate := req.QuantizationConfig
			if params.QuantizationConfig != nil {
				candidate = params.QuantizationConfig
			}
			if first {
				cfg = candidate
				first = false
			} else if !proto.Equal(cfg, candidate) {
				return nil, fmt.Errorf("different quantization modes for named vectors in one collection are not supported")
			}
		}
	}
	if _, err := searchQuantizationConfig(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func searchQuantizationConfig(cfg *qpb.QuantizationConfig) (search.HNSWQuantizationConfig, error) {
	if cfg == nil {
		return search.HNSWQuantizationConfig{}, nil
	}
	switch q := cfg.Quantization.(type) {
	case *qpb.QuantizationConfig_Scalar:
		if q.Scalar == nil || q.Scalar.Type != qpb.QuantizationType_Int8 {
			return search.HNSWQuantizationConfig{}, fmt.Errorf("scalar quantization requires int8")
		}
		if q.Scalar.Quantile != nil && q.Scalar.GetQuantile() != 1 {
			return search.HNSWQuantizationConfig{}, fmt.Errorf("scalar quantiles other than 1 are not supported")
		}
		return search.HNSWQuantizationConfig{Mode: search.HNSWQuantizationScalar}, nil
	case *qpb.QuantizationConfig_Binary:
		if q.Binary == nil {
			return search.HNSWQuantizationConfig{}, fmt.Errorf("binary quantization configuration is missing")
		}
		if q.Binary.GetEncoding() != qpb.BinaryQuantizationEncoding_OneBit {
			return search.HNSWQuantizationConfig{}, fmt.Errorf("binary storage currently requires one bit per dimension")
		}
		bits := 1
		switch q.Binary.GetQueryEncoding().GetSetting() {
		case qpb.BinaryQuantizationQueryEncoding_Default, qpb.BinaryQuantizationQueryEncoding_Binary:
		case qpb.BinaryQuantizationQueryEncoding_Scalar4Bits:
			bits = 4
		case qpb.BinaryQuantizationQueryEncoding_Scalar8Bits:
			bits = 8
		default:
			return search.HNSWQuantizationConfig{}, fmt.Errorf("unsupported binary query encoding")
		}
		return search.HNSWQuantizationConfig{Mode: search.HNSWQuantizationBinary, BinaryQueryBits: bits}, nil
	case *qpb.QuantizationConfig_Product:
		if q.Product == nil {
			return search.HNSWQuantizationConfig{}, fmt.Errorf("product quantization configuration is missing")
		}
		ratio := 0
		switch q.Product.Compression {
		case qpb.CompressionRatio_x4:
			ratio = 4
		case qpb.CompressionRatio_x8:
			ratio = 8
		case qpb.CompressionRatio_x16:
			ratio = 16
		case qpb.CompressionRatio_x32:
			ratio = 32
		case qpb.CompressionRatio_x64:
			ratio = 64
		default:
			return search.HNSWQuantizationConfig{}, fmt.Errorf("unsupported product compression ratio")
		}
		cfg := search.HNSWQuantizationConfigFromEnv()
		cfg.Mode = search.HNSWQuantizationProduct
		cfg.PQCompression = ratio
		return cfg, nil
	default:
		return search.HNSWQuantizationConfig{}, fmt.Errorf("quantization mode is required")
	}
}
