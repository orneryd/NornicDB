package search

import (
	"context"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"sync"

	"github.com/orneryd/nornicdb/pkg/simd"
)

// HNSWQuantization selects the coordinate representation used for graph search.
// Original coordinates still own construction and exact rescoring.
type HNSWQuantization string

const (
	// HNSWQuantizationNone uses original coordinates for traversal.
	HNSWQuantizationNone HNSWQuantization = ""
	// HNSWQuantizationScalar stores signed int8 coordinates and per-vector scales.
	HNSWQuantizationScalar HNSWQuantization = "scalar"
	// HNSWQuantizationProduct stores product codes against shared codebooks.
	HNSWQuantizationProduct HNSWQuantization = "pq"
	// HNSWQuantizationBinary stores one sign bit per coordinate.
	HNSWQuantizationBinary HNSWQuantization = "binary"
)

// HNSWQuantizationConfig controls graph coordinate compression independently of
// the graph quality preset. PQCompression is the float32-to-code byte ratio:
// 16 means four dimensions per one-byte code. For example:
//
//	cfg := HNSWQuantizationConfig{Mode: HNSWQuantizationProduct, PQCompression: 16}
//	err := index.SetQuantization(ctx, cfg)
//
// Scalar and binary use 8 and 1 bits per dimension respectively. Codebooks,
// scalar scales, graph links and retained originals are additional storage.
type HNSWQuantizationConfig struct {
	Mode               HNSWQuantization
	BinaryQueryBits    int // 1 (default), 4 or 8; stored binary coordinates remain one bit.
	PQCompression      int
	TrainingSampleMax  int
	TrainingIterations int
}

type hnswQuantizedVectors struct {
	config     HNSWQuantizationConfig
	dimensions int
	stride     int
	codes      []byte
	scales     []float32
	binary     []uint64
	books      []ivfpqCodebook
	queryPool  sync.Pool
}

func newHNSWQuantizedVectors(ctx context.Context, cfg HNSWQuantizationConfig, dimensions int, training [][]float32) (*hnswQuantizedVectors, error) {
	if dimensions <= 0 {
		return nil, ErrDimensionMismatch
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var err error
	cfg, err = normalizeHNSWQuantizationConfig(cfg)
	if err != nil {
		return nil, err
	}
	q := &hnswQuantizedVectors{config: cfg, dimensions: dimensions}
	switch cfg.Mode {
	case HNSWQuantizationScalar:
		q.stride = dimensions
	case HNSWQuantizationBinary:
		q.stride = (dimensions + 63) / 64
	case HNSWQuantizationProduct:
		if len(training) == 0 {
			return nil, fmt.Errorf("HNSW PQ requires training vectors")
		}
		chunk := cfg.PQCompression / 4
		q.stride = (dimensions + chunk - 1) / chunk
		q.books = make([]ivfpqCodebook, q.stride)
		for _, v := range training {
			if len(v) != dimensions {
				return nil, ErrDimensionMismatch
			}
		}
		errors := make([]error, q.stride)
		err := parallelOver(ctx, q.stride, func(seg int) {
			start, end := seg*chunk, min((seg+1)*chunk, dimensions)
			subvectors := make([][]float32, len(training))
			for i, v := range training {
				subvectors[i] = v[start:end]
			}
			// Each independent book has its own deterministic stream. Worker
			// scheduling cannot change the trained model.
			rng := rand.New(rand.NewSource(ivfpqTrainSeed + int64(seg)))
			centers, err := ivfpqTrainPQCodebook(ctx, subvectors, min(256, len(training)), cfg.TrainingIterations, rng)
			errors[seg] = err
			q.books[seg] = ivfpqCodebook{SubDim: end - start, Codeword: centers}
		})
		if err != nil {
			return nil, err
		}
		for _, err := range errors {
			if err != nil {
				return nil, err
			}
		}

	default:
		return nil, fmt.Errorf("unsupported HNSW quantization %q", cfg.Mode)
	}
	q.config = cfg
	return q, nil
}

// set writes codes at the graph's stable internal ID, including tombstone slots.
// The owning graph serializes mutations; original construction vectors stay intact.
func (q *hnswQuantizedVectors) set(id uint32, v []float32) {
	start, end := int(id)*q.stride, (int(id)+1)*q.stride
	if q.config.Mode == HNSWQuantizationBinary {
		if end > len(q.binary) {
			q.binary = append(q.binary, make([]uint64, end-len(q.binary))...)
		}
		row := q.binary[start:end]
		clear(row)
		for i, x := range v {
			if x > 0 {
				row[i/64] |= uint64(1) << uint(i%64)
			}
		}
		return
	}
	if end > len(q.codes) {
		q.codes = append(q.codes, make([]byte, end-len(q.codes))...)
	}
	row := q.codes[start:end]
	if q.config.Mode == HNSWQuantizationScalar {
		if int(id) >= len(q.scales) {
			q.scales = append(q.scales, make([]float32, int(id)+1-len(q.scales))...)
		}
		q.scales[id] = encodeHNSWInt8(row, v)
		return
	}
	offset := 0
	for seg, book := range q.books {
		sub := v[offset : offset+book.SubDim]
		row[seg] = byte(nearestPQCodeword(sub, book.Codeword))
		offset += book.SubDim
	}
}

func encodeHNSWInt8(dst []byte, v []float32) float32 {
	var maximum float32
	for _, x := range v {
		if x < 0 {
			x = -x
		}
		if x > maximum {
			maximum = x
		}
	}
	if maximum == 0 {
		clear(dst)
		return 0
	}
	scale := maximum / 127
	for i, x := range v {
		dst[i] = byte(int8(math.Round(float64(x / scale))))
	}
	return scale
}

type hnswQuantizedQuery struct {
	store      *hnswQuantizedVectors
	scalar     []byte
	scale      float32
	binary     []uint64
	lut        []float32
	planes     []uint64
	integerSum int32
}

func (q *hnswQuantizedVectors) query(v []float32) *hnswQuantizedQuery {
	out, _ := q.queryPool.Get().(*hnswQuantizedQuery)
	if out == nil {
		out = &hnswQuantizedQuery{store: q}
	}
	out.scale, out.integerSum = 0, 0
	switch q.config.Mode {
	case HNSWQuantizationScalar:
		out.scalar = resizeQuantizationBuffer(out.scalar, len(v))
		out.scale = encodeHNSWInt8(out.scalar, v)
	case HNSWQuantizationBinary:
		if q.config.BinaryQueryBits > 1 {
			out.encodeBinaryQuery(v)
			break
		}
		out.binary = resizeQuantizationBuffer(out.binary, q.stride)
		clear(out.binary)
		for i, x := range v {
			if x > 0 {
				out.binary[i/64] |= uint64(1) << uint(i%64)
			}
		}
	case HNSWQuantizationProduct:
		out.lut = resizeQuantizationBuffer(out.lut, len(q.books)*256)
		offset := 0
		for seg, book := range q.books {
			for code, center := range book.Codeword {
				var dot float32
				for d, x := range center {
					dot += v[offset+d] * x
				}
				out.lut[seg*256+code] = dot
			}
			offset += book.SubDim
		}
	}
	return out
}

func (q *hnswQuantizedQuery) dot(id uint32) float32 {
	s := q.store
	start := int(id) * s.stride
	switch s.config.Mode {
	case HNSWQuantizationScalar:
		row := s.codes[start : start+s.stride]
		sum := simd.DotProductInt8(q.scalar, row)
		return float32(sum) * q.scale * s.scales[id]
	case HNSWQuantizationBinary:
		row := s.binary[start : start+s.stride]
		if len(q.planes) > 0 {
			return q.binaryScalarDot(row)
		}
		different := 0
		for i, a := range q.binary {
			different += bits.OnesCount64(a ^ row[i])
		}
		return 1 - 2*float32(different)/float32(s.dimensions)
	default:
		row := s.codes[start : start+s.stride]
		var sum float32
		for seg, code := range row {
			sum += q.lut[seg*256+int(code)]
		}
		return sum
	}
}

func (q *hnswQuantizedVectors) bytes() int {
	size := len(q.codes) + 8*len(q.binary) + 4*len(q.scales)
	for _, book := range q.books {
		for _, center := range book.Codeword {
			size += 4 * len(center)
		}
	}
	return size
}

func resizeQuantizationBuffer[T any](buf []T, size int) []T {
	if cap(buf) < size {
		return make([]T, size)
	}
	return buf[:size]
}

func (q *hnswQuantizedQuery) release() { q.store.queryPool.Put(q) }
