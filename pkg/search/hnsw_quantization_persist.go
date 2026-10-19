package search

// Quantized graph data uses the existing atomic graph checkpoint. Originals
// remain in their original vector store; they are not duplicated in the file.
type hnswQuantizationSnapshot struct {
	Config     HNSWQuantizationConfig
	Dimensions int
	Stride     int
	Codes      []byte
	Scales     []float32
	Binary     []uint64
	Books      []ivfpqCodebook
	// MaxResidual and ResidualKnown carry the codes' error bound
	// (hnswQuantizedVectors.scoreBound). A graph saved before them loads
	// with no bound.
	MaxResidual   float64 `msgpack:",omitempty"`
	ResidualKnown bool    `msgpack:",omitempty"`
}

func (q *hnswQuantizedVectors) snapshot() *hnswQuantizationSnapshot {
	if q == nil {
		return nil
	}
	return &hnswQuantizationSnapshot{Config: q.config, Dimensions: q.dimensions, Stride: q.stride,
		Codes: append([]byte(nil), q.codes...), Scales: append([]float32(nil), q.scales...), Binary: append([]uint64(nil), q.binary...), Books: q.books,
		MaxResidual: q.residualBound(), ResidualKnown: q.residualKnown}
}

func restoreHNSWQuantization(s *hnswQuantizationSnapshot, dim, count int) (*hnswQuantizedVectors, bool) {
	if s == nil {
		return nil, true
	}
	if s.Dimensions != dim || dim <= 0 {
		return nil, false
	}
	q := &hnswQuantizedVectors{config: s.Config, dimensions: dim, stride: s.Stride, codes: s.Codes, scales: s.Scales, binary: s.Binary, books: s.Books,
		residualKnown: s.ResidualKnown}
	q.noteResidual(s.MaxResidual)
	switch s.Config.Mode {
	case HNSWQuantizationScalar:
		if s.Stride != dim || len(s.Codes)/dim != count || len(s.Codes)%dim != 0 || len(s.Scales) != count {
			return nil, false
		}
	case HNSWQuantizationBinary:
		stride := (dim + 63) / 64
		if s.Stride != stride || len(s.Binary)/stride != count || len(s.Binary)%stride != 0 {
			return nil, false
		}
	case HNSWQuantizationProduct:
		switch s.Config.PQCompression {
		case 4, 8, 16, 32, 64:
		default:
			return nil, false
		}
		chunk := s.Config.PQCompression / 4
		stride := (dim + chunk - 1) / chunk
		if s.Stride != stride || len(s.Books) != stride || len(s.Codes)/stride != count || len(s.Codes)%stride != 0 {
			return nil, false
		}
		for seg, book := range s.Books {
			if book.SubDim != min(chunk, dim-seg*chunk) || len(book.Codeword) == 0 || len(book.Codeword) > 256 {
				return nil, false
			}
			for _, center := range book.Codeword {
				if len(center) != book.SubDim {
					return nil, false
				}
			}
		}
		for i, code := range s.Codes {
			if int(code) >= len(s.Books[i%stride].Codeword) {
				return nil, false
			}
		}
	default:
		return nil, false
	}
	return q, true
}

func (q *hnswQuantizedVectors) grow(count int) {
	end := count * q.stride
	if q.config.Mode == HNSWQuantizationBinary {
		if end > len(q.binary) {
			q.binary = append(q.binary, make([]uint64, end-len(q.binary))...)
		}
		return
	}
	if end > len(q.codes) {
		q.codes = append(q.codes, make([]byte, end-len(q.codes))...)
	}
	if q.config.Mode == HNSWQuantizationScalar && count > len(q.scales) {
		q.scales = append(q.scales, make([]float32, count-len(q.scales))...)
	}
}
