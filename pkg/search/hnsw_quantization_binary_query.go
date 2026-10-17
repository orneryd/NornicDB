package search

import (
	"math"
	"math/bits"
)

// A scalar query adds precision without expanding the stored one-bit vectors.
// Transpose signed integer coordinates into bitplanes so scoring needs only
// popcounts, rather than decoding each stored dimension or reading originals.
func (q *hnswQuantizedQuery) encodeBinaryQuery(v []float32) {
	width := q.store.config.BinaryQueryBits
	q.planes = resizeQuantizationBuffer(q.planes, width*q.store.stride)
	clear(q.planes)
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
		return
	}
	largest := (1 << uint(width-1)) - 1
	q.scale = maximum / float32(largest)
	for i, x := range v {
		value := int32(math.Round(float64(x / q.scale)))
		q.integerSum += value
		for plane := 0; plane < width; plane++ {
			if uint32(value)&(1<<uint(plane)) != 0 {
				q.planes[plane*q.store.stride+i/64] |= 1 << uint(i%64)
			}
		}
	}
}

func (q *hnswQuantizedQuery) binaryScalarDot(row []uint64) float32 {
	width, stride := q.store.config.BinaryQueryBits, q.store.stride
	var selected int32
	for plane := 0; plane < width; plane++ {
		weight := int32(1 << uint(plane))
		if plane == width-1 {
			weight = -weight
		}
		count := 0
		for i, word := range row {
			count += bits.OnesCount64(word & q.planes[plane*stride+i])
		}
		selected += int32(count) * weight
	}
	// A stored positive bit contributes +query; a negative bit contributes
	// -query. The reconstructed sign vector has unit length.
	return float32(2*selected-q.integerSum) * q.scale / float32(math.Sqrt(float64(q.store.dimensions)))
}
