package simd

// DotProductInt8 computes an exact signed-int8 dot product from two's-complement
// bytes. An int64 result preserves accumulation across long vectors. Empty or
// differently sized inputs return zero, matching DotProduct's input contract.
//
// Example: DotProductInt8([]byte{1, 255}, []byte{2, 3}) returns -1:
// the encoded vectors are [1, -1] and [2, 3].
func DotProductInt8(a, b []byte) int64 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	return dotProductInt8(a, b)
}

func dotProductInt8Reference(a, b []byte) int64 {
	var sum int64
	for i, x := range a {
		sum += int64(int8(x)) * int64(int8(b[i]))
	}
	return sum
}
