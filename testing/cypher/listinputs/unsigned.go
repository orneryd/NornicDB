package listinputs

import (
	"math"
	"strconv"
)

// UnsignedInput pins the signed conversion contract independently of its reader.
type UnsignedInput struct {
	Name    string
	Value   interface{}
	Integer int64
	Valid   bool
}

func UnsignedInputs() []UnsignedInput {
	inputs := []UnsignedInput{
		{"uint-zero", uint(0), 0, true},
		{"uint-one", uint(1), 1, true},
		{"uint8-zero", uint8(0), 0, true},
		{"uint8-max", uint8(math.MaxUint8), math.MaxUint8, true},
		{"uint16-zero", uint16(0), 0, true},
		{"uint16-max", uint16(math.MaxUint16), math.MaxUint16, true},
		{"uint32-zero", uint32(0), 0, true},
		{"uint32-max", uint32(math.MaxUint32), math.MaxUint32, true},
		{"uint64-zero", uint64(0), 0, true},
		{"uint64-one", uint64(1), 1, true},
		{"uint64-below-bound", uint64(math.MaxInt64 - 1), math.MaxInt64 - 1, true},
		{"uint64-bound", uint64(math.MaxInt64), math.MaxInt64, true},
		{"uint64-above-bound", uint64(math.MaxInt64) + 1, 0, false},
		{"uint64-max", uint64(math.MaxUint64), 0, false},
	}
	if strconv.IntSize == 64 {
		below, bound, above, maximum := uint64(math.MaxInt64-1), uint64(math.MaxInt64), uint64(math.MaxInt64)+1, uint64(math.MaxUint64)
		inputs = append(inputs,
			UnsignedInput{"uint-below-bound", uint(below), math.MaxInt64 - 1, true},
			UnsignedInput{"uint-bound", uint(bound), math.MaxInt64, true},
			UnsignedInput{"uint-above-bound", uint(above), 0, false},
			UnsignedInput{"uint-max", uint(maximum), 0, false},
		)
	}
	return inputs
}
