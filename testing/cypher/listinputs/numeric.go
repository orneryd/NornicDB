package listinputs

import (
	"math"
	"reflect"
	"strconv"
)

// NumericInput is an independent boundary oracle. Core follows Cypher's
// numeric saturation/string-null policy; APOC and Sum retain zero/skip policy.
type NumericInput struct {
	Name                   string
	Scalar, List, Property interface{}
	Core                   interface{}
	Integer                int64
	Valid                  bool
	APOC, Sum              int64
}

type signedInput interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}
type unsignedInput interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}
type namedInt16 int16

func numericInput(name string, value, core interface{}, integer int64, valid bool) NumericInput {
	list := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(value)), 1, 1)
	list.Index(0).Set(reflect.ValueOf(value))
	property := interface{}([]interface{}{value})
	// Preserve the supported typed property representations. Other runtime
	// widths retain their scalar value in the supported boxed property carrier.
	switch value.(type) {
	case int, int32, int64, float32, float64, string:
		property = list.Interface()
	}
	// Named scalars remain named in parameter/boxed forms. Storage only accepts
	// physical property scalar types, so persist the same integer value there.
	if reflected := reflect.ValueOf(value); reflected.Type().PkgPath() != "" && reflected.Kind() == reflect.Int16 {
		property = []int64{reflected.Int()}
	}
	return NumericInput{name, value, list.Interface(), property, core, integer, valid, integer, integer}
}

func signedInputs[T signedInput](name string, minimum, maximum T) []NumericInput {
	values := []T{0, 12, -12, minimum, maximum}
	result := make([]NumericInput, 0, len(values))
	for i, value := range values {
		result = append(result, numericInput(name+"/"+strconv.Itoa(i), value, int64(value), int64(value), true))
	}
	return result
}
func unsignedInputs[T unsignedInput](name string, maximum T) []NumericInput {
	values := []T{0, 12, maximum}
	result := make([]NumericInput, 0, len(values))
	for i, value := range values {
		input := numericInput(name+"/"+strconv.Itoa(i), value, int64(value), int64(value), true)
		if uint64(value) > math.MaxInt64 {
			input.Core = nil
			input.Integer = 0
			input.Valid = false
			input.APOC = 0
			input.Sum = 0
		}
		result = append(result, input)
	}
	return result
}

// NumericInputs covers every runtime width, both signs, exact endpoints and
// adjacent floating/unsigned overflow values. No production reader is used.
func NumericInputs() []NumericInput {
	var inputs []NumericInput
	inputs = append(inputs, signedInputs("int", -int(^uint(0)>>1)-1, int(^uint(0)>>1))...)
	inputs = append(inputs, signedInputs("int8", int8(math.MinInt8), int8(math.MaxInt8))...)
	inputs = append(inputs, signedInputs("int16", int16(math.MinInt16), int16(math.MaxInt16))...)
	inputs = append(inputs, signedInputs("int32", int32(math.MinInt32), int32(math.MaxInt32))...)
	inputs = append(inputs, signedInputs("int64", int64(math.MinInt64), int64(math.MaxInt64))...)
	inputs = append(inputs, signedInputs("named-int16", namedInt16(math.MinInt16), namedInt16(math.MaxInt16))...)
	inputs = append(inputs, unsignedInputs("uint", ^uint(0))...)
	inputs = append(inputs, unsignedInputs("uint8", uint8(math.MaxUint8))...)
	inputs = append(inputs, unsignedInputs("uint16", uint16(math.MaxUint16))...)
	inputs = append(inputs, unsignedInputs("uint32", uint32(math.MaxUint32))...)
	inputs = append(inputs, unsignedInputs("uint64", uint64(math.MaxUint64))...)
	for _, value := range []uint64{math.MaxInt64, math.MaxInt64 + 1} {
		input := numericInput("uint64/bound/"+strconv.FormatUint(value, 10), value, nil, 0, false)
		if value == math.MaxInt64 {
			input.Core = int64(math.MaxInt64)
			input.Integer = math.MaxInt64
			input.Valid = true
			input.APOC = math.MaxInt64
			input.Sum = math.MaxInt64
		}
		inputs = append(inputs, input)
	}
	for _, tc := range []struct {
		name          string
		value         float64
		core, integer int64
		valid         bool
	}{
		{"positive", 12.75, 12, 12, true}, {"negative", -12.75, -12, -12, true}, {"zero", 0, 0, 0, true},
		{"below-max", math.Nextafter(0x1p63, 0), 9223372036854774784, 9223372036854774784, true},
		{"max-overflow", 0x1p63, math.MaxInt64, 0, false},
		{"minimum", -0x1p63, math.MinInt64, math.MinInt64, true},
		{"minimum-overflow", math.Nextafter(-0x1p63, math.Inf(-1)), math.MinInt64, 0, false},
		{"positive-overflow", 1e20, math.MaxInt64, 0, false}, {"negative-overflow", -1e20, math.MinInt64, 0, false},
		{"nan", math.NaN(), 0, 0, false}, {"positive-inf", math.Inf(1), math.MaxInt64, 0, false}, {"negative-inf", math.Inf(-1), math.MinInt64, 0, false},
	} {
		inputs = append(inputs, numericInput("float64/"+tc.name, tc.value, tc.core, tc.integer, tc.valid))
	}
	for _, tc := range []struct {
		name          string
		value         float32
		core, integer int64
		valid         bool
	}{
		{"positive", 12.75, 12, 12, true}, {"negative", -12.75, -12, -12, true}, {"zero", 0, 0, 0, true},
		{"below-max", math.Nextafter32(0x1p63, 0), 9223371487098961920, 9223371487098961920, true},
		{"max-overflow", 0x1p63, math.MaxInt64, 0, false}, {"minimum", -0x1p63, math.MinInt64, math.MinInt64, true},
		{"minimum-overflow", math.Nextafter32(-0x1p63, float32(math.Inf(-1))), math.MinInt64, 0, false},
		{"nan", float32(math.NaN()), 0, 0, false}, {"positive-inf", float32(math.Inf(1)), math.MaxInt64, 0, false}, {"negative-inf", float32(math.Inf(-1)), math.MinInt64, 0, false},
	} {
		inputs = append(inputs, numericInput("float32/"+tc.name, tc.value, tc.core, tc.integer, tc.valid))
	}
	for _, tc := range []struct {
		text          string
		core          interface{}
		integer, apoc int64
		valid         bool
	}{
		{"9223372036854775807", int64(math.MaxInt64), math.MaxInt64, math.MaxInt64, true},
		{"9223372036854775808", nil, 0, 0, false}, {"-9223372036854775809", nil, 0, 0, false},
		{"9223372036854775807.0", int64(math.MaxInt64), math.MaxInt64, 0, true},
		{"-9223372036854775808.0", int64(math.MinInt64), math.MinInt64, 0, true},
		{"9223372036854775806.5", int64(9223372036854775806), 9223372036854775806, 0, true},
		{"3.9", int64(3), 3, 0, true}, {"-3.9", int64(-3), -3, 0, true}, {"1e3", int64(1000), 1000, 0, true},
		{"1e20", nil, 0, 0, false}, {"-1e20", nil, 0, 0, false}, {"NaN", nil, 0, 0, false}, {"Infinity", nil, 0, 0, false},
	} {
		input := numericInput("string/"+tc.text, tc.text, tc.core, tc.integer, tc.valid)
		input.APOC = tc.apoc
		input.Sum = 0
		inputs = append(inputs, input)
	}
	return inputs
}
