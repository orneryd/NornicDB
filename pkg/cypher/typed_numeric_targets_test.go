package cypher

import (
	"context"
	"math"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/testing/cypher/listinputs"
	"github.com/stretchr/testify/require"
)

type typedSignedTarget interface {
	~int8 | ~int16 | ~int32 | ~int64
}
type typedUnsignedTarget interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64
}

func checkTypedSignedTargets[T typedSignedTarget](t *testing.T) {
	exec, _ := typedNumericExecutor(t)
	width := reflect.TypeOf(*new(T)).Bits()
	maximum := int64((uint64(1) << (width - 1)) - 1)
	minimum := -maximum - 1
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, input := range listinputs.NumericInputs() {
			t.Run(input.Name+boolTestName(disabled), func(t *testing.T) {
				_, text := input.Scalar.(string)
				valid := !text && input.Valid && input.Integer >= minimum && input.Integer <= maximum
				params := map[string]interface{}{"xs": input.List}
				scalar, err := TypedExecute[T](context.Background(), exec, "RETURN head($xs) AS v", params)
				list, listErr := TypedExecute[[]T](context.Background(), exec, "RETURN $xs AS v", params)
				if valid {
					require.NoError(t, err)
					require.NoError(t, listErr)
					require.Equal(t, []T{T(input.Integer)}, scalar.Rows)
					require.Equal(t, [][]T{{T(input.Integer)}}, list.Rows)
				} else {
					require.Error(t, err)
					require.Error(t, listErr)
				}
				if kind := reflect.TypeOf(input.Scalar).Kind(); kind == reflect.Float32 || kind == reflect.Float64 {
					floating := reflect.ValueOf(input.Scalar).Float()
					valid = valid && math.Trunc(floating) == floating
				}
				calls := 0
				withPluginFunctions(t, map[string]interface{}{
					"zz.numeric.scalar": func(value T) T { calls++; return value },
					"zz.numeric.list":   func(values []T) []T { calls++; return values },
				})
				for _, expression := range []string{"zz.numeric.scalar(head($xs))", "zz.numeric.list($xs)"} {
					got, err := exec.Execute(context.Background(), "RETURN "+expression+" AS v", params)
					if valid {
						require.NoError(t, err)
						var want interface{} = T(input.Integer)
						if expression == "zz.numeric.list($xs)" {
							want = []T{T(input.Integer)}
						}
						require.Equal(t, typedNumericJSON(t, want), typedNumericJSON(t, got.Rows[0][0]))
					} else {
						require.Error(t, err)
					}
				}
				if valid {
					require.Equal(t, 2, calls)
				} else {
					require.Zero(t, calls, "conversion must fail before invoking a typed handler")
				}
			})
		}
	}
}

// The unsigned oracle retains the full uint64 range. A signed-int oracle would
// incorrectly label MaxUint64 invalid even for a uint64 target.
func typedUnsignedExpected(value interface{}, width int, whole bool) (uint64, bool) {
	source := reflect.ValueOf(value)
	var unsigned uint64
	switch source.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		integer := source.Int()
		if integer < 0 {
			return 0, false
		}
		unsigned = uint64(integer)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		unsigned = source.Uint()
	case reflect.Float32, reflect.Float64:
		floating := source.Float()
		truncated := math.Trunc(floating)
		if math.IsNaN(floating) || math.IsInf(floating, 0) || truncated < 0 || truncated >= 0x1p64 || whole && floating != truncated {
			return 0, false
		}
		unsigned = uint64(truncated)
	default:
		return 0, false
	}
	if unsigned > math.MaxUint64>>uint(64-width) {
		return 0, false
	}
	return unsigned, true
}

func checkTypedUnsignedTargets[T typedUnsignedTarget](t *testing.T) {
	exec, _ := typedNumericExecutor(t)
	width := reflect.TypeOf(*new(T)).Bits()
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, input := range listinputs.NumericInputs() {
			t.Run(input.Name+boolTestName(disabled), func(t *testing.T) {
				want, valid := typedUnsignedExpected(input.Scalar, width, false)
				params := map[string]interface{}{"xs": input.List}
				scalar, err := TypedExecute[T](context.Background(), exec, "RETURN head($xs) AS v", params)
				list, listErr := TypedExecute[[]T](context.Background(), exec, "RETURN $xs AS v", params)
				if valid {
					require.NoError(t, err)
					require.NoError(t, listErr)
					require.Equal(t, []T{T(want)}, scalar.Rows)
					require.Equal(t, [][]T{{T(want)}}, list.Rows)
				} else {
					require.Error(t, err)
					require.Error(t, listErr)
				}
				want, valid = typedUnsignedExpected(input.Scalar, width, true)
				calls := 0
				withPluginFunctions(t, map[string]interface{}{
					"zz.numeric.scalar": func(value T) T { calls++; return value }, "zz.numeric.list": func(values []T) []T { calls++; return values },
				})
				for _, expression := range []string{"zz.numeric.scalar(head($xs))", "zz.numeric.list($xs)"} {
					got, err := exec.Execute(context.Background(), "RETURN "+expression+" AS v", params)
					if valid {
						require.NoError(t, err)
						var expected interface{} = T(want)
						if expression == "zz.numeric.list($xs)" {
							expected = []T{T(want)}
						}
						require.Equal(t, typedNumericJSON(t, expected), typedNumericJSON(t, got.Rows[0][0]))
					} else {
						require.Error(t, err)
					}
				}
				if valid {
					require.Equal(t, 2, calls)
				} else {
					require.Zero(t, calls)
				}
			})
		}
	}
}

func TestTypedNumericCheckedTargets(t *testing.T) {
	t.Run("int8", checkTypedSignedTargets[int8])
	t.Run("int16", checkTypedSignedTargets[int16])
	t.Run("int32", checkTypedSignedTargets[int32])
	t.Run("int64", checkTypedSignedTargets[int64])
	t.Run("uint8", checkTypedUnsignedTargets[uint8])
	t.Run("uint16", checkTypedUnsignedTargets[uint16])
	t.Run("uint32", checkTypedUnsignedTargets[uint32])
	t.Run("uint64", checkTypedUnsignedTargets[uint64])
}
