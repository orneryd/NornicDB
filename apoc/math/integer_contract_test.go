// Author: Codex NOR-184 implementation lane, 2026-10-29.
package math_test

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/apoc"
	apocmath "github.com/orneryd/nornicdb/apoc/math"
	"github.com/orneryd/nornicdb/apoc/number"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/testing/apocintegers"
	"github.com/stretchr/testify/require"
)

// The same fixture compiles on the old scalar API and the checked API. This
// adapter has no arithmetic; a panic is reported separately from a valid error.
func integerCallNOR184(fn interface{}, a, b int64) (value int64, err error, panicked interface{}) {
	defer func() { panicked = recover() }()
	switch fn := fn.(type) {
	case func(int64, int64) int64:
		value = fn(a, b)
	case func(int64, int64) (int64, error):
		value, err = fn(a, b)
	default:
		panic("unexpected integer export signature")
	}
	return
}

func TestIntegerContractNOR184(t *testing.T) {
	for _, entry := range []struct {
		name, operation string
		fn              interface{}
	}{
		{"math_gcd", "gcd", apocmath.Gcd}, {"number_gcd", "gcd", number.GCD},
		{"math_lcm", "lcm", apocmath.Lcm}, {"number_lcm", "lcm", number.LCM},
	} {
		for _, a := range apocintegers.Values {
			for _, b := range apocintegers.Values {
				t.Run(fmt.Sprintf("%s/%d/%d", entry.name, a, b), func(t *testing.T) {
					want, fits := apocintegers.Expected(entry.operation, a, b)
					got, err, panicked := integerCallNOR184(entry.fn, a, b)
					require.Nil(t, panicked, "no class permits a panic")
					if !fits {
						require.Error(t, err, "nonnegative result exceeds int64")
						return
					}
					require.NoError(t, err)
					require.Equal(t, want, got)
				})
			}
		}
	}
}

func TestAPOCIntegerCallContractNOR184(t *testing.T) {
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
	for _, namespace := range []string{"math", "number"} {
		for _, operation := range []string{"gcd", "lcm"} {
			name := "apoc." + namespace + "." + operation
			for _, a := range apocintegers.Values {
				for _, b := range apocintegers.Values {
					t.Run(fmt.Sprintf("%s/%d/%d", name, a, b), func(t *testing.T) {
						want, fits := apocintegers.Expected(operation, a, b)
						var got interface{}
						var err error
						require.NotPanics(t, func() { got, err = apoc.Call(name, []interface{}{a, b}) })
						if !fits {
							require.Error(t, err)
							require.Nil(t, got)
							return
						}
						require.NoError(t, err)
						require.Equal(t, want, got)
					})
				}
			}
			for _, input := range apocintegers.Inputs {
				for _, other := range []int64{0, 6} {
					for _, swap := range []bool{false, true} {
						t.Run(fmt.Sprintf("%s/%s/%d/swap=%t", name, input.Name, other, swap), func(t *testing.T) {
							args := []interface{}{input.Value, other}
							if swap {
								args[0], args[1] = args[1], args[0]
							}
							got, err := apoc.Call(name, args)
							want, fits := apocintegers.Expected(operation, input.Integer, other)
							if !input.Valid || !fits {
								require.Error(t, err)
								require.Nil(t, got)
								return
							}
							require.NoError(t, err)
							require.Equal(t, want, got)
						})
					}
				}
			}
			for _, args := range [][]interface{}{nil, {int64(1)}, {int64(1), int64(2), int64(3)}} {
				_, err := apoc.Call(name, args)
				require.Error(t, err, "argument count")
			}
		}
	}
}
