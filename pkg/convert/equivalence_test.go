package convert

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// Two numbers have one equivalence key exactly when Cypher's = holds.
func TestNumberEquivalenceKeys(t *testing.T) {
	require.Equal(t, IntegerEquivalenceKey(42), FloatEquivalenceKey(42.0))
	require.Equal(t, IntegerEquivalenceKey(0), FloatEquivalenceKey(math.Copysign(0, -1)))
	require.Equal(t, UnsignedEquivalenceKey(42), IntegerEquivalenceKey(42))
	require.Equal(t, "I18446744073709551615", UnsignedEquivalenceKey(math.MaxUint64))
	require.Equal(t, "F2.5", FloatEquivalenceKey(2.5))
	require.Equal(t, "F+Inf", FloatEquivalenceKey(math.Inf(1)))
	require.Equal(t, "I9223372036854775808", FloatEquivalenceKey(math.Exp2(63)))
	require.Equal(t, "I-9223372036854775808", FloatEquivalenceKey(math.Exp2(63)*-1))
	require.NotEqual(t, IntegerEquivalenceKey(9007199254740993), FloatEquivalenceKey(9007199254740992.0))
	require.Equal(t, IntegerEquivalenceKey(9007199254740992), FloatEquivalenceKey(9007199254740992.0))
	require.NotEqual(t, FloatEquivalenceKey(math.NaN()), FloatEquivalenceKey(math.NaN()))
	buffer := []byte("x")
	require.Equal(t, "xI42", string(AppendFloatEquivalenceKey(buffer, 42)))
	require.Equal(t, "xI7", string(AppendUnsignedEquivalenceKey(buffer, 7)))
	require.Equal(t, "xF0.5", string(AppendFloatEquivalenceKey(buffer, 0.5)))
	require.Less(t, NextNaNKey(), NextNaNKey())
}
