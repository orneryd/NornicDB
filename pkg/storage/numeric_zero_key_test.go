package storage

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// Both zero signs and integer 0 are one value: every hash or text form of the
// canonical numeric key must be identical for them, not just Go map equality.
func TestNumericKeyZeroSignsSerializeAlike(t *testing.T) {
	zeros := []interface{}{math.Copysign(0, -1), 0.0, int64(0), float32(math.Copysign(0, -1)), uint64(0)}
	key, ok, keyErr := indexValueKey(t.Context(), zeros[0])
	require.NoError(t, keyErr)
	require.True(t, ok)
	require.False(t, math.Signbit(key.(float64)))
	tuple := Constraint{Type: ConstraintUnique, Label: "Z", Properties: []string{"tenant", "v"}}
	var hashes, locks, relationships []interface{}
	for _, zero := range zeros {
		k, ok, keyErr := indexValueKey(t.Context(), zero)
		require.NoError(t, keyErr)
		require.True(t, ok)
		composite, err := NewCompositeKey(t.Context(), k, uint64(9007199254740993))
		require.NoError(t, err)
		hashes = append(hashes, composite.Hash)

		props := map[string]interface{}{"tenant": zero, "v": int64(9007199254740993)}
		lock, ok, err := nodeConstraintLockKey(t.Context(), tuple, &Node{Properties: props})
		require.NoError(t, err)
		require.True(t, ok)
		locks = append(locks, lock)
		rel, ok, keyErr := relationshipConstraintValueKeys(t.Context(), []string{"tenant"}, props, false)
		require.NoError(t, keyErr)
		require.True(t, ok)
		relationships = append(relationships, rel[0])
	}
	for _, forms := range [][]interface{}{hashes, locks, relationships} {
		for _, form := range forms[1:] {
			require.Equal(t, forms[0], form)
		}
	}
	// Non-zero keys and NaN keep their own forms.
	one, _, keyErr := indexValueKey(t.Context(), int64(1))
	require.NoError(t, keyErr)
	keyHash, err := NewCompositeKey(t.Context(), key)
	require.NoError(t, err)
	oneHash, err := NewCompositeKey(t.Context(), one)
	require.NoError(t, err)
	require.NotEqual(t, keyHash.Hash, oneHash.Hash)

	_, ok, keyErr = relationshipConstraintValueKeys(t.Context(), []string{"v"}, map[string]interface{}{"v": math.NaN()}, false)
	require.NoError(t, keyErr)
	require.False(t, ok, "NaN is no equality key")
}
