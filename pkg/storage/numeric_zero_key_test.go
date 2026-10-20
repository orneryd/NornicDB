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
	key, ok := indexValueKey(zeros[0])
	require.True(t, ok)
	require.False(t, math.Signbit(key.(float64)))
	tuple := Constraint{Type: ConstraintUnique, Label: "Z", Properties: []string{"tenant", "v"}}
	var hashes, locks, relationships []interface{}
	for _, zero := range zeros {
		k, ok := indexValueKey(zero)
		require.True(t, ok)
		hashes = append(hashes, NewCompositeKey(k, uint64(9007199254740993)).Hash)
		props := map[string]interface{}{"tenant": zero, "v": int64(9007199254740993)}
		lock, ok := nodeConstraintLockKey(tuple, &Node{Properties: props})
		require.True(t, ok)
		locks = append(locks, lock)
		rel, ok := relationshipConstraintValues([]string{"tenant"}, props)
		require.True(t, ok)
		relationships = append(relationships, rel[0])
	}
	for _, forms := range [][]interface{}{hashes, locks, relationships} {
		for _, form := range forms[1:] {
			require.Equal(t, forms[0], form)
		}
	}
	// Non-zero keys and NaN keep their own forms.
	one, _ := indexValueKey(int64(1))
	require.NotEqual(t, NewCompositeKey(key).Hash, NewCompositeKey(one).Hash)
	_, ok = relationshipConstraintValues([]string{"v"}, map[string]interface{}{"v": math.NaN()})
	require.False(t, ok, "NaN is no equality key")
}
