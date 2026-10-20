package cypher

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Every integer type and every whole-valued float in int64 range key as the
// same int64, so a membership index built from one numeric type finds the
// equal value of another (1 IN [1.0], an HTTP float parameter against a stored
// integer). Larger unsigned integers use the canonical uint64 key without
// narrowing; fractional floats and non-numbers retain their own keys.
func TestMembershipKeyNormalizesNumbers(t *testing.T) {
	for _, v := range []interface{}{
		int64(7), int(7), int32(7), int16(7), int8(7),
		uint(7), uint64(7), uint32(7), uint16(7), uint8(7),
		float64(7), float32(7),
	} {
		assert.Equal(t, int64(7), membershipKey(v), "%T", v)
	}

	assert.Equal(t, int64(math.MaxInt64), membershipKey(uint64(math.MaxInt64)))
	assert.Equal(t, uint64(math.MaxInt64)+1, membershipKey(uint64(math.MaxInt64)+1), "above int64 keeps its own key")
	assert.Equal(t, uint64(math.MaxUint), membershipKey(uint(math.MaxUint)), "above int64 retains exact bits in the canonical unsigned key")
	assert.Equal(t, 1.5, membershipKey(1.5))
	assert.Equal(t, float64(float32(2.5)), membershipKey(float32(2.5)))
	assert.Equal(t, 1e19, membershipKey(1e19), "whole float beyond int64 stays a float")
	assert.True(t, math.IsNaN(membershipKey(math.NaN()).(float64)))
	assert.Equal(t, "7", membershipKey("7"), "strings are not numbers")
	assert.Equal(t, true, membershipKey(true))

	set, nonComparable := buildComparableMembershipIndex([]interface{}{int32(1), 2.0, float32(3.5), "x", nil, []interface{}{int64(4)}})
	equals := (&StorageExecutor{}).compareEqual
	assert.True(t, evaluateComparableMembership(int64(1), set, nonComparable, equals))
	assert.True(t, evaluateComparableMembership(uint8(2), set, nonComparable, equals))
	assert.True(t, evaluateComparableMembership(3.5, set, nonComparable, equals))
	assert.True(t, evaluateComparableMembership("x", set, nonComparable, equals))
	assert.False(t, evaluateComparableMembership("1", set, nonComparable, equals))
	assert.False(t, evaluateComparableMembership(nil, set, nonComparable, equals))

	// The key's Go carrier is internal; preserve the public numeric value and
	// ensure canonicalizing unsigned widths never aliases the signed value -1.
	set, nonComparable = buildComparableMembershipIndex([]interface{}{uint(math.MaxUint)})
	assert.True(t, evaluateComparableMembership(uint64(math.MaxUint), set, nonComparable, equals))
	assert.False(t, evaluateComparableMembership(int64(-1), set, nonComparable, equals))
	exec := setupTestExecutor(t)
	for _, needle := range []interface{}{uint(math.MaxUint), uint64(math.MaxUint)} {
		assert.Equal(t, true, typedNumericRead(t, exec, "RETURN $needle IN $xs AS v", map[string]interface{}{"needle": needle, "xs": []uint{math.MaxUint}}))
	}
	assert.Equal(t, false, typedNumericRead(t, exec, "RETURN -1 IN $xs AS v", map[string]interface{}{"xs": []uint{math.MaxUint}}))
}
