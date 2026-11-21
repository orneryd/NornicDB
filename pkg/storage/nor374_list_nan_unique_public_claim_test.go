package storage

// NOR-374 controls for UNIQUE claim identity of supported list values.
//
// A list holding NaN is a supported property value
// (validatePropertyValueForStorage accepts either element), and compareValues /
// equalConstraintLists keep it unequal even to itself, as scalar NaN is. A
// canonical key can only be reflexive, so such a value has no indexValueKey
// (writeConstraintListNumericKey declines NaN): the UNIQUE claim cache never
// answers for it, the label scan decides, and a value cache rebuild adopts the
// stored nodes instead of rejecting them.
//
// The fail-first regression for that admission is
// TestNOR374UniqueWriteAdmissionIsIndependentOfClaimCacheCompleteness. These
// subtests are the controls around it: the contracts the fix must not change
// (finite lists, cross-width numeric lists, scalar NaN) and the agreement of
// the scan and rebuild consumers on the supported NaN list.

import (
	"math"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type nor374ClaimState struct {
	holder      NodeID
	found       bool
	constrained bool
	complete    bool
}

// nor374ReadClaim reads the public, planner-trusted claim for value.
func nor374ReadClaim(t *testing.T, schema *SchemaManager, value any) nor374ClaimState {
	t.Helper()
	holder, found, constrained, complete, err := schema.LookupUniqueConstraintValue(t.Context(), "Doc", "ref", value)
	require.NoError(t, err, "the public claim lookup must not fail")
	return nor374ClaimState{holder: holder, found: found, constrained: constrained, complete: complete}
}

// nor374UniqueDocEngine is a public BadgerEngine with a live UNIQUE constraint
// on (:Doc).ref whose claim cache is complete, which is the state a running
// database reaches once the cache has been built from storage.
func nor374UniqueDocEngine(t *testing.T) (*BadgerEngine, *SchemaManager) {
	t.Helper()
	engine := newTestEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddUniqueConstraint("doc_ref", "Doc", "ref"))
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(t.Context(), engine, schema))
	return engine, schema
}

func nor374CreateDoc(t *testing.T, engine *BadgerEngine, id NodeID, value any) error {
	t.Helper()
	_, err := engine.CreateNode(t.Context(), &Node{
		ID:         id,
		Labels:     []string{"Doc"},
		Properties: map[string]any{"ref": value},
	})
	return err
}

// nor374RequireStoredNaNList asserts the stored image keeps the list container,
// its two positions and the NaN element (the property value round-trips).
func nor374RequireStoredNaNList(t *testing.T, engine *BadgerEngine, id NodeID) {
	t.Helper()
	stored, err := engine.GetNode(t.Context(), id)
	require.NoError(t, err)
	value, ok := stored.Properties["ref"]
	require.Truef(t, ok, "stored node %s must keep ref", id)
	list := reflect.ValueOf(value)
	require.Contains(t, []reflect.Kind{reflect.Slice, reflect.Array}, list.Kind(), "the stored image must keep the list container")
	require.Equal(t, 2, list.Len())
	first := list.Index(0)
	if first.Kind() == reflect.Interface {
		first = first.Elem()
	}
	require.Equal(t, reflect.Float64, first.Kind())
	require.True(t, math.IsNaN(first.Float()), "the stored list must keep NaN")
	second := list.Index(1)
	if second.Kind() == reflect.Interface {
		second = second.Elem()
	}
	require.Equal(t, reflect.String, second.Kind())
	require.Equal(t, "key", second.String())
}

func TestNOR374ListNaNUniqueClaimIdentityPublicControl(t *testing.T) {
	// Control: a finite list needs no NaN handling; canonical identity and the
	// equality owners agree with each other and with the duplicate rejection.
	t.Run("finite_list_control_duplicate_is_rejected", func(t *testing.T) {
		engine, schema := nor374UniqueDocEngine(t)

		first := []any{"key", int64(1)}
		second := []any{"key", int64(1)}
		require.True(t, compareValues(first, second))
		equal, lists := equalConstraintLists(first, second)
		require.True(t, lists)
		require.True(t, equal)
		firstKey, keyed, err := indexValueKey(t.Context(), first)
		require.NoError(t, err)
		require.True(t, keyed)
		secondKey, keyed, err := indexValueKey(t.Context(), second)
		require.NoError(t, err)
		require.True(t, keyed)
		require.Equal(t, firstKey, secondKey)

		require.NoError(t, nor374CreateDoc(t, engine, "test:a", first))
		claim := nor374ReadClaim(t, schema, first)
		require.True(t, claim.found)
		require.Equal(t, NodeID("test:a"), claim.holder)

		err = nor374CreateDoc(t, engine, "test:b", second)
		violation := asConstraintViolation(t, err)
		require.Equal(t, ConstraintUnique, violation.Type)
		require.Equal(t, "Node with ref=[key 1] already exists (nodeID: test:a)", violation.Message)

		_, err = engine.GetNode(t.Context(), "test:b")
		require.ErrorIs(t, err, ErrNotFound)
		count, err := engine.NodeCount(t.Context())
		require.NoError(t, err)
		require.Equal(t, int64(1), count)
	})

	// Control: the approved list numeric-width identity (int 7 and float 7 in a
	// list are one value) still agrees across canonical identity and equality.
	t.Run("numeric_list_control_duplicate_is_rejected", func(t *testing.T) {
		engine, schema := nor374UniqueDocEngine(t)

		first := []any{int64(7), "k"}
		second := []any{float64(7), "k"}
		require.True(t, compareValues(first, second), "the approved list numeric-width contract equates 7 and 7.0")
		equal, lists := equalConstraintLists(first, second)
		require.True(t, lists)
		require.True(t, equal)
		firstKey, keyed, err := indexValueKey(t.Context(), first)
		require.NoError(t, err)
		require.True(t, keyed)
		secondKey, keyed, err := indexValueKey(t.Context(), second)
		require.NoError(t, err)
		require.True(t, keyed)
		require.Equal(t, firstKey, secondKey)

		require.NoError(t, nor374CreateDoc(t, engine, "test:a", first))
		claim := nor374ReadClaim(t, schema, second)
		require.True(t, claim.found)
		require.Equal(t, NodeID("test:a"), claim.holder)

		err = nor374CreateDoc(t, engine, "test:b", second)
		violation := asConstraintViolation(t, err)
		require.Equal(t, ConstraintUnique, violation.Type)
		require.Equal(t, "Node with ref=[7 k] already exists (nodeID: test:a)", violation.Message)

		count, err := engine.NodeCount(t.Context())
		require.NoError(t, err)
		require.Equal(t, int64(1), count)
	})

	// Control: scalar NaN keeps the existing approved non-reflexive contract
	// that TestNativeBulkUniqueNaNMatchesIndividualWrites pins on this same
	// public path: two NaN writes are both stored.
	t.Run("scalar_nan_control_stays_non_reflexive", func(t *testing.T) {
		engine, schema := nor374UniqueDocEngine(t)

		first, second := math.NaN(), math.NaN()
		require.NoError(t, validatePropertyValueForStorage(first))
		require.False(t, compareValues(first, second), "scalar NaN is unequal to itself under the existing comparator")
		firstKey, keyed, err := indexValueKey(t.Context(), first)
		require.NoError(t, err)
		require.True(t, keyed)
		require.True(t, math.IsNaN(firstKey.(float64)), "scalar NaN keeps its own numeric key")
		secondKey, keyed, err := indexValueKey(t.Context(), second)
		require.NoError(t, err)
		require.True(t, keyed)
		require.False(t, firstKey == secondKey, "a non-reflexive key cannot equal even itself")

		require.NoError(t, nor374CreateDoc(t, engine, "test:a", first))
		require.NoError(t, nor374CreateDoc(t, engine, "test:b", second), "the approved non-reflexive contract accepts both scalar NaN writes")
		count, err := engine.NodeCount(t.Context())
		require.NoError(t, err)
		require.Equal(t, int64(2), count)
		for _, id := range []NodeID{"test:a", "test:b"} {
			stored, err := engine.GetNode(t.Context(), id)
			require.NoError(t, err)
			storedValue, ok := stored.Properties["ref"].(float64)
			require.True(t, ok)
			require.True(t, math.IsNaN(storedValue))
		}

		// No canonical claim can address a non-reflexive key, which is why no
		// duplicate is reported for scalar NaN.
		claim := nor374ReadClaim(t, schema, math.NaN())
		require.True(t, claim.constrained)
		require.True(t, claim.complete)
		require.False(t, claim.found, "the scalar NaN key can never be found through the claim cache")
	})

	// The supported NaN list is decided by the equality owners, not by the
	// claim cache, so the label-scan consumer and the value cache rebuild reach
	// the same decision, and a keyed value still uses the complete cache.
	t.Run("list_nan_consumers_agree_before_and_after_the_rebuild", func(t *testing.T) {
		engine := newTestEngine(t)
		schema := engine.GetSchemaForNamespace("test")
		require.NoError(t, schema.AddUniqueConstraint("doc_ref", "Doc", "ref"))

		first := []any{math.NaN(), "key"}
		second := []any{math.NaN(), "key"}
		require.NoError(t, validatePropertyValueForStorage(first), "a NaN-bearing list is a supported property value")
		require.NoError(t, validatePropertyValueForStorage(second), "a NaN-bearing list is a supported property value")
		_, keyed, err := indexValueKey(t.Context(), first)
		require.NoError(t, err)
		require.False(t, keyed, "a list holding NaN has no reflexive equality key")

		claim := nor374ReadClaim(t, schema, first)
		require.True(t, claim.constrained)
		require.False(t, claim.complete, "without a rebuild the label scan decides")

		require.NoError(t, nor374CreateDoc(t, engine, "test:a", first))
		var scans atomic.Int64
		restoreScanHook := setUniqueConstraintScanHook(func() { scans.Add(1) })
		defer restoreScanHook()
		require.NoError(t, nor374CreateDoc(t, engine, "test:b", second),
			"compareValues keeps list NaN unequal, so the second supported value is admitted")
		require.Equal(t, int64(1), scans.Load(), "the label-scan consumer must be the one that decided")

		count, err := engine.NodeCount(t.Context())
		require.NoError(t, err)
		require.Equal(t, int64(2), count, "both accepted writes are stored")
		nor374RequireStoredNaNList(t, engine, "test:a")
		nor374RequireStoredNaNList(t, engine, "test:b")

		require.NoError(t, RefreshUniqueConstraintValuesForEngine(t.Context(), engine, schema),
			"the rebuild consumer adopts the stored NaN lists the label scan admitted")
		claim = nor374ReadClaim(t, schema, second)
		require.True(t, claim.constrained)
		require.False(t, claim.complete, "an unkeyable value is never answered by the claim cache")
		require.False(t, claim.found)

		require.NoError(t, nor374CreateDoc(t, engine, "test:c", []any{math.NaN(), "key"}),
			"an identical supported value is still admitted after the rebuild")
		count, err = engine.NodeCount(t.Context())
		require.NoError(t, err)
		require.Equal(t, int64(3), count)

		// The rebuilt cache is complete for keyed values: a finite duplicate is
		// now rejected by the cache itself, without a label scan.
		require.NoError(t, nor374CreateDoc(t, engine, "test:d", []any{"key", int64(1)}))
		scans.Store(0)
		err = nor374CreateDoc(t, engine, "test:e", []any{"key", int64(1)})
		violation := asConstraintViolation(t, err)
		require.Equal(t, ConstraintUnique, violation.Type)
		require.Equal(t, "Node with ref=[key 1] already exists (nodeID: test:d)", violation.Message)
		require.Zero(t, scans.Load(), "a keyed value must be decided by the complete claim cache")
		count, err = engine.NodeCount(t.Context())
		require.NoError(t, err)
		require.Equal(t, int64(4), count)
	})
}
