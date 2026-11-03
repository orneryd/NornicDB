package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A cancelled context yields no composite key at all: the caller gets the
// context's error and never a partially framed key.
func TestNewCompositeKeyCancellationReturnsNoKey(t *testing.T) {
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()

	key, err := NewCompositeKey(cancelled, "Alice", "Johnson")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, CompositeKey{}, key, "a cancelled preparation returns no key")
	require.Empty(t, key.Hash)
	require.Nil(t, key.Values)
}

// The tuple framing survives the ctx-aware entry point: delimiter-shaped values
// stay distinct, and a repeated tuple stays one key.
func TestNewCompositeKeyKeepsTupleFraming(t *testing.T) {
	left, err := NewCompositeKey(t.Context(), "a|string:b", "c")
	require.NoError(t, err)
	right, err := NewCompositeKey(t.Context(), "a", "b|string:c")
	require.NoError(t, err)
	require.NotEqual(t, left.Hash, right.Hash, "distinct property tuples must not alias before HMAC")

	again, err := NewCompositeKey(t.Context(), "a", "b|string:c")
	require.NoError(t, err)
	require.Equal(t, right.Hash, again.Hash, "the same tuple is the same key")
	require.Equal(t, []interface{}{"a", "b|string:c"}, again.Values)
}

// Schema discovery through the ctx-aware entry points applies the same
// existence and fill rules as the seeks that use them, and reports an absent
// index as a value rather than as an error.
func TestPropertyIndexDiscoveryReportsFillState(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("person_name", "Person", []string{"name"}))

	idx, seekable, err := sm.GetPropertyIndex(t.Context(), "Person", "name")
	require.NoError(t, err)
	require.True(t, seekable, "CREATE INDEX is seekable as soon as it exists")
	require.NotNil(t, idx)
	require.Equal(t, "person_name", idx.Name)

	exists, err := sm.HasPropertyIndex(t.Context(), "Person", "name")
	require.NoError(t, err)
	require.True(t, exists)

	// A constraint's owned index is not seekable until it is filled (#875).
	require.NoError(t, sm.AddUniqueConstraint("rec_uid", "Rec", "uid"))
	idx, seekable, err = sm.GetPropertyIndex(t.Context(), "Rec", "uid")
	require.NoError(t, err)
	require.False(t, seekable, "an unfilled owned index is not seekable")
	require.Nil(t, idx)

	exists, err = sm.HasPropertyIndex(t.Context(), "Rec", "uid")
	require.NoError(t, err)
	require.False(t, exists)

	require.NoError(t, sm.BackfillPropertyIndex("Rec", "uid", nil))
	idx, seekable, err = sm.GetPropertyIndex(t.Context(), "Rec", "uid")
	require.NoError(t, err)
	require.True(t, seekable)
	require.NotNil(t, idx)
	require.Equal(t, "rec_uid", idx.Name)

	exists, err = sm.HasPropertyIndex(t.Context(), "Rec", "uid")
	require.NoError(t, err)
	require.True(t, exists)

	idx, seekable, err = sm.GetPropertyIndex(t.Context(), "Person", "missing")
	require.NoError(t, err)
	require.False(t, seekable, "an unknown property is absent, not an error")
	require.Nil(t, idx)
}

// A lookup waiting on another goroutine's schema write leaves with its own
// cancellation instead of waiting for the holder, and a retry after the holder
// releases sees the schema.
func TestPropertyIndexLookupCancelsWhileSchemaLockHeld(t *testing.T) {
	type result struct {
		index *PropertyIndex
		found bool
		err   error
	}
	for _, name := range []string{"get", "has"} {
		t.Run(name, func(t *testing.T) {
			sm := NewSchemaManager()
			require.NoError(t, sm.AddPropertyIndex("person_name", "Person", []string{"name"}))
			read := func(ctx context.Context) result {
				if name == "get" {
					index, found, err := sm.GetPropertyIndex(ctx, "Person", "name")
					return result{index, found, err}
				}
				found, err := sm.HasPropertyIndex(ctx, "Person", "name")
				return result{found: found, err: err}
			}
			sm.mu.Lock()
			var released sync.Once
			release := func() { released.Do(sm.mu.Unlock) }
			base, cancel := context.WithCancel(t.Context())
			ctx := &indexWaitContext{Context: base, entered: make(chan struct{})}
			returned, done := make(chan result, 1), make(chan struct{})
			t.Cleanup(func() {
				cancel()
				release()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("discovery did not join after holder release")
				}
			})
			go func() { defer close(done); returned <- read(ctx) }()
			select {
			case <-ctx.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("discovery never reached the actual held-lock wait")
			}
			cancel()
			var canceled result
			select {
			case canceled = <-returned:
			case <-time.After(5 * time.Second):
				t.Fatal("canceled discovery did not return while the holder retained its lock")
			}
			require.ErrorIs(t, canceled.err, context.Canceled)
			require.Nil(t, canceled.index)
			require.False(t, canceled.found)
			release()
			healthy := read(t.Context())
			require.NoError(t, healthy.err)
			require.True(t, healthy.found)
			if name == "get" {
				require.NotNil(t, healthy.index)
				require.Equal(t, "person_name", healthy.index.Name)
			}
		})
	}
}

// The arity-1 scan is the causal owner of the lookup: it observes the caller's
// context per candidate index, so a cancellation is an error rather than a
// silent "no index", and a cancelled scan exposes no index at all.
func TestPropertyIndexScanObservesCancellation(t *testing.T) {
	sm := NewSchemaManager()
	for i := 0; i < 32; i++ {
		require.NoError(t, sm.AddPropertyIndex(fmt.Sprintf("other_%d", i), fmt.Sprintf("Other%d", i), []string{"value"}))
	}

	// Nothing matches Person/name, so the scan has to visit every candidate.
	idx, seekable, err := sm.GetPropertyIndex(newIndexPollCancelContext(t, 4), "Person", "name")
	require.ErrorIs(t, err, context.Canceled, "the scan must observe cancellation instead of reporting an absent index")
	require.Nil(t, idx)
	require.False(t, seekable)

	// Positive control: the same absence under a live context is not an error.
	idx, seekable, err = sm.GetPropertyIndex(t.Context(), "Person", "name")
	require.NoError(t, err)
	require.False(t, seekable)
	require.Nil(t, idx)

	// Retry control: after the cancellation, the index becomes discoverable.
	require.NoError(t, sm.AddPropertyIndex("person_name", "Person", []string{"name"}))
	idx, seekable, err = sm.GetPropertyIndex(t.Context(), "Person", "name")
	require.NoError(t, err)
	require.True(t, seekable)
	require.NotNil(t, idx)
	require.Equal(t, "person_name", idx.Name)
}

// Statement lookups carry their request into key preparation: a cancelled
// request returns its error and no IDs.
func TestCompositeIndexLookupsRejectCancelledContext(t *testing.T) {
	idx := &CompositeIndex{
		Name:        "pair",
		Label:       "Pair",
		Properties:  []string{"left", "right"},
		fullIndex:   map[string][]NodeID{},
		prefixIndex: map[string][]NodeID{},
	}
	require.NoError(t, idx.IndexNode(t.Context(), "pair:1", map[string]interface{}{"left": "a", "right": "b"}))

	ids, err := idx.LookupFull(t.Context(), "a", "b")
	require.NoError(t, err)
	require.Equal(t, []NodeID{"pair:1"}, ids)
	ids, err = idx.LookupPrefix(t.Context(), "a")
	require.NoError(t, err)
	require.Equal(t, []NodeID{"pair:1"}, ids)

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	ids, err = idx.LookupFull(cancelled, "a", "b")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, ids)
	ids, err = idx.LookupPrefix(cancelled, "a")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, ids)
}

// A formatter is ordinary supported input to the public Go constructor. Count
// actual formatting work so cancellation cannot return a partial key after
// continuing through the remaining tuple values.
type observedCompositeKeyValue struct {
	formatted *int
	value     int
}

func (v observedCompositeKeyValue) String() string { (*v.formatted)++; return fmt.Sprint(v.value) }

func TestNewCompositeKeyStopsFormattingAfterCancellation(t *testing.T) {
	formatted := 0
	values := []interface{}{observedCompositeKeyValue{&formatted, 1}, observedCompositeKeyValue{&formatted, 2}, observedCompositeKeyValue{&formatted, 3}}
	ctx := newIndexPollCancelContext(t, 3)
	key, err := NewCompositeKey(ctx, values...)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, CompositeKey{}, key)
	require.Equal(t, 1, formatted, "cancellation must stop before formatting the next tuple value")
	select {
	case <-ctx.Done():
	default:
		t.Fatal("the probe must cancel the real context, not invent Err")
	}
	healthy, err := NewCompositeKey(t.Context(), values...)
	require.NoError(t, err)
	require.Equal(t, 4, formatted)
	again, err := NewCompositeKey(t.Context(), values...)
	require.NoError(t, err)
	require.Equal(t, healthy.Hash, again.Hash)
	require.Equal(t, values, healthy.Values)
}

// Composite-index and constraint discovery are request-bearing reads: a
// discovery waiting on another goroutine's schema write leaves with its own
// cancellation instead of waiting for the holder, keeps no partial answer, and
// a healthy retry after the holder releases still finds the schema.
func TestSchemaDiscoveryCancelsWhileSchemaLockHeld(t *testing.T) {
	type result struct {
		indexes     []*CompositeIndex
		constraints []Constraint
		err         error
	}
	for _, target := range []string{"composite indexes", "constraints"} {
		t.Run(target, func(t *testing.T) {
			sm := NewSchemaManager()
			require.NoError(t, sm.AddPropertyIndex("pair_ab", "Pair", []string{"a", "b"}))
			require.NoError(t, sm.AddUniqueConstraint("pair_key", "Pair", "k"))
			read := func(ctx context.Context) result {
				if target == "composite indexes" {
					indexes, err := sm.SeekableCompositeIndexesForLabel(ctx, "Pair")
					return result{indexes: indexes, err: err}
				}
				constraints, err := sm.GetConstraintsForLabels(ctx, []string{"Pair"})
				return result{constraints: constraints, err: err}
			}
			sm.mu.Lock()
			var released sync.Once
			release := func() { released.Do(sm.mu.Unlock) }
			base, cancel := context.WithCancel(t.Context())
			ctx := &indexWaitContext{Context: base, entered: make(chan struct{})}
			returned, done := make(chan result, 1), make(chan struct{})
			t.Cleanup(func() {
				cancel()
				release()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("discovery did not join after holder release")
				}
			})
			go func() { defer close(done); returned <- read(ctx) }()
			select {
			case <-ctx.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("discovery never reached the actual held-lock wait")
			}
			cancel()
			var canceled result
			select {
			case canceled = <-returned:
			case <-time.After(5 * time.Second):
				t.Fatal("canceled discovery did not return while the holder retained its lock")
			}
			require.ErrorIs(t, canceled.err, context.Canceled)
			require.Nil(t, canceled.indexes)
			require.Nil(t, canceled.constraints)
			release()
			healthy := read(t.Context())
			require.NoError(t, healthy.err)
			if target == "composite indexes" {
				require.Len(t, healthy.indexes, 1)
				require.Equal(t, "pair_ab", healthy.indexes[0].Name)
			} else {
				require.Len(t, healthy.constraints, 1)
				require.Equal(t, "pair_key", healthy.constraints[0].Name)
			}
		})
	}
}

// Discovery enumerates every declared constraint or index, so a cancellation
// that arrives mid-enumeration must end it: the caller gets its context's error
// and no partially collected list, and the next live call still sees the whole
// schema.
func TestSchemaDiscoveryStopsMidEnumeration(t *testing.T) {
	const population = 2 * bulkReadCancellationInterval
	t.Run("constraints", func(t *testing.T) {
		sm := NewSchemaManager()
		for i := 0; i < population; i++ {
			require.NoError(t, sm.AddConstraint(Constraint{
				Name: fmt.Sprintf("other_%d", i), Type: ConstraintExists,
				Label: fmt.Sprintf("Other%d", i), Properties: []string{"value"},
			}))
		}
		// Nothing matches Wanted, so the enumeration must visit every entry.
		constraints, err := sm.GetConstraintsForLabels(newIndexPollCancelContext(t, 2), []string{"Wanted"})
		require.ErrorIs(t, err, context.Canceled, "the enumeration must observe cancellation mid-scan")
		require.Nil(t, constraints, "a canceled enumeration returns no partial candidate list")

		// Positive control: the same population under a live context is empty,
		// not an error, and a matching constraint is still discoverable.
		constraints, err = sm.GetConstraintsForLabels(t.Context(), []string{"Wanted"})
		require.NoError(t, err)
		require.Empty(t, constraints)
		require.NoError(t, sm.AddConstraint(Constraint{Name: "wanted_value", Type: ConstraintExists, Label: "Wanted", Properties: []string{"value"}}))
		constraints, err = sm.GetConstraintsForLabels(t.Context(), []string{"Wanted"})
		require.NoError(t, err)
		require.Len(t, constraints, 1)
		require.Equal(t, "wanted_value", constraints[0].Name)
	})
	t.Run("composite indexes", func(t *testing.T) {
		sm := NewSchemaManager()
		for i := 0; i < population; i++ {
			require.NoError(t, sm.AddPropertyIndex(fmt.Sprintf("other_%d", i), fmt.Sprintf("Other%d", i), []string{"value"}))
		}
		indexes, err := sm.SeekableCompositeIndexesForLabel(newIndexPollCancelContext(t, 2), "Wanted")
		require.ErrorIs(t, err, context.Canceled, "the enumeration must observe cancellation mid-scan")
		require.Nil(t, indexes, "a canceled enumeration returns no partial candidate list")

		indexes, err = sm.SeekableCompositeIndexesForLabel(t.Context(), "Wanted")
		require.NoError(t, err)
		require.Empty(t, indexes)
		require.NoError(t, sm.AddPropertyIndex("wanted_pair", "Wanted", []string{"a", "b"}))
		indexes, err = sm.SeekableCompositeIndexesForLabel(t.Context(), "Wanted")
		require.NoError(t, err)
		require.Len(t, indexes, 1)
		require.Equal(t, "wanted_pair", indexes[0].Name)
	})
}

// The longest prefix a complete arity-N insert files is the same key as its
// full key, and both are the canonical composite-key digest of the same values.
// IndexNode stages the full digest once and reuses it for that prefix, so the
// reuse cannot change a stored key hash.
func TestCompositeIndexFinalPrefixKeepsFullKeyHash(t *testing.T) {
	properties := map[string]interface{}{"country": "US", "city": "NYC", "zipcode": int64(10001)}
	index := &CompositeIndex{
		Name:        "location",
		Label:       "User",
		Properties:  []string{"country", "city", "zipcode"},
		values:      make(map[interface{}][]NodeID),
		keysDirty:   true,
		fullIndex:   make(map[string][]NodeID),
		prefixIndex: make(map[string][]NodeID),
	}
	require.NoError(t, index.IndexNode(t.Context(), "user:1", properties))

	// The values are the canonical ones the index files, so these are the exact
	// digests a caller's lookup prepares for the same property map.
	values, err := compositeIndexValues(t.Context(), index, properties)
	require.NoError(t, err)
	require.Len(t, values, len(index.Properties))

	full, err := NewCompositeKey(t.Context(), values...)
	require.NoError(t, err)
	require.Equal(t, []NodeID{"user:1"}, index.fullIndex[full.Hash], "the full key is the canonical digest of every value")
	require.Equal(t, []NodeID{"user:1"}, index.prefixIndex[full.Hash], "the full key is also its own longest prefix")

	for i := 1; i < len(values); i++ {
		prefix, err := NewCompositeKey(t.Context(), values[:i]...)
		require.NoError(t, err)
		require.Equal(t, []NodeID{"user:1"}, index.prefixIndex[prefix.Hash], "prefix %d is the canonical digest of its values", i)
	}
}
