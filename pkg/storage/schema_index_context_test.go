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
	require.NoError(t, idx.IndexNode("pair:1", map[string]interface{}{"left": "a", "right": "b"}))

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
