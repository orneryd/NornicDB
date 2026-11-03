package storage

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// normalizationPollCounter records every observation a call makes of its
// caller lifetime. It never cancels: it is the work-count control for the
// healthy path.
type normalizationPollCounter struct {
	context.Context
	polls int
}

func (c *normalizationPollCounter) Err() error {
	c.polls++
	return c.Context.Err()
}

// normalizationPollCancelContext reports the state its inner context really
// has, then closes that real Done channel on the chosen observation, so the
// following observation reports the cancellation and nothing invents an error
// from a live context.
type normalizationPollCancelContext struct {
	context.Context
	cancel   context.CancelFunc
	polls    int
	cancelAt int
}

func newNormalizationPollCancelContext(t *testing.T, cancelAt int) *normalizationPollCancelContext {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	return &normalizationPollCancelContext{Context: ctx, cancel: cancel, cancelAt: cancelAt}
}

func (c *normalizationPollCancelContext) Err() error {
	c.polls++
	err := c.Context.Err()
	if c.polls == c.cancelAt {
		c.cancel()
	}
	return err
}

// normalizationFlatList is the supported flat property list a wire or Cypher
// parameter decodes to: a slice of strings, one element per property value.
func normalizationFlatList(count int) []interface{} {
	values := make([]interface{}, count)
	for i := range values {
		values[i] = "value-" + strconv.Itoa(i)
	}
	return values
}

// schemaIndexPostings renders a composite index's filing state as comparable
// text (fmt orders map keys), so a canceled or failed preparation can be shown
// to have filed nothing and to have left the dirty flag and fill state alone.
func schemaIndexPostings(index *CompositeIndex) string {
	index.mu.RLock()
	defer index.mu.RUnlock()
	var out strings.Builder
	fmt.Fprintf(&out, "values:%v\nfull:%v\nprefix:%v\ndirty:%t\nunfilled:%t",
		index.values, index.fullIndex, index.prefixIndex, index.keysDirty, index.unfilled.Load())
	return out.String()
}

// pollsForNormalization returns the observations one healthy lookup of value
// makes on its caller lifetime.
func pollsForNormalization(t *testing.T, read func(context.Context) ([]NodeID, error)) int {
	t.Helper()
	counter := &normalizationPollCounter{Context: t.Context()}
	ids, err := read(counter)
	require.NoError(t, err)
	require.NotEmpty(t, ids)
	return counter.polls
}

// TestValueKeyNormalizationPollsCallerContext is the work-count proof that a
// flat list's canonical key text is prepared under the caller's lifetime: the
// number of observations grows with the element count at the bounded kernel's
// interval, so a cancellation during preparation is seen within one block
// instead of after the whole value.
func TestValueKeyNormalizationPollsCallerContext(t *testing.T) {
	const count = 16384
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("normalization_polls", "Item", []string{"p"}))
	value := normalizationFlatList(count)
	require.NoError(t, sm.PropertyIndexInsert("Item", "p", "node", value))

	polls := pollsForNormalization(t, func(ctx context.Context) ([]NodeID, error) {
		return sm.PropertyIndexLookup(ctx, "Item", "p", value)
	})
	require.GreaterOrEqual(t, polls, count/bulkReadCancellationInterval,
		"flat list preparation must observe the caller's lifetime at least once per %d units (observed %d)",
		bulkReadCancellationInterval, polls)
}

// TestValueKeyNormalizationPollsWithinMapsAndNesting covers the traversal paths
// a flat list does not exercise: nested lists and the sorted map entries. Both
// share the same cancellation budget, so both observe the caller's lifetime at
// the same granularity.
func TestValueKeyNormalizationPollsWithinMapsAndNesting(t *testing.T) {
	const entries = 4096
	object := make(map[string]interface{}, entries)
	nested := make([]interface{}, 0, entries)
	for i := 0; i < entries; i++ {
		object[fmt.Sprintf("key-%06d", i)] = []interface{}{int64(i), []interface{}{"leaf"}}
		nested = append(nested, []interface{}{int64(i)})
	}
	for name, value := range map[string]interface{}{"map": object, "nested list": nested} {
		t.Run(name, func(t *testing.T) {
			counter := &normalizationPollCounter{Context: t.Context()}
			key, ok, err := indexValueKey(counter, value)
			require.NoError(t, err)
			require.True(t, ok)
			require.NotEmpty(t, key)
			require.GreaterOrEqual(t, counter.polls, entries/bulkReadCancellationInterval,
				"%s preparation must observe the caller's lifetime at least once per %d units (observed %d)",
				name, bulkReadCancellationInterval, counter.polls)
		})
	}
}

// TestPropertyIndexLookupCanceledFlatListPreparation cancels a real context
// while a flat list's key is being prepared: the lookup returns the caller's
// error and no ids, files nothing, and the same lookup on a live lifetime still
// finds the node.
func TestPropertyIndexLookupCanceledFlatListPreparation(t *testing.T) {
	const count = 4096
	sm := NewSchemaManager()
	require.NoError(t, sm.AddPropertyIndex("normalization_cancel", "Item", []string{"p"}))
	value := normalizationFlatList(count)
	require.NoError(t, sm.PropertyIndexInsert("Item", "p", "node", value))
	index, exists := sm.GetCompositeIndex("normalization_cancel")
	require.True(t, exists)

	read := func(ctx context.Context) ([]NodeID, error) {
		return sm.PropertyIndexLookup(ctx, "Item", "p", value)
	}
	// Cancel halfway through the observations the healthy path makes, which is
	// inside the value's preparation, not before or after it.
	cancelAt := pollsForNormalization(t, read) / 2
	before := schemaIndexPostings(index)
	ids, err := read(newNormalizationPollCancelContext(t, cancelAt))
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, ids, "a canceled preparation yields no partial result")
	require.Equal(t, before, schemaIndexPostings(index), "a canceled preparation files nothing")

	ids, err = read(t.Context())
	require.NoError(t, err)
	require.Equal(t, []NodeID{"node"}, ids, "the healthy lifetime still finds the filed node")
	require.Equal(t, before, schemaIndexPostings(index))
}

// TestCompositeIndexLookupCanceledFlatListPreparation is the same contract for
// the composite index's full and prefix lookups, whose arity-2 keys prepare the
// leading list value the same way inserts do.
func TestCompositeIndexLookupCanceledFlatListPreparation(t *testing.T) {
	const count = 4096
	sm := NewSchemaManager()
	require.NoError(t, sm.AddCompositeIndex("normalization_pair", "Pair", []string{"p", "q"}))
	index, exists := sm.GetCompositeIndex("normalization_pair")
	require.True(t, exists)
	value := normalizationFlatList(count)
	require.NoError(t, index.IndexNode(t.Context(), "pair-node", map[string]interface{}{"p": value, "q": int64(1)}))

	for name, read := range map[string]func(context.Context) ([]NodeID, error){
		"full": func(ctx context.Context) ([]NodeID, error) { return index.LookupFull(ctx, value, int64(1)) },
		"prefix": func(ctx context.Context) ([]NodeID, error) {
			return index.LookupPrefix(ctx, value)
		},
	} {
		t.Run(name, func(t *testing.T) {
			cancelAt := pollsForNormalization(t, read) / 2
			before := schemaIndexPostings(index)
			ids, err := read(newNormalizationPollCancelContext(t, cancelAt))
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, ids, "a canceled preparation yields no partial result")
			require.Equal(t, before, schemaIndexPostings(index), "a canceled preparation files nothing")

			ids, err = read(t.Context())
			require.NoError(t, err)
			require.Equal(t, []NodeID{"pair-node"}, ids)
			require.Equal(t, before, schemaIndexPostings(index))
		})
	}
}

// TestIndexNodeCanceledKeyPreparationFilesNothing cancels a real context while
// a large property value's canonical key is being prepared. The attempt returns
// the caller's error and leaves the whole filing state exactly as it was —
// values, full and prefix populations, dirty flag and fill state — and a retry
// on a live lifetime files the node. Both the scalar (values) and composite
// (full/prefix) filing paths are covered because both stage keys before the
// first posting write.
func TestIndexNodeCanceledKeyPreparationFilesNothing(t *testing.T) {
	const count = 4096
	value := normalizationFlatList(count)
	for _, test := range []struct {
		name          string
		properties    []string
		lookup        []interface{}
		propertiesFor func(node int) map[string]interface{}
	}{
		{
			name:       "scalar",
			properties: []string{"p"},
			lookup:     []interface{}{value},
			propertiesFor: func(int) map[string]interface{} {
				return map[string]interface{}{"p": value}
			},
		},
		{
			name:       "composite",
			properties: []string{"p", "q"},
			lookup:     []interface{}{value, int64(1)},
			propertiesFor: func(int) map[string]interface{} {
				return map[string]interface{}{"p": value, "q": int64(1)}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			index := &CompositeIndex{
				Name:        "cancel_" + test.name,
				Label:       "Item",
				Properties:  test.properties,
				values:      make(map[interface{}][]NodeID),
				keysDirty:   true,
				fullIndex:   make(map[string][]NodeID),
				prefixIndex: make(map[string][]NodeID),
			}
			require.NoError(t, index.IndexNode(t.Context(), "item:1", test.propertiesFor(1)))
			// A healthy attempt on the same value counts the observations the
			// preparation makes on its caller lifetime, so the cancellation lands
			// inside the preparation rather than before or after it.
			counter := &normalizationPollCounter{Context: t.Context()}
			require.NoError(t, index.IndexNode(counter, "item:2", test.propertiesFor(2)))
			require.Positive(t, counter.polls)

			before := schemaIndexPostings(index)
			err := index.IndexNode(newNormalizationPollCancelContext(t, counter.polls/2), "item:3", test.propertiesFor(3))
			require.ErrorIs(t, err, context.Canceled, "cancellation during preparation is the caller's error")
			require.Equal(t, before, schemaIndexPostings(index), "a canceled preparation files nothing")

			require.NoError(t, index.IndexNode(t.Context(), "item:3", test.propertiesFor(3)))
			ids, err := index.LookupFull(t.Context(), test.lookup...)
			require.NoError(t, err)
			require.Equal(t, []NodeID{"item:1", "item:2", "item:3"}, ids, "the healthy retry files the node")
		})
	}
}
