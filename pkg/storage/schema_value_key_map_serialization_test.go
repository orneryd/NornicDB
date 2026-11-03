package storage

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// This component witness observes only the canonical writer's traversal polls,
// excluding ctxsort's separate polls. It cancels an actual lifetime; no error
// is synthesized and no production phase seam is needed.
type mapSerializationContext struct {
	context.Context
	cancel      context.CancelFunc
	directPolls int
	cancelAt    int
}

func (c *mapSerializationContext) Err() error {
	var pcs [16]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	writer, sorting := false, false
	for {
		frame, more := frames.Next()
		writer = writer || strings.HasSuffix(frame.Function, ".writeCompositeIndexKey")
		sorting = sorting || strings.Contains(frame.Function, "/internal/ctxsort.")
		if !more {
			break
		}
	}
	if writer && !sorting {
		c.directPolls++
		if c.directPolls == c.cancelAt {
			c.cancel()
		}
	}
	return c.Context.Err()
}

func TestFlatMapSerializationObservesCancellation(t *testing.T) {
	const entries = 4096
	value := make(map[string]interface{}, entries)
	var expected strings.Builder
	expected.WriteByte('{')
	for i := 0; i < entries; i++ {
		key := fmt.Sprintf("key-%04d", i)
		value[key] = int64(i)
		if i > 0 {
			expected.WriteByte(',')
		}
		expected.WriteString(strconv.Quote(key))
		expected.WriteString(":n" + strconv.Itoa(i))
	}
	expected.WriteByte('}')
	t.Run("healthy exact bytes and both traversal phases", func(t *testing.T) {
		actual, cancel := context.WithCancel(t.Context())
		defer cancel()
		ctx := &mapSerializationContext{Context: actual, cancel: cancel}
		key, ok, err := compositeIndexKeyOf(ctx, value)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, compositeIndexKey(expected.String()), key)
		require.GreaterOrEqual(t, ctx.directPolls, 2*entries/bulkReadCancellationInterval,
			"collection and serialization must each observe the lifetime; sort polls are excluded")
	})
	t.Run("real cancellation at serialization and healthy retry", func(t *testing.T) {
		actual, cancel := context.WithCancel(t.Context())
		defer cancel()
		// Collection consumes exactly entries/interval polls. The next direct
		// writer poll belongs to serialization, independent of map/sort order.
		ctx := &mapSerializationContext{Context: actual, cancel: cancel, cancelAt: entries/bulkReadCancellationInterval + 1}
		key, ok, err := compositeIndexKeyOf(ctx, value)
		require.ErrorIs(t, actual.Err(), context.Canceled, "serialization cancellation phase was reached")
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, ok)
		require.Empty(t, key)
		retry, ok, err := compositeIndexKeyOf(t.Context(), value)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, compositeIndexKey(expected.String()), retry)
	})
}
