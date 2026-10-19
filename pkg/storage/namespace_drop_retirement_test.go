package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSolR3LifetimeMultipleRetirements(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprint(buffered), func(t *testing.T) {
			dir := t.TempDir()
			b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true, WriteBehind: buffered, WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			defer func() { require.NoError(t, b.Close()) }()
			unbound, err := b.BeginTransaction()
			require.NoError(t, err)
			_, _, err = b.DeleteByPrefix("a:")
			require.NoError(t, err)
			freshA, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, freshA.SetImplicit(buffered))
			_, _, err = b.DeleteByPrefix("b:")
			require.NoError(t, err)
			_, err = freshA.CreateNode(&Node{ID: "a:fresh"})
			require.NoError(t, err)
			require.NoError(t, freshA.Commit(), "later B retirement must preserve A's new lifetime, including lazy binding")
			_, err = unbound.CreateNode(&Node{ID: "a:stale"})
			require.NoError(t, err)
			require.ErrorIs(t, unbound.Commit(), ErrConflict)
			oldA, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, oldA.SetNamespace("a"))
			_, _, err = b.DeleteByPrefix("a:")
			require.NoError(t, err)
			require.ErrorIs(t, oldA.Commit(), ErrConflict)
			require.NoError(t, b.Close())
			b, err = NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true, WriteBehind: buffered})
			require.NoError(t, err)
			newA, err := b.BeginTransaction()
			require.NoError(t, err)
			_, err = newA.CreateNode(&Node{ID: "a:reopened"})
			require.NoError(t, err)
			require.NoError(t, newA.Commit())
			require.NoError(t, b.FlushWriteBehind())
			count, err := NewNamespacedEngine(b, "a").NodeCount()
			require.NoError(t, err)
			require.Equal(t, int64(1), count)
		})
	}
}
