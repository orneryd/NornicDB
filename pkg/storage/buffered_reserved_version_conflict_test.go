package storage

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A reserved version is not yet an acknowledged image. A frame beginning
// during observer preparation must not overwrite that image after publication.
func TestBufferedReservedVersionDoesNotHideConcurrentUpdate(t *testing.T) {
	for _, implicit := range []bool{false, true} {
		for _, replay := range []bool{false, true} {
			t.Run(fmt.Sprintf("implicit=%t/replayed=%t", implicit, replay), func(t *testing.T) {
				b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
				_, err := b.CreateNode(t.Context(), &Node{ID: "test:counter", Properties: map[string]any{"n": int64(0)}})
				require.NoError(t, err)
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				done := make(chan struct{})
				var commitErr error
				require.NoError(t, b.InitializeStoredEntityObserver("test", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
					return func(StoredEntityPublication) (func(), error) {
						once.Do(func() { close(entered); <-release })
						return nil, nil
					}, nil
				}, nil))
				older, err := b.BeginTransaction(t.Context())
				require.NoError(t, err)
				t.Cleanup(func() { _ = older.Rollback() })
				require.NoError(t, older.SetImplicit(true))
				require.NoError(t, older.SetNamespace("test"))
				original, err := older.GetNode(t.Context(), "test:counter")
				require.NoError(t, err)
				next := CopyNode(original)
				next.Properties["n"] = int64(1)
				require.NoError(t, older.UpdateGraphNode(t.Context(), NodeUpdateRequest{Before: original, After: next, Policy: ClientNodeEdits}))
				go func() { defer close(done); commitErr = older.Commit() }()
				var released sync.Once
				unblock := func() { released.Do(func() { close(release) }) }
				t.Cleanup(func() {
					unblock()
					select {
					case <-done:
					case <-time.After(10 * time.Second):
						t.Error("commit did not settle")
					}
				})
				select {
				case <-entered:
				case <-time.After(10 * time.Second):
					t.Fatal("observer did not enter")
				}
				stale, err := b.BeginTransaction(t.Context())
				require.NoError(t, err)
				defer stale.Rollback()
				require.NoError(t, stale.SetImplicit(implicit))
				require.NoError(t, stale.SetNamespace("test"))
				before, err := stale.GetNode(t.Context(), "test:counter")
				require.NoError(t, err)
				require.Equal(t, int64(0), before.Properties["n"])
				after := CopyNode(before)
				after.Properties["n"] = int64(1)
				require.NoError(t, stale.UpdateGraphNode(t.Context(), NodeUpdateRequest{Before: before, After: after, Policy: ClientNodeEdits}))
				unblock()
				select {
				case <-done:
					require.NoError(t, commitErr)
				case <-time.After(10 * time.Second):
					t.Fatal("older commit did not finish")
				}
				if replay {
					require.NoError(t, b.FlushWriteBehind(context.Background()))
				}
				require.ErrorIs(t, stale.Commit(), ErrConflict, "reserved but unpublished version must not mask the changed predecessor")
				require.NoError(t, b.FlushWriteBehind(context.Background()))
				node, err := b.GetNode(t.Context(), "test:counter")
				require.NoError(t, err)
				require.Equal(t, int64(1), node.Properties["n"])
				// A fresh reader observes the acknowledged image and can advance it.
				bufferedWrite(t, b, func(tx *BadgerTransaction) error {
					before, err := tx.GetNode(t.Context(), "test:counter")
					if err != nil {
						return err
					}
					after := CopyNode(before)
					after.Properties["n"] = int64(2)
					return tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{Before: before, After: after, Policy: ClientNodeEdits})
				})
				require.NoError(t, b.FlushWriteBehind(context.Background()))
				node, err = b.GetNode(t.Context(), "test:counter")
				require.NoError(t, err)
				require.Equal(t, int64(2), node.Properties["n"])
			})
		}
	}
}

func TestBufferedMutationRetainsUnchangedNonReflexiveProperty(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	_, err := b.CreateNode(t.Context(), &Node{ID: "test:nan", Properties: map[string]any{"unchanged": math.Float64frombits(0x7ff8000000000042), "n": int64(0)}})
	require.NoError(t, err)
	bufferedWrite(t, b, func(tx *BadgerTransaction) error {
		before, err := tx.GetNode(t.Context(), "test:nan")
		if err != nil {
			return err
		}
		after := CopyNode(before)
		after.Properties["n"] = int64(1)
		return tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{Before: before, After: after, Policy: ClientNodeEdits})
	})
	require.NoError(t, b.FlushWriteBehind(context.Background()))
	got, err := b.GetNode(t.Context(), "test:nan")
	require.NoError(t, err)
	require.Equal(t, int64(1), got.Properties["n"])
	require.Equal(t, uint64(0x7ff8000000000042), math.Float64bits(got.Properties["unchanged"].(float64)))
}

func TestStoredValueNaNIdentity(t *testing.T) {
	for _, value := range []any{math.Float64frombits(0x7ff8000000000042), math.Float32frombits(0x7fc00042)} {
		require.True(t, sameStoredValue(value, value))
		require.True(t, sameStoredValue(map[string]any{"v": []any{value}}, map[string]any{"v": []any{value}}))
	}
	require.False(t, sameStoredValue(math.Float64frombits(0x7ff8000000000042), math.Float64frombits(0x7ff8000000000043)))
	require.False(t, sameStoredValue(math.Float32frombits(0x7fc00042), math.Float32frombits(0x7fc00043)))
	require.True(t, sameStoredValue(int64(1), float64(1)))
	require.False(t, sameStoredValue(int64(1), float64(2)))
}
