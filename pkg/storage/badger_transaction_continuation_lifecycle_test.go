package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Independent child commits may advance an implicit caller's read view while
// it retains writes. A maintenance pass or prefix deletion must not disappear
// from that caller's old write footprint when the read view advances.
func TestIndependentContinuationPreservesLifecycleConflict(t *testing.T) {
	cases := []struct {
		name     string
		apply    func(*BadgerEngine) error
		conflict bool
	}{
		{name: "ordinary continuation"},
		{name: "temporal rebuild", apply: func(b *BadgerEngine) error {
			return b.RebuildTemporalIndexes(context.Background())
		}, conflict: true},
		{name: "same namespace prefix deletion", apply: func(b *BadgerEngine) error {
			_, _, err := b.DeleteByPrefix("test:unrelated-")
			return err
		}, conflict: true},
		{name: "other namespace prefix deletion", apply: func(b *BadgerEngine) error {
			_, _, err := b.DeleteByPrefix("other:")
			return err
		}},
	}
	for _, writeBehind := range []bool{false, true} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("write-behind=%t/%s", writeBehind, tc.name), func(t *testing.T) {
				var b *BadgerEngine
				if writeBehind {
					b = newWriteBehindTestEngine(t, time.Hour)
				} else {
					b = createTestBadgerEngine(t)
				}
				_, err := b.CreateNode(&Node{ID: "test:retained", Labels: []string{"Doc"}, Properties: map[string]any{"v": int64(1)}})
				require.NoError(t, err)
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.SetNamespace("test"))
				require.NoError(t, tx.SetImplicit(true))
				node, err := tx.GetNode("test:retained")
				require.NoError(t, err)
				node.Properties["v"] = int64(2)
				require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
				if tc.apply != nil {
					require.NoError(t, tc.apply(b))
				}

				err = tx.ResumeAfterIndependentCommit()
				want := int64(2)
				if tc.conflict {
					require.ErrorIs(t, err, ErrConflict, "continuation cannot erase lifecycle invalidation of pending writes")
					pending, readErr := tx.GetNode("test:retained")
					require.NoError(t, readErr)
					require.Equal(t, int64(2), pending.Properties["v"], "refused continuation keeps the original pending state")
					require.NoError(t, tx.Rollback())
					want = 1
				} else {
					require.NoError(t, err)
					require.NoError(t, tx.Commit())
				}
				require.NoError(t, b.FlushWriteBehind())
				stored, err := b.GetStoredNode("test:retained")
				require.NoError(t, err)
				require.Equal(t, want, stored.Properties["v"], "a refused continuation must not publish the pending mutation")
			})
		}
	}
}
