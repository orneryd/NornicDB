package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
)

// Visibility changes while a visitor consumes the population must not become
// permanent omissions from a later read of the same physical snapshot.
func TestTransactionLabelPopulationSurvivesSuppressionDuringScan(t *testing.T) {
	for _, projection := range [][]string{nil, {"value"}} {
		name := "full"
		if projection != nil {
			name = "projected"
		}
		t.Run(name, func(t *testing.T) {
			for _, restoreInScan := range []bool{false, true} {
				timing := "restore_after_scan"
				if restoreInScan {
					timing = "restore_during_scan"
				}
				t.Run(timing, func(t *testing.T) {
					engine := newTestMemoryEngine(t)
					ctx := t.Context()
					expected := []NodeID{"test:a", "test:b", "test:c"}
					for _, id := range expected {
						_, err := engine.CreateNode(ctx, &Node{ID: id, Labels: []string{"Population"}, Properties: map[string]any{"value": int64(1)}})
						require.NoError(t, err)
					}
					tx, err := engine.BeginTransaction(ctx)
					require.NoError(t, err)
					t.Cleanup(func() { _ = tx.Rollback() })
					require.NoError(t, tx.SetNamespace("test"))
					var delivered []NodeID
					require.NoError(t, tx.StreamNodesByLabelProjected(ctx, "Population", projection, func(n *Node) error {
						delivered = append(delivered, n.ID)
						if n.ID == "test:a" {
							require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, "test:b", true))
						}
						if n.ID == "test:c" && restoreInScan {
							require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, "test:b", false))
						}
						return nil
					}))
					require.Equal(t, []NodeID{"test:a", "test:c"}, delivered)
					if !restoreInScan {
						require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, "test:b", false))
					}
					_, err = tx.GetNode(ctx, "test:b")
					require.NoError(t, err, "the pinned body remains present and is visible again")
					delivered = nil
					require.NoError(t, tx.StreamNodesByLabelProjected(ctx, "Population", projection, func(n *Node) error { delivered = append(delivered, n.ID); return nil }))
					require.ElementsMatch(t, expected, delivered, "a live visibility decision must not remove a pinned population member")
				})
			}
		})
	}
}

// The equality accelerator is a consumer of the same pinned label population.
// It must not turn a cached candidate into an exemption from live visibility.
func TestTransactionEqualityCandidatesFollowLiveSuppression(t *testing.T) {
	for _, initiallyHidden := range []bool{false, true} {
		name := "cached_visible"
		if initiallyHidden {
			name = "cached_hidden"
		}
		t.Run(name, func(t *testing.T) {
			engine := newTestMemoryEngine(t)
			ctx := t.Context()
			id, err := engine.CreateNode(ctx, &Node{ID: "test:one", Labels: []string{"Population"}, Properties: map[string]any{"value": int64(1)}})
			require.NoError(t, err)
			if initiallyHidden {
				require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(id), true))
			}
			tx, err := engine.BeginTransaction(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			require.NoError(t, tx.SetNamespace("test"))
			read := func(want int) {
				t.Helper()
				nodes, complete, err := tx.LookupNodesByEquality(ctx, "Population", map[string]any{"value": int64(1)})
				require.NoError(t, err)
				require.True(t, complete)
				require.Len(t, nodes, want)
			}
			if initiallyHidden {
				read(0)
			} else {
				read(1)
			}
			require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(id), !initiallyHidden))
			if initiallyHidden {
				read(1)
			} else {
				read(0)
			}
			require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(id), initiallyHidden))
			if initiallyHidden {
				read(0)
			} else {
				read(1)
			}
		})
	}
}
