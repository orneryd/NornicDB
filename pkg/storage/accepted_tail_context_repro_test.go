package storage

import (
	"context"
	"errors"
	"sync"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func TestAcceptedPositionalTailRetainsCommittedCounts(t *testing.T) {
	for _, mode := range []string{"cancel_after_durable", "one_shot_decision"} {
		t.Run(mode, func(t *testing.T) {
			engine, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			for _, node := range []*Node{{ID: "tail:a", Labels: []string{"Start"}}, {ID: "tail:b", Labels: []string{"End"}}} {
				_, err := engine.CreateNode(t.Context(), node)
				require.NoError(t, err)
			}
			tx, err := engine.BeginTransaction(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.CreateEdge(t.Context(), &Edge{ID: "tail:e", StartNode: "tail:a", EndNode: "tail:b", Type: "LINK"}))
			requirePositionalChangesPending(t, tx)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			decisions := 0
			if mode == "one_shot_decision" {
				ctx = WithPublicationDecision(ctx, func(_ context.Context, _ PublicationIdentity, action func() error) error {
					decisions++
					if decisions > 1 {
						return errors.New("semantic publication already accepted")
					}
					return action()
				})
			}
			var once sync.Once
			restore := setCommitTailHook(func() {
				if mode == "cancel_after_durable" {
					once.Do(cancel)
				}
			})
			defer restore()
			err = tx.CommitContext(ctx)
			require.NoError(t, err, "durable accepted commit cannot turn into rollback")
			edge, err := engine.GetEdge(t.Context(), "tail:e")
			require.NoError(t, err)
			require.Equal(t, "LINK", edge.Type)
			start, err := engine.EdgeCountByStartLabelInNamespace(context.Background(), "tail", "Start", "LINK")
			require.NoError(t, err)
			end, err := engine.EdgeCountByEndLabelInNamespace(context.Background(), "tail", "End", "LINK")
			require.NoError(t, err)
			require.Equal(t, int64(1), start, "committed edge must have persisted start-label count")
			require.Equal(t, int64(1), end, "committed edge must have persisted end-label count")
			if mode == "one_shot_decision" {
				require.Equal(t, 1, decisions, "accepted metadata tail cannot reenter semantic admission")
			}
		})
	}
}
