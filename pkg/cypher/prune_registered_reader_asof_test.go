package cypher

// Written by the prune-lockup lane (Claude Opus 5.5), 9 November 2026.
//
// A historical query at the cut of an open transaction still finds that
// version after a prune, also when an older transaction predates the
// queried record (review r4509519740 on #1768). The query reads history
// through a fresh view, so only the registered transaction's floor keeps the
// version it asks for.

import (
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/storage/lifecycle"
	"github.com/stretchr/testify/require"
)

func TestTemporalAsOfAtOpenTransactionCutSurvivesPrune(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{InMemory: true, EngineOptions: storage.EngineOptions{
				RetentionPolicy: storage.RetentionPolicy{MaxVersionsPerKey: 100},
			}}))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			lifecycleConfig := lifecycle.DefaultLifecycleConfig()
			lifecycleConfig.Enabled = true
			lifecycleConfig.MaxVersionsPerKey = 100
			manager := lifecycle.NewMVCCLifecycleManager(lifecycleConfig, base)
			base.SetLifecycleController(manager)
			ctx := context.Background()

			// An open transaction that began before the document existed.
			_, err = base.CreateNode(t.Context(), &storage.Node{ID: "test:other", Labels: []string{"Other"}})
			require.NoError(t, err)
			older, err := base.BeginTransaction(ctx)
			require.NoError(t, err)
			defer func() { _ = older.Rollback() }()
			require.NoError(t, older.SetNamespace("test"))

			write := func(v int) {
				doc := &storage.Node{ID: "test:doc", Labels: []string{"Doc"}, Properties: map[string]any{
					"key": "k", "validFrom": "2020-01-01T00:00:00Z", "validTo": "2099-01-01T00:00:00Z", "v": v,
				}}
				if v == 0 {
					_, err := base.CreateNode(t.Context(), doc)
					require.NoError(t, err)
					return
				}
				require.NoError(t, base.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: doc}))
			}
			for v := 0; v <= 2; v++ {
				write(v)
			}
			head, err := base.GetNodeCurrentHead(ctx, "test:doc")
			require.NoError(t, err)
			// A newer open transaction whose cut sees v=2.
			newer, err := base.BeginTransaction(ctx)
			require.NoError(t, err)
			defer func() { _ = newer.Rollback() }()
			require.NoError(t, newer.SetNamespace("test"))
			for v := 3; v <= 6; v++ {
				write(v)
			}
			_, err = manager.RunPruneNow(ctx, storage.MVCCPruneOptions{MaxVersionsPerKey: 1})
			require.NoError(t, err)

			executor := NewStorageExecutor(base)
			result, err := executor.Execute(ctx,
				"CALL db.temporal.asOf('Doc', 'key', 'k', 'validFrom', 'validTo', '2050-01-01T00:00:00Z', $systemTime, $systemSequence) YIELD node RETURN node.v AS v",
				map[string]interface{}{
					"systemTime":     head.Version.CommitTimestamp.Format(time.RFC3339Nano),
					"systemSequence": int64(head.Version.CommitSequence),
				})
			require.NoError(t, err)
			require.Len(t, result.Rows, 1, "the version at the open transaction's cut was pruned")
			require.EqualValues(t, 2, result.Rows[0][0])
		})
	}
}
