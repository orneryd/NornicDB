// Author: Codex, NOR-180 producer-outward public API regressions, 2026-10-27.
package cypher_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestWALTerminalProducerFlows(t *testing.T) {
	for _, family := range walterminal.FlowFamilies {
		for _, outcome := range walterminal.FlowOutcomes {
			for _, closed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/closed=%v", family, outcome, closed), func(t *testing.T) {
					engine := walterminal.New(t)
					flow := walterminal.NewOutcomeFlow(t, engine, family, outcome, closed)
					exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
					result, err := exec.Execute(context.Background(), flow.Query, nil)
					// Inspect committed storage even when the response is wrong.
					require.Equal(t, flow.CommittedNodes, walterminal.Count(t, engine))
					if !flow.WantError {
						require.NoError(t, err)
						require.NotNil(t, result)
						return
					}
					require.Error(t, err, "logical terminal failure must not become success/counts: result=%+v", result)
					if flow.Primary != nil {
						require.ErrorIs(t, err, flow.Primary)
						code, _ := nornicerrors.Neo4jStatus(flow.Primary)
						got, _ := nornicerrors.Neo4jStatus(err)
						require.Equal(t, code, got)
					}
					if closed {
						require.ErrorIs(t, err, storage.ErrWALClosed)
						var diagnostic interface{ StatusDiagnostic() localization.Message }
						require.ErrorAs(t, err, &diagnostic)
						require.Contains(t, err.Error(), storage.ErrWALClosed.Error())
						if outcome == "commit" {
							walterminal.RequireCommitMarkerFailure(t, err.Error())
						}
					}
				})
			}
		}
	}
}

func TestWALTerminalProducerMultipleOutcomesAndIsolation(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		for _, closed := range []bool{false, true} {
			t.Run(fmt.Sprintf("parallel=%v/closed=%v", parallel, closed), func(t *testing.T) {
				engines := []*walterminal.Engine{walterminal.New(t), walterminal.New(t)}
				flow := walterminal.NewMultipleOutcomeFlow(t, engines, walterminal.MultipleOutcomeOptions{Parallel: parallel, HealthyWAL: !closed})
				outer := walterminal.New(t)
				exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(outer, "nornic"))
				result, err := exec.Execute(context.Background(), flow.Query, nil)
				for _, engine := range engines {
					require.Zero(t, walterminal.Count(t, engine))
				}
				if closed {
					require.Error(t, err, "multiple failed logical outcomes must not become a successful count: %+v", result)
					require.ErrorIs(t, err, context.DeadlineExceeded)
					require.ErrorIs(t, err, memory.ErrLimitExceeded)
					require.ErrorIs(t, err, storage.ErrWALClosed)
					var admission *walterminal.AdmissionError
					require.ErrorAs(t, err, &admission)
					observed := flow.Observed()
					require.Len(t, observed, 2)
					for _, child := range observed {
						require.ErrorIs(t, child, storage.ErrWALClosed)
					}
					_, message := nornicerrors.Neo4jStatus(err)
					require.Equal(t, 2, strings.Count(message, "failed to write WAL tx abort"), "every independently failed append must reach the single renderer")
				} else {
					require.NoError(t, err)
					require.NotNil(t, result)
				}
				// Retention is per invocation; the same executor must recover normally.
				healthy, err := exec.Execute(context.Background(), "CREATE (:WalWire) RETURN 1 AS value", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1)}}, healthy.Rows)
				require.Equal(t, int64(1), walterminal.Count(t, outer))
			})
		}
	}
}

// Commit status, known-rollback identity and the commit-failure message are
// qualified after the append fails. They belong to the returned outcome too.
func TestWALTerminalProducerFinalQualification(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, closed := range []bool{false, true} {
			t.Run(fmt.Sprintf("explicit=%v/closed=%v", explicit, closed), func(t *testing.T) {
				engine := walterminal.New(t)
				primary := &storage.ConstraintViolationError{Type: storage.ConstraintUnique, Label: "WalWire", Properties: []string{"id"}}
				walterminal.RejectCommit(engine, primary, closed)
				child := cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
				name := fmt.Sprintf("nor180.final%d", terminalQualificationID.Add(1))
				var childErr error
				require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{
					Name: name, Signature: name + "() :: (value :: INTEGER)", Mode: cypher.ProcedureModeWrite, OwnsTransactions: true,
					Returns: []cypher.ProcedureColumn{{Name: "value", Type: "INTEGER"}},
				}, func(ctx context.Context, _ *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
					if explicit {
						if _, err := child.Execute(ctx, "BEGIN", nil); err != nil {
							return nil, err
						}
						if _, err := child.Execute(ctx, "CREATE (:WalWire {id:1})", nil); err != nil {
							return nil, err
						}
						_, childErr = child.Execute(ctx, "COMMIT", nil)
					} else {
						_, childErr = child.Execute(ctx, "MERGE (n:WalWire {id:1}) RETURN n", nil)
					}
					return &cypher.ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{int64(0)}}, Stats: &cypher.QueryStats{}}, nil
				}))
				outer := walterminal.New(t)
				exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(outer, "nornic"))
				result, err := exec.Execute(context.Background(), "CALL "+name+"() YIELD value RETURN value", nil)
				require.Zero(t, walterminal.Count(t, engine))
				require.ErrorIs(t, childErr, primary)
				if explicit {
					require.True(t, nornicerrors.IsCommitRolledBack(childErr))
				} else {
					require.True(t, nornicerrors.IsMergeCommitTimeUniqueConflict(childErr))
				}
				if !closed {
					require.NoError(t, err)
					require.NotNil(t, result)
					return
				}
				require.ErrorIs(t, err, primary)
				require.ErrorIs(t, err, storage.ErrWALClosed)
				var violation *storage.ConstraintViolationError
				require.ErrorAs(t, err, &violation)
				require.Same(t, primary, violation)
				require.Equal(t, nornicerrors.IsCommitRolledBack(childErr), nornicerrors.IsCommitRolledBack(err))
				require.Equal(t, nornicerrors.IsMergeCommitTimeUniqueConflict(childErr), nornicerrors.IsMergeCommitTimeUniqueConflict(err))
				childCode, _ := nornicerrors.Neo4jStatus(childErr)
				code, message := nornicerrors.Neo4jStatus(err)
				require.Equal(t, childCode, code)
				require.Contains(t, message, "commit failed")
				require.Contains(t, message, "WAL tx abort")
			})
		}
	}
}

var terminalQualificationID atomic.Uint64
