// Author: Codex, NOR-180 implicit admission outcome regression, 2026-10-27.
package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txphase"
	"github.com/stretchr/testify/require"
)

// Inline embedding runs after statement execution, immediately before implicit
// commit admission. Canceling there reaches the actual rejected-admission owner
// rather than returning an execution error from the mutating statement.
func TestWALTerminalImplicitAdmissionKeepsCleanup(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(fmt.Sprintf("closed=%v", closed), func(t *testing.T) {
			exec, engine, store := newWALOutcomeExecutor(t)
			ctx, cancel := txphase.WithExecutionDeadline(context.Background(), time.Minute)
			defer cancel()
			embedded := false
			exec.QueryEmbedderBinding().Store(&queryEmbedderStub{
				embedFn: func(context.Context, string) ([]float32, error) {
					embedded = true
					if closed {
						require.NoError(t, engine.wal.Close())
					}
					cancel()
					return []float32{1, 0}, nil
				},
				chunkFn: func(text string, _, _ int) ([]string, error) { return []string{text}, nil },
			})
			committed := false
			engine.commit = func(context.Context, *storage.BadgerTransaction) error {
				committed = true
				return fmt.Errorf("native commit must not follow rejected admission")
			}
			result, err := exec.Execute(ctx, "CREATE (:WalOutcome {content:'admission outcome'}) WITH EMBEDDING", nil)
			require.True(t, embedded)
			require.False(t, committed)
			require.False(t, txphase.CommitAdmitted(ctx))
			require.Nil(t, result)
			require.ErrorIs(t, err, context.Canceled)
			if closed {
				require.ErrorIs(t, err, storage.ErrWALClosed)
				var diagnostic interface{ StatusDiagnostic() localization.Message }
				require.ErrorAs(t, err, &diagnostic)
				require.Contains(t, err.Error(), "WAL tx abort")
			} else {
				require.NotContains(t, err.Error(), "WAL tx abort")
			}
			requireWALOutcomeCount(t, store, 0)
		})
	}
}
