package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Cancellation occurs in the existing quoted-name preparation owner. USE and
// ORDER preparation must retain that operation's context, including on retry.
func TestPublicUseOrderPreparationCancellation(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			require.Equal(t, parser, config.GetParserType())
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "use_cancel"))
			require.Equal(t, parser, config.GetParserType())
			query := "USE " + strings.Repeat(" ", 32<<20) + "`use_cancel` RETURN 7 AS k ORDER BY k"
			ctx, cancel := newReaderEntryCancelContext(".scanQuotedTokens")
			defer cancel()
			var canceledAt time.Time
			actualCancel := ctx.cancel
			ctx.cancel = func() {
				canceledAt = time.Now()
				actualCancel()
			}
			result, err := e.Execute(ctx, query, nil)
			elapsed := time.Since(canceledAt)
			require.True(t, ctx.reached, "must cancel inside actual public quoted preparation")
			t.Logf("cancellation stack: %v", ctx.frames)
			require.ErrorIs(t, ctx.Context.Err(), context.Canceled)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, result)
			t.Logf("parser=%s bytes=%d postcancel execute=%s", parser, len(query), elapsed)
			// Run the same-executor retry even when the latency assertion fails.
			healthy, err := e.Execute(t.Context(), "USE `use_cancel` RETURN 7 AS k ORDER BY k", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(7)}}, healthy.Rows)
			require.Less(t, elapsed, 100*time.Millisecond, "canceled USE/order preparation continued working")
		})
	}
}

func TestPublicUseOrderLargeWhitespaceHealthy(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			require.Equal(t, parser, config.GetParserType())
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "use_cancel"))
			require.Equal(t, parser, config.GetParserType())
			query := "USE " + strings.Repeat(" ", 32<<20) + "`use_cancel` RETURN 7 AS k ORDER BY k"
			result, err := e.Execute(t.Context(), query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
		})
	}
}

// Exercise the two incoming owners, rather than relying only on cancellation
// before standalone rewriting begins. The keyword-name case has no order-tail
// word, so the standalone rewriter's quick exit leaves it to the keyword owner.
func TestPublicUsePreparationScannerOwnersCancellation(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, tc := range []struct {
				database, suffix, ancestor string
			}{
				{"limit", " ORDER BY k", ".scanStandaloneOrderClauses"},
				{"use", "", ".useGraphNameAt"},
			} {
				t.Run(tc.database, func(t *testing.T) {
					e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), tc.database))
					if tc.database == "use" {
						e = newFabricCallRowsExecutor(t)
						manager := e.dbManager.(*testDatabaseManagerAdapter).manager
						require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "use"}))
					}
					query := "USE GRAPH " + tc.database + " RETURN 7 AS k" + tc.suffix + fmt.Sprintf(" /*use_owner_probe_%d*/", usePreparationProbeID.Add(1))
					ctx, cancel := newReaderEntryCancelContext(".useClauseReferenceEnd")
					defer cancel()
					result, err := e.Execute(ctx, query, nil)
					require.True(t, ctx.reached)
					requireFrameContaining(t, ctx.frames, tc.ancestor)
					require.ErrorIs(t, err, context.Canceled)
					require.Nil(t, result)
					healthy, err := e.Execute(t.Context(), query, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(7)}}, healthy.Rows)
				})
			}
		})
	}
}
