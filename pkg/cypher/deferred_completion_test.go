package cypher

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type deferredCompletionHandler struct {
	entered chan struct{}
	release chan struct{}
}

func (h *deferredCompletionHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *deferredCompletionHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "slow query" || r.Message == "query rejected" {
		select {
		case h.entered <- struct{}{}:
		default:
		}
		<-h.release
	}
	return nil
}
func (h *deferredCompletionHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *deferredCompletionHandler) WithGroup(string) slog.Handler      { return h }

func TestExecuteDeferredLoggingFinalOutcome(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, outcome := range []string{"terminate", "cancel", "rejection", "committed_write"} {
			if (explicit && outcome == "committed_write") || (!explicit && outcome == "rejection") {
				continue
			}
			t.Run(outcome+map[bool]string{false: "/auto", true: "/explicit"}[explicit], func(t *testing.T) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
				session, observer := NewStorageExecutor(store), NewStorageExecutor(store)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				ctx = WithRequestIdentity(ctx, &RequestIdentity{Connection: ClientConnection{ID: "deferred-completion"}})
				if explicit {
					_, err := session.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				h := &deferredCompletionHandler{entered: make(chan struct{}, 1), release: make(chan struct{})}
				defer func() {
					select {
					case <-h.release:
					default:
						close(h.release)
					}
				}()
				session.SetLogger(slog.New(h))
				forceSlowQueryLogging(t, session)
				query := "MATCH (n) RETURN n"
				if outcome == "rejection" {
					query = "RETURN unknownDeferredVariable"
				}
				if outcome == "committed_write" {
					query = "CREATE (:DeferredCommitted)"
				}
				type answer struct {
					result *ExecuteResult
					err    error
				}
				finished := make(chan answer, 1)
				go func() { result, err := session.Execute(ctx, query, nil); finished <- answer{result, err} }()
				select {
				case <-h.entered:
				case <-time.After(10 * time.Second):
					t.Fatal("logger did not start")
				}
				if outcome == "cancel" {
					cancel()
				} else {
					listed, err := observer.Execute(t.Context(), "SHOW TRANSACTIONS YIELD transactionId, connectionId WHERE connectionId = 'deferred-completion' RETURN transactionId", nil)
					require.NoError(t, err)
					require.Len(t, listed.Rows, 1)
					_, err = observer.Execute(t.Context(), "TERMINATE TRANSACTION $id", map[string]interface{}{"id": listed.Rows[0][0]})
					require.NoError(t, err)
				}
				close(h.release)
				select {
				case got := <-finished:
					if outcome == "committed_write" {
						require.NoError(t, got.err)
					} else {
						require.Nil(t, got.result)
						if outcome == "cancel" {
							require.ErrorIs(t, got.err, context.Canceled)
						} else {
							require.Error(t, got.err)
							require.Contains(t, statusText(got.err), "Neo.ClientError.Transaction.Terminated")
						}
					}
				case <-time.After(10 * time.Second):
					t.Fatal("Execute did not finish after logging")
				}
				if explicit {
					_, _ = session.Execute(t.Context(), "ROLLBACK", nil)
				}
			})
		}
	}
}
