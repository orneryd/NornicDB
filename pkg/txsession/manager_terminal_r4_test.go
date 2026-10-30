package txsession

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txphase"
	"github.com/stretchr/testify/require"
)

type r4TerminalLog struct{ entered, release chan struct{} }

func (*r4TerminalLog) Enabled(context.Context, slog.Level) bool { return true }
func (h *r4TerminalLog) Handle(_ context.Context, record slog.Record) error {
	commit := false
	record.Attrs(func(a slog.Attr) bool {
		if a.Key == "query" && strings.EqualFold(strings.TrimSpace(a.Value.String()), "COMMIT") {
			commit = true
		}
		return true
	})
	if commit {
		close(h.entered)
		<-h.release
	}
	return nil
}
func (h *r4TerminalLog) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *r4TerminalLog) WithGroup(string) slog.Handler      { return h }

func TestManagerR4TerminalRetirement(t *testing.T) {
	for _, operation := range []string{"delete", "canceled commit", "failed commit overlaps close"} {
		t.Run(operation, func(t *testing.T) {
			engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			executor := cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
			manager := NewManager(time.Minute, func(string) (*cypher.StorageExecutor, error) { return executor, nil })
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			if operation == "delete" {
				session, err := manager.Open(context.Background(), "nornic")
				require.NoError(t, err)
				manager.Delete(session.ID)
				require.False(t, executor.HasActiveTransaction(), "Delete forgot a live native transaction")
				return
			}
			// Use the public two-step API available on the rejected head; this
			// schedule starts after admission and does not exercise the open gap.
			session, err := manager.OpenForOwner(context.Background(), "nornic", "owner")
			require.NoError(t, err)
			lease, accepted := manager.AcquireForOwner(session.ID, "owner")
			require.True(t, accepted)
			defer lease.Release()
			if operation == "canceled commit" {
				// The merged release assigns HTTP rejection to txphase admission;
				// an admitted terminal commit already owns its actual result.
				ctx, cancel := txphase.WithExecutionDeadline(context.Background(), time.Minute)
				cancel()
				_, err = lease.CommitAndDelete(ctx)
				require.ErrorIs(t, err, context.Canceled)
				require.False(t, executor.HasActiveTransaction(), "failed COMMIT removed its session before actual rollback")
				return
			}

			_, err = lease.ExecuteInSession(context.Background(), "THIS IS NOT CYPHER", nil)
			require.Error(t, err)
			gate := &r4TerminalLog{make(chan struct{}), make(chan struct{})}
			var once sync.Once
			release := func() { once.Do(func() { close(gate.release) }) }
			defer release()
			executor.SetLogger(slog.New(gate))
			executor.SetSlowQueryThreshold(time.Nanosecond)
			committed := make(chan error, 1)
			go func() {
				_, commitErr := lease.CommitAndDelete(context.Background())
				lease.Release()
				committed <- commitErr
			}()
			select {
			case <-gate.entered:
			case <-time.After(time.Second):
				t.Fatal("actual terminal execution did not reach its completion observer")
			}
			closed := make(chan error, 1)
			go func() { closed <- manager.CloseContext(context.Background()) }()
			require.Eventually(t, func() bool { _, found := manager.Get(lease.Session.ID); return !found }, time.Second, time.Millisecond)
			release()
			select {
			case err := <-committed:
				require.Error(t, err, "failed transaction must still report its COMMIT outcome")
			case <-time.After(time.Second):
				t.Fatal("terminal execution did not finish")
			}
			select {
			case err := <-closed:
				require.NoError(t, err, "shutdown must observe actual terminal completion before attempting another rollback")
			case <-time.After(time.Second):
				t.Fatal("shutdown did not join actual terminal completion")
			}
		})
	}
}
