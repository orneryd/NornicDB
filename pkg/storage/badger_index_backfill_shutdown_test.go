package storage

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

type backfillOperationResult struct {
	err      error
	panicked any
	stack    string
}

func runBackfillOperation(operation func() error) <-chan backfillOperationResult {
	done := make(chan backfillOperationResult, 1)
	go func() {
		var result backfillOperationResult
		defer func() {
			if result.panicked = recover(); result.panicked != nil {
				result.stack = string(debug.Stack())
			}
			done <- result
		}()
		result.err = operation()
	}()
	return done
}

func awaitBackfillSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(10 * time.Second):
		t.Fatal("backfill lifecycle did not reach the scheduled pause")
	}
}

func awaitBackfillResult(t *testing.T, done <-chan backfillOperationResult) backfillOperationResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(10 * time.Second):
		t.Fatal("backfill lifecycle did not finish")
		return backfillOperationResult{}
	}
}

// A public DROP or Restore that loses lifecycle admission to Close must not
// restart work. DROP used to pass a read-only open check here, then access a
// handle Close had released. Restore retained its barrier but could register
// a new task after Close had already stopped the backfills.
func TestIndexBackfillPublicRecoveryRejectsShutdown(t *testing.T) {
	for _, operation := range []string{"drop", "restore"} {
		t.Run(operation, func(t *testing.T) {
			b := createTestBadgerEngine(t)
			_, err := b.CreateNode(&Node{ID: "test:old", Labels: []string{"Doc"}})
			require.NoError(t, err)
			markMatrixIndexBuilding(t, b)
			ready, err := b.indexBackfillReady(startupLabelIndex)
			require.NoError(t, err)
			require.False(t, ready, "the recovery must need a label rebuild")

			recovery, resumeRecovery := make(chan struct{}), make(chan struct{})
			stopped, resumeClose := make(chan struct{}), make(chan struct{})
			checked, resumeStart := make(chan struct{}), make(chan struct{})
			var recoveryOnce, closeOnce, startOnce sync.Once
			var releaseRecovery, releaseClose, releaseStart sync.Once
			defer releaseStart.Do(func() { close(resumeStart) })
			defer releaseClose.Do(func() { close(resumeClose) })
			defer releaseRecovery.Do(func() { close(resumeRecovery) })
			restore := setMaintenancePauseHook(func(point string) {
				switch point {
				case pauseIndexBackfillRecovery:
					recoveryOnce.Do(func() { close(recovery); <-resumeRecovery })
				case pauseCloseIndexBackfillsStopped:
					closeOnce.Do(func() { close(stopped); <-resumeClose })
				case pauseIndexBackfillStartCheck:
					startOnce.Do(func() { close(checked); <-resumeStart })
				}
			})
			defer restore()
			done := runBackfillOperation(func() error {
				if operation == "restore" {
					// Admission must fail before opening this file.
					return b.Restore("not-opened.backup")
				}
				_, _, err := b.DeleteByPrefix("test:")
				return err
			})
			awaitBackfillSignal(t, recovery)
			closed := runBackfillOperation(b.Close)
			awaitBackfillSignal(t, stopped)
			releaseRecovery.Do(func() { close(resumeRecovery) })

			var result backfillOperationResult
			select {
			case result = <-done:
				releaseClose.Do(func() { close(resumeClose) })
			case <-checked:
				// This is the old read-only check accepting closing. On DROP
				// there is no barrier: let Close nil the handle before use.
				t.Error("recovery admitted a backfill after Close stopped it")
				releaseClose.Do(func() { close(resumeClose) })
				if operation == "drop" {
					closeResult := awaitBackfillResult(t, closed)
					require.NoError(t, closeResult.err)
					require.Nil(t, closeResult.panicked, closeResult.stack)
					closed = nil
				}
				releaseStart.Do(func() { close(resumeStart) })
				result = awaitBackfillResult(t, done)
			case <-time.After(10 * time.Second):
				t.Fatal("recovery neither rejected shutdown nor reached its restart check")
			}
			require.Nil(t, result.panicked, "recovery panicked: %v\n%s", result.panicked, result.stack)
			require.ErrorIs(t, result.err, ErrStorageClosed)
			if closed != nil {
				closeResult := awaitBackfillResult(t, closed)
				require.NoError(t, closeResult.err)
				require.Nil(t, closeResult.panicked, closeResult.stack)
			}
		})
	}
}

// A start admitted before Close owns the handle until its probe finishes.
// Registration must still reject shutdown if Close stopped the tasks while
// that probe was paused. Both families and both start modes use this rule.
func TestIndexBackfillAdmissionSurvivesShutdown(t *testing.T) {
	for _, kind := range []startupIndexKind{startupLabelIndex, startupEdgeBetweenIndex} {
		for _, mode := range []indexBackfillStartMode{initializeIndexBackfill, forceIndexBackfill} {
			t.Run(fmt.Sprintf("kind-%d/mode-%d", kind, mode), func(t *testing.T) {
				b := createTestBadgerEngine(t)
				_, err := b.CreateNode(&Node{ID: "test:old", Labels: []string{"Doc"}})
				require.NoError(t, err)
				require.NoError(t, b.CreateEdge(&Edge{ID: "test:e", StartNode: "test:old", EndNode: "test:old", Type: "LINK"}))
				markMatrixIndexBuilding(t, b)
				if kind == startupEdgeBetweenIndex {
					require.NoError(t, b.withUpdate(func(txn *badger.Txn) error { return txn.Delete(edgeBetweenIndexReadyKey) }))
				}
				checked, resumeStart := make(chan struct{}), make(chan struct{})
				stopped, resumeClose := make(chan struct{}), make(chan struct{})
				var startOnce, closeOnce, releaseStart, releaseClose sync.Once
				defer releaseStart.Do(func() { close(resumeStart) })
				defer releaseClose.Do(func() { close(resumeClose) })
				restore := setMaintenancePauseHook(func(point string) {
					switch point {
					case pauseIndexBackfillStartCheck:
						startOnce.Do(func() { close(checked); <-resumeStart })
					case pauseCloseIndexBackfillsStopped:
						closeOnce.Do(func() { close(stopped); <-resumeClose })
					}
				})
				defer restore()
				done := runBackfillOperation(func() error { return b.startIndexBackfill(kind, mode) })
				awaitBackfillSignal(t, checked)
				closed := runBackfillOperation(b.Close)
				awaitBackfillSignal(t, stopped)
				// Close has passed its stop calls. Registering now would leave a
				// task outside that cancellation/join, even with a live handle.
				releaseStart.Do(func() { close(resumeStart) })
				result := awaitBackfillResult(t, done)
				require.Nil(t, result.panicked, result.stack)
				require.ErrorIs(t, result.err, ErrStorageClosed)
				releaseClose.Do(func() { close(resumeClose) })
				closeResult := awaitBackfillResult(t, closed)
				require.NoError(t, closeResult.err)
				require.Nil(t, closeResult.panicked, closeResult.stack)
			})
		}
	}
}

// Even a start not reached through DROP or Restore owns its probe until it
// returns. Close may mark the engine closed, but must keep its handle alive
// until the probe releases its helper lease.
func TestIndexBackfillProbePinsHandleAgainstClose(t *testing.T) {
	for _, path := range []struct {
		name    string
		kind    startupIndexKind
		rebuild bool
	}{
		{"kind-0", startupLabelIndex, false},
		{"kind-1", startupEdgeBetweenIndex, false},
		{"label-rebuild", startupLabelIndex, true},
	} {
		t.Run(path.name, func(t *testing.T) {
			b := createTestBadgerEngine(t)
			_, err := b.CreateNode(&Node{ID: "test:old", Labels: []string{"Doc"}})
			require.NoError(t, err)
			markMatrixIndexBuilding(t, b)
			if path.kind == startupEdgeBetweenIndex {
				require.NoError(t, b.withUpdate(func(txn *badger.Txn) error { return txn.Delete(edgeBetweenIndexReadyKey) }))
			}
			checked, resume := make(chan struct{}), make(chan struct{})
			var once, release sync.Once
			defer release.Do(func() { close(resume) })
			restore := setMaintenancePauseHook(func(point string) {
				if point == pauseIndexBackfillStartCheck {
					once.Do(func() { close(checked); <-resume })
				}
			})
			defer restore()
			done := runBackfillOperation(func() error {
				if path.rebuild {
					_, err := b.rebuildLabelIndex(nil)
					return err
				}
				return b.startIndexBackfill(path.kind, initializeIndexBackfill)
			})
			awaitBackfillSignal(t, checked)
			closed := runBackfillOperation(b.Close)
			require.Eventually(t, func() bool {
				return errors.Is(b.ensureOpen(false), ErrStorageClosed)
			}, 10*time.Second, time.Millisecond, "Close must reach its helper-transaction join")
			select {
			case result := <-closed:
				t.Error("Close returned while an admitted backfill probe was paused")
				require.NoError(t, result.err)
				require.Nil(t, result.panicked, result.stack)
				closed = nil
			case <-time.After(100 * time.Millisecond):
			}
			b.mu.RLock()
			hasHandle := b.db != nil
			b.mu.RUnlock()
			require.True(t, hasHandle, "the backfill admission must pin the handle")
			release.Do(func() { close(resume) })
			result := awaitBackfillResult(t, done)
			require.Nil(t, result.panicked, result.stack)
			require.ErrorIs(t, result.err, ErrStorageClosed)
			if closed != nil {
				result = awaitBackfillResult(t, closed)
				require.NoError(t, result.err)
				require.Nil(t, result.panicked, result.stack)
			}
		})
	}
}
