package bolt

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// claimTransactionOperation joins timeout cleanup when expiry wins admission,
// so no operation response can outrun storage ownership release.
func (s *Session) claimTransactionOperation() (context.Context, error) {
	ctx, err := s.txLifecycle.claimOperation()
	if errors.Is(err, errTransactionTimedOut) {
		if cleanupErr := s.joinTransactionTimeout(); cleanupErr != nil {
			return nil, cleanupErr
		}
	}
	return ctx, err
}

// joinTransactionTimeout propagates cleanup uncertainty as a fatal session
// error and quarantines a raw executor rather than exposing it for reuse.
func (s *Session) joinTransactionTimeout() error {
	if err := s.txLifecycle.rollback(transactionTerminalTimeout); err != nil {
		s.markTransactionCleanupFailed()
		return fmt.Errorf("transaction timeout cleanup failed: %w", err)
	}
	return nil
}

// sendTransactionTimeoutAfterJoin emits the client timeout status only after
// cleanup is proven successful; cleanup failure closes the connection instead.
func (s *Session) sendTransactionTimeoutAfterJoin() error {
	if err := s.joinTransactionTimeout(); err != nil {
		return err
	}
	return s.sendTransactionTimeoutFailure()
}

// flushPendingExecutorWrites performs the legacy deferred Flush under explicit
// transaction lifecycle ownership. Autocommit Flush behavior is unchanged.
func (s *Session) flushPendingExecutorWrites() error {
	if !s.pendingFlush {
		return nil
	}
	// The pending work is consumed by this attempt. In particular, a timeout
	// that wins admission must never leave teardown or RESET able to flush it.
	s.pendingFlush = false

	claimed := false
	if s.inTransaction {
		if _, err := s.claimTransactionOperation(); err != nil {
			return err
		}
		claimed = true
		defer func() { _ = s.txLifecycle.finishOperation() }()
	}

	var flushErr error
	if flushable, ok := s.executor.(FlushableExecutor); ok {
		flushErr = flushable.Flush()
	}
	if claimed {
		// Atomically determine whether the operation or timeout won. If the
		// timeout won, finishOperation joins cleanup before returning.
		if err := s.txLifecycle.finishOperation(); err != nil {
			if cleanupErr := s.joinTransactionTimeout(); cleanupErr != nil {
				return cleanupErr
			}
			return errTransactionTimedOut
		}
		if flushErr != nil {
			return flushErr
		}
	}
	// Preserve the legacy best-effort Flush error contract for autocommit only.
	return nil
}

// sendFlushLifecycleFailure distinguishes fatal cleanup uncertainty from a
// recoverable explicit Flush error that requires RESET.
func (s *Session) sendFlushLifecycleFailure(err error) error {
	if s.transactionCleanupFailed {
		return err
	}
	if errors.Is(err, errTransactionTimedOut) {
		return s.sendTransactionTimeoutFailure()
	}
	return s.sendTransactionControlFailure(
		"Neo.DatabaseError.General.UnknownError",
		fmt.Sprintf("Deferred transaction flush failed: %v", err),
	)
}

// rollbackExplicitTransaction clears protocol state only after lifecycle
// cleanup succeeds; failure keeps the executor installed and fail-closed.
func (s *Session) rollbackExplicitTransaction(reason transactionTerminalReason) error {
	err := s.txLifecycle.rollback(reason)
	if err == nil {
		s.clearExplicitTransactionState()
	} else {
		s.markTransactionCleanupFailed()
	}
	return err
}

// markTransactionCleanupFailed suppresses teardown Flush and quarantines a raw
// single-connection executor across subsequent sessions.
func (s *Session) markTransactionCleanupFailed() {
	s.transactionCleanupFailed = true
	if s.rawTransactionExecutor && s.server != nil {
		s.server.rawTransactionExecutorPoisoned.Store(true)
	}
}

// failClosedTransactionCleanup is safe to call from the timeout goroutine. It
// avoids session protocol fields, quarantines shared storage ownership, and
// interrupts the connection so teardown can join cleanup on the session loop.
func (s *Session) failClosedTransactionCleanup(_ error) {
	if s.rawTransactionExecutor && s.server != nil {
		s.server.rawTransactionExecutorPoisoned.Store(true)
	}
	if s.connCancel != nil {
		s.connCancel()
	}
	if s.conn != nil {
		_ = s.conn.Close()
	}
}

// clearExplicitTransactionState discards pending results and deferred Flush so
// a completed terminal path cannot persist stale transaction work later.
func (s *Session) clearExplicitTransactionState() {
	s.inTransaction = false
	s.txMetadata = nil
	s.txDatabase = ""
	s.txHasMerge = false
	s.txHasNonMergeWrite = false
	s.pendingFlush = false
	s.lastResult = nil
	s.resultIndex = 0
	if s.baseExec != nil {
		s.executor = s.baseExec
	}
}

// observeTransactionTerminal emits phase-accurate structured diagnostics while
// lifecycle code contains logger panics outside transaction ownership.
func (s *Session) observeTransactionTerminal(
	reason transactionTerminalReason,
	database string,
	duration time.Duration,
	cleanupErr error,
) {
	if s.server == nil {
		return
	}
	logger := s.server.logger()
	level := slog.LevelDebug
	event := localization.BoltTransactionTerminatedEvent(string(reason), database, duration)
	if reason == transactionTerminalTimeoutCleanupRequested {
		level = slog.LevelWarn
		event = localization.BoltTransactionTimeoutCleanupRequestedEvent(string(reason), database, duration)
	} else if reason == transactionTerminalCommit && cleanupErr != nil {
		level = slog.LevelError
		event = localization.BoltTransactionCommitFailedEvent(string(reason), database, duration, cleanupErr)
	} else if cleanupErr != nil {
		level = slog.LevelError
		event = localization.BoltTransactionCleanupFailedEvent(string(reason), database, duration, cleanupErr)
	} else if reason == transactionTerminalTimeout {
		level = slog.LevelWarn
		event = localization.BoltTransactionTimeoutCleanupCompletedEvent(string(reason), database, duration)
	}
	if !logger.Enabled(context.Background(), level) {
		return
	}
	s.server.logEvent(context.Background(), level, event)
}
