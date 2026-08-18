package bolt

import (
	"context"
	"fmt"
	"time"
)

// rollbackWithCleanupDeadline gives cooperative storage work a deadline without
// inheriting connection cancellation. It invokes rollback synchronously: Go
// cannot preempt a storage implementation that ignores context, and abandoning
// a goroutine would leak transaction ownership.
func rollbackWithCleanupDeadline(base context.Context, executor TransactionalExecutor) error {
	return rollbackWithCleanupTimeout(base, executor, transactionCleanupRequestTimeout)
}

// rollbackWithCleanupTimeout constructs an uncancelled, bounded request while
// keeping the rollback call synchronous so ownership is never abandoned.
func rollbackWithCleanupTimeout(
	base context.Context,
	executor TransactionalExecutor,
	timeout time.Duration,
) error {
	if executor == nil {
		return nil
	}
	if base == nil {
		base = context.Background()
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(base), timeout)
	defer cancel()
	return invokeTransactionRollback(ctx, executor)
}

// invokeTransactionRollback converts adapter panics into a cleanup result that
// lifecycle waiters and operator telemetry can observe safely.
func invokeTransactionRollback(ctx context.Context, executor TransactionalExecutor) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("transaction rollback panicked: %v", recovered)
		}
	}()
	return executor.RollbackTransaction(ctx)
}

// invokeTransactionBegin preserves a backend panic separately from returned
// errors so compensation runs before the original panic is rethrown.
func invokeTransactionBegin(
	ctx context.Context,
	executor TransactionalExecutor,
	metadata map[string]any,
) (recovered any, err error) {
	defer func() {
		recovered = recover()
	}()
	err = executor.BeginTransaction(ctx, metadata)
	return nil, err
}

// observeTransactionLifecycle keeps diagnostics outside transaction ownership.
// A custom slog handler may panic; observability must not prevent durable
// cleanup, strand terminal waiters, or replace an executor panic.
func observeTransactionLifecycle(
	observe func(transactionTerminalReason, string, time.Duration, error),
	reason transactionTerminalReason,
	database string,
	duration time.Duration,
	err error,
) {
	if observe == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	observe(reason, database, duration, err)
}

func notifyTransactionCleanupFailure(notify func(error), err error) {
	if notify == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	notify(err)
}
