package cypher

import (
	"context"
	"fmt"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/txphase"
)

// executeNativeCoordinatedStatement gives dynamic native autocommit a single
// existing statement coordinator before an opaque callback can touch storage.
// Only the native root issued here may detach independently owned batches.
func (e *StorageExecutor) executeNativeCoordinatedStatement(ctx context.Context, query, upper string) (result *ExecuteResult, resultErr error) {
	defer func() {
		completeStatementTerminalOutcome(ctx, true, &result, &resultErr, nil)
		// Staged callback rows are not a result when execution or commit fails,
		// even if no WAL failure created a terminal journal entry.
		if resultErr != nil {
			result = nil
		}
	}()
	tx := fabric.NewFabricTransaction(fmt.Sprintf("native-%d", time.Now().UnixNano()))
	dispatch := &cypherFabricExecutor{base: e, authToken: GetAuthTokenFromContext(ctx), autoCommit: true,
		transaction: tx, nativeOrigin: ctx, wholeStatementMutable: true}
	defer func() {
		if dispatch.remote != nil {
			resultErr = nornicerrors.CombineTerminalOutcomes(resultErr, dispatch.remote.Close())
		}
	}()
	statement := dispatch.statementFor(ctx, tx)
	ctx, stats := beginProcedureStatementStatsScope(ctx)
	defer func() {
		if resultErr != nil {
			stats.discard()
		} else {
			var value *QueryStats
			if result != nil {
				value = result.Stats
			}
			stats.commit(value)
		}
	}()
	completion, resultErr := dispatch.runStatementOwner(ctx, true, func(runCtx context.Context) error {
		root, err := statement.participant(e.databaseName(), e.storage)
		if err != nil {
			return err
		}
		statement.nativeRoot = root
		executor, ownedCtx := root.run(runCtx, statement)
		if state := e.resolvedStatementModeOf(runCtx); state != nil {
			var release func()
			executor, release = executor.bindResolvedStatementMode(state)
			defer release()
		}
		result, err = executor.executeWithoutTransaction(ownedCtx, query, upper)
		if err == nil {
			err = getExpressionFailure(ownedCtx)
		}
		if err == nil {
			err = memoryFailureFromContext(ownedCtx)
		}
		return err
	})
	if completion != nil {
		ctx = completion
	}
	if resultErr == nil && result != nil && dispatch.nativeCompletion != nil && tx.WriteShard() != "" {
		// Completion metadata belongs to the actual native publication owner;
		// transfer its receipt and consistency fact without reclassifying ACKs.
		for key, value := range dispatch.nativeCompletion.Metadata {
			if result.Metadata == nil {
				result.Metadata = make(map[string]interface{})
			}
			result.Metadata[key] = value
		}
	}
	return result, resultErr
}

// admitNativeWrite is shared by enlisted native participants and authorized
// independent batches. A write claims the sole database before any mutation.
func (c *cypherFabricExecutor) admitNativeWrite(ctx context.Context, name, database string) error {
	if err := authorizeDatabase(ctx, database, "write"); err != nil {
		return err
	}
	_, err := c.fabricTransaction().GetOrOpen(name, true)
	return err
}

// runStatementOwner is the terminal kernel for implicit coordinated execution.
// A participant is never abandoned on an error or panic; commit admission and
// cleanup retain their actual joined outcome rather than retrying execution.
func (c *cypherFabricExecutor) runStatementOwner(ctx context.Context, auto bool, run func(context.Context) error) (completion context.Context, resultErr error) {
	completion = ctx
	// Native statement construction already owns this detached cleanup scope.
	// Other coordinator callbacks retain their own bound cleanup lifetimes;
	// their dispatch input is the original caller, never a failed admission result.
	cleanupCtx := ctx
	if c.nativeOrigin != nil {
		cleanupCtx = c.statement.begin
	}
	tx := c.fabricTransaction()
	if auto {
		defer func() {
			cause := recover()
			if tx.State() == "open" {
				func() {
					defer func() {
						if cleanup := recover(); cleanup != nil && cause == nil {
							cause = cleanup
						}
					}()
					resultErr = nornicerrors.CombineTerminalOutcomes(resultErr, tx.Rollback(cleanupCtx, nil))
				}()
			}
			if cause != nil {
				panic(cause)
			}
		}()
	}
	if resultErr = run(ctx); resultErr != nil {
		return completion, resultErr
	}
	if !auto {
		return completion, nil
	}
	if c.nativeOrigin != nil {
		if resultErr = ctx.Err(); resultErr != nil {
			return completion, resultErr
		}
		if resultErr = memoryFailureFromContext(ctx); resultErr != nil {
			return completion, resultErr
		}
	}
	completion, resultErr = txphase.CommitContext(ctx)
	if resultErr != nil {
		return completion, resultErr
	}
	if resultErr = tx.Commit(fabric.TerminalContexts{Commit: completion, Cleanup: cleanupCtx}, nil, nil); resultErr != nil {
		if commitFailureWroteNothing(false, resultErr) {
			resultErr = nornicerrors.MarkCommitRolledBack(resultErr)
		}
		return completion, resultErr
	}
	if tx.WriteShard() != "" {
		publishStatementCommit(ctx)
	}
	return completion, nil
}

// bindNativeStatementLifetime retains the actual request cancellation when an
// issued callback supplies an unrelated context. Its own cancellation is also
// honored; forwarding the owner's context does not allocate another lifetime.
func bindNativeStatementLifetime(ctx, origin context.Context) (context.Context, func()) {
	if origin.Done() == nil || ctx.Done() == origin.Done() {
		return ctx, func() {}
	}
	var deadlineCancel context.CancelFunc
	if deadline, ok := origin.Deadline(); ok {
		if child, present := ctx.Deadline(); !present || deadline.Before(child) {
			ctx, deadlineCancel = context.WithDeadline(ctx, deadline)
		}
	}
	bound, cancel := context.WithCancelCause(ctx)
	stop := lifecycle.AfterFuncJoined(origin, func() { cancel(context.Cause(origin)) })
	if origin.Err() != nil {
		cancel(context.Cause(origin))
	}
	return bound, func() {
		stop()
		cancel(nil)
		if deadlineCancel != nil {
			deadlineCancel()
		}
	}
}
