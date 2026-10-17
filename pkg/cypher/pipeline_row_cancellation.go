package cypher

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
)

// pipelineWorkerContext gives a goroutine that runs part of the statement its
// own probe (a probe's counter is not shared across goroutines). The fork
// reads the same context and reports a cancellation it sees to the statement
// probe (statementCancellationErr), so the statement's owner learns of it.
func pipelineWorkerContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, pipelineCancellationKey{}, &pipelineCancellation{ctx: ctx, parent: contextPipelineProbe(ctx)})
}

func clearPipelineRow(row pipelineRow, probe *pipelineCancellation) bool {
	if probe == nil {
		clear(row)
		return true
	}
	for name := range row {
		if !probe.check() {
			return false
		}
		delete(row, name)
	}
	return true
}

// recoverWorkerPanic is the last line of defence of a goroutine that runs
// part of a statement under its forked probe (pipelineWorkerContext): a panic
// there would end the process. It is not how a statement stops at its
// deadline (readers return their cancellation). It logs the panic with its
// stack and fails the statement through its owner: with the cancellation when
// the statement was cancelled, otherwise with an internal error naming the
// panic. Without a statement probe to report to, it re-raises the panic.
func recoverWorkerPanic(ctx context.Context) {
	recovered := recover()
	if recovered == nil {
		return
	}
	stack := debug.Stack()
	log.Printf("cypher: recovered panic in a statement worker: %v\n%s", recovered, stack)
	probe := contextPipelineProbe(ctx)
	if probe == nil || probe.parent == nil {
		// No statement owner to fail: the panic stays a panic.
		panic(recovered)
	}
	var err error
	if ctxErr := ctx.Err(); ctxErr != nil {
		err = ctxErr
	} else {
		err = newSemanticError("Neo.DatabaseError.General.UnknownError", "UnknownError", fmt.Sprintf("internal error in a statement worker: %v", recovered))
	}
	// Every probe above the worker learns of it: the statement reads the
	// probe it was admitted with, which need not be the root (a preparation
	// probe sits above the admitted one).
	for owner := probe.parent; owner != nil; owner = owner.parent {
		owner.failure.CompareAndSwap(nil, &err)
		owner.observed.Store(true)
	}
}
