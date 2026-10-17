package cypher

import "context"

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

// recoverCancelledWorker is deferred by a goroutine running part of a
// statement under its forked probe (pipelineWorkerContext): a panic that
// follows the statement's cancellation ends the worker and is reported to the
// statement's owner, which returns the cancellation. Any other panic is
// re-raised.
func recoverCancelledWorker(ctx context.Context) {
	if recovered := recover(); recovered != nil {
		if cancelledPanic(ctx) != nil {
			if probe := contextPipelineProbe(ctx); probe != nil {
				probe.observed.Store(true)
				if probe.parent != nil {
					probe.parent.observed.Store(true)
				}
			}
			return
		}
		panic(recovered)
	}
}
