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
