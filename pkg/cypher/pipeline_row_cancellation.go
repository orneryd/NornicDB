package cypher

import "context"

// A worker borrows the same query context and cancellation owner, with its own
// counter. Recursive operations inside that worker share this one probe.
func pipelineWorkerContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, pipelineCancellationKey{}, &pipelineCancellation{ctx: ctx})
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
