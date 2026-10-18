package cypher

import "context"

func parameterCancellationForContext(ctx context.Context) *pipelineCancellation {
	if probe := contextPipelineProbe(ctx); probe != nil {
		return probe
	}
	if ctx.Done() == nil {
		return nil
	}
	return pipelineCancellationForContext(ctx)
}

func copyParameterListWithCancellation(values []interface{}, probe *pipelineCancellation) []interface{} {
	if probe != nil && !probe.check() {
		return nil
	}
	if len(values) > pipelineCancellationInterval && probe != nil && !probe.poll() {
		return nil
	}
	return appendPipelineItems(make([]interface{}, 0), values, probe)
}

