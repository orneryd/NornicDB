package cypher

import "context"

func parameterCancellationForContext(ctx context.Context) *pipelineCancellation {
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

// The external parameter representation remains a native Go map. Its key
// hashing and allocation are runtime boundaries; every application-controlled
// scan borrows the statement probe. Generated alias keys have bounded length.
// Copy lazily so a statement without a bound quoted parameter keeps its map.
func bindQuotedQueryParametersWithCancellation(names *quotedVariableNames, params map[string]interface{}, probe *pipelineCancellation) map[string]interface{} {
	if probe == nil {
		return names.parameterValues(params)
	}
	var bound map[string]interface{}
	lookup := func(name string) (interface{}, bool) {
		for key, value := range params {
			if !probe.check() {
				return nil, false
			}
			if comparePipelineStrings(key, name, probe) == 0 {
				return value, probe.err == nil
			}
			if probe.err != nil {
				return nil, false
			}
		}
		return nil, false
	}
	bind := func(name string, value interface{}) bool {
		if bound == nil {
			bound = make(map[string]interface{}, len(params)+len(names.parameterBindings))
			for key, original := range params {
				if !probe.check() {
					return false
				}
				bound[key] = original
			}
		}
		if !probe.check() {
			return false
		}
		bound[name] = value
		return true
	}
	if !names.bindParameterValuesWithCancellation(lookup, bind, probe) {
		return nil
	}
	if bound != nil {
		return bound
	}
	return params
}
