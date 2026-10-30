package cypher

import nornicerrors "github.com/orneryd/nornicdb/pkg/errors"

type pipelineDispatchState uint8

const (
	pipelineDispatchNotApplicable pipelineDispatchState = iota
	pipelineDispatchHandled
	pipelineDispatchParseRejected
	pipelineDispatchFailed
)

type pipelineDispatchOutcome struct {
	state  pipelineDispatchState
	result *ExecuteResult
	err    error
}

func newPipelineDispatchOutcome(result *ExecuteResult, handled bool, err error) pipelineDispatchOutcome {
	if err != nil {
		state := pipelineDispatchFailed
		if nornicerrors.HasNeo4jStatus(err) {
			if code, _ := nornicerrors.Neo4jStatus(err); code == "Neo.ClientError.Statement.SyntaxError" {
				state = pipelineDispatchParseRejected
			}
		}
		return pipelineDispatchOutcome{state: state, result: result, err: err}
	}
	if handled {
		return pipelineDispatchOutcome{state: pipelineDispatchHandled, result: result}
	}
	return pipelineDispatchOutcome{state: pipelineDispatchNotApplicable}
}

func (outcome pipelineDispatchOutcome) terminal() bool {
	return outcome.state != pipelineDispatchNotApplicable
}

func (outcome pipelineDispatchOutcome) handled() bool {
	return outcome.state == pipelineDispatchHandled
}
