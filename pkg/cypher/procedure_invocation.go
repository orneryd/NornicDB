package cypher

import (
	"context"
	"fmt"
)

// admittedProcedureCall is one procedure CALL as the statement compiler
// admitted it: the resolved procedure, its argument expressions and its YIELD.
// Execution reads these facts; it never resolves a procedure, counts
// arguments or parses YIELD again.
type admittedProcedureCall struct {
	procedure registeredProcedure
	arguments []string // nil: implicit arguments, read from parameters
	yield     *yieldClause
	text      string
}

// executionProcedureCall reads the admitted declaration of a CALL clause from
// the statement's bound program. A clause the program did not admit is
// unresolved; there is no text fallback.
func executionProcedureCall(ctx context.Context, clause pipelineClause) (admittedProcedureCall, error) {
	program, _ := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram)
	if program == nil || !clause.syntax.Valid() || program.syntax.Source() != clause.syntax.Source() {
		return admittedProcedureCall{}, unresolvedStatementError()
	}
	bound := program.clauses[clause.syntax.ID()]
	if bound == nil || bound.procedure == nil || bound.procedure.Handler == nil {
		return admittedProcedureCall{}, unresolvedStatementError()
	}
	return admittedProcedureCall{
		procedure: *bound.procedure,
		arguments: procedureArgumentTextsFromSyntax(clause.syntax),
		yield:     yieldFromSyntax(clause.syntax),
		text:      clause.text,
	}, nil
}

// admittedProcedureWrites reports whether an admitted CALL clause may write.
// A clause without an admitted local procedure is treated as writing.
func admittedProcedureWrites(ctx context.Context, clause pipelineClause) bool {
	call, err := executionProcedureCall(ctx, clause)
	return err != nil || call.procedure.Spec.writes()
}

// bindProcedureCall returns the admitted CALL that starts statement. When the
// statement's bound program is in ctx, its first clause is read directly.
// Otherwise (a CALL inside an executed tail) callOnly, the call with its
// YIELD, is admitted through the same compiler with the caller's incoming
// names.
func (e *StorageExecutor) bindProcedureCall(ctx context.Context, statement, callOnly string, incoming map[string]struct{}) (context.Context, admittedProcedureCall, error) {
	if program, _ := ctx.Value(boundStatementProgramKey{}).(*boundStatementProgram); program != nil && program.syntax.Source() == statementAdmissionSource(ctx, statement) &&
		len(program.root.branches) > 0 && len(program.root.branches[0].shape.clauses) > 0 {
		if first := program.root.branches[0].shape.clauses[0]; first.kind == pipelineClauseCall {
			call, err := executionProcedureCall(ctx, first.pipelineClause)
			return ctx, call, err
		}
	}
	clauses, ok := splitPipelineClausesAllowingProcedureCalls(ctx, callOnly)
	if !ok || len(clauses) == 0 || clauses[0].kind != pipelineClauseCall {
		if err := ctx.Err(); err != nil {
			return ctx, admittedProcedureCall{}, err
		}
		return ctx, admittedProcedureCall{}, unresolvedStatementError()
	}
	ctx, clauses, err := e.bindPipelineProgram(ctx, clauses, incoming)
	if err != nil {
		return ctx, admittedProcedureCall{}, err
	}
	call, err := executionProcedureCall(ctx, clauses[0])
	return ctx, call, err
}

// procedureArguments evaluates the admitted argument expressions in the
// current row (or reads implicit arguments from the parameters) and coerces
// them to the declared parameter types. Arity was decided at admission.
func (e *StorageExecutor) procedureArguments(ctx context.Context, call admittedProcedureCall) ([]interface{}, error) {
	spec := call.procedure.Spec
	if call.arguments == nil {
		params := getParamsFromContext(ctx)
		args := make([]interface{}, 0, len(spec.Params))
		for index, parameter := range spec.Params {
			value, exists := params[parameter.Name]
			if !exists {
				if index >= spec.MinArgs && parameter.Optional {
					continue
				}
				return nil, newSemanticError(
					"Neo.ClientError.Statement.ParameterMissing",
					"MissingParameter",
					fmt.Sprintf("missing implicit procedure parameter %s", parameter.Name),
				)
			}
			args = append(args, value)
		}
		return validateAndCoerceProcedureArguments(spec, args, nil)
	}
	ctx = withExpressionFailureSlot(ctx)
	bindings := valueBindingsFromContext(ctx)
	if bindings == nil {
		bindings = make(map[string]interface{})
		bindParameterRow(ctx, pipelineRow(bindings))
	}
	args := make([]interface{}, len(call.arguments))
	for index, text := range call.arguments {
		value, resolved := e.evaluateRowExpressionWithContext(ctx, text, pipelineRow(bindings))
		if !resolved {
			pipelineItemUnevaluable(ctx, text)
			return nil, getExpressionFailure(ctx)
		}
		if err := getExpressionFailure(ctx); err != nil {
			return nil, err
		}
		args[index] = value
	}
	return validateAndCoerceProcedureArguments(spec, args, call.arguments)
}

// invokeProcedure runs an admitted procedure once and applies its YIELD.
func (e *StorageExecutor) invokeProcedure(ctx context.Context, call admittedProcedureCall) (*ExecuteResult, error) {
	proc := call.procedure
	args, err := e.procedureArguments(ctx, call)
	if err != nil {
		return nil, err
	}
	procedureCtx, procedureExec := e.systemProcedureScope(ctx)
	if proc.Spec.OwnsTransactions {
		if err := procedureExec.checkPeriodicTransactionScope(procedureCtx, proc.Spec.Name); err != nil {
			return nil, err
		}
	}
	result, err := procedureExec.executeProcedureHandler(procedureCtx, proc, call.text, args)
	if err != nil {
		return nil, procedureRuntimeError(proc.Spec.Name, err)
	}
	if proc.User {
		result = runtimeProcedureResult(result)
	}
	if call.yield == nil {
		return result, nil
	}
	// Closed procedure schemas were admitted before publication. An opaque
	// user handler declares no schema: its documented extension checks the
	// admitted YIELD items against the fields it actually materialized.
	if proc.User && len(procedureDeclaredReturns(proc.Spec)) == 0 {
		if err := validateYieldColumnsExist(result.Columns, call.yield); err != nil {
			return nil, err
		}
	}
	return e.applyYieldFilter(ctx, result, call.yield)
}
