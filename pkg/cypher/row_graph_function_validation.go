package cypher

import (
	"context"
	"errors"
	"fmt"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// invalidFunctionArgument is the TypeError for a function argument of the
// wrong type, shared by RETURN-row validation and the expression evaluator
// (functionEvaluationFailure).
func invalidFunctionArgument(function string, value interface{}) error {
	return newSemanticError(
		"Neo.ClientError.Statement.TypeError",
		"InvalidArgumentValue",
		fmt.Sprintf("%s() received an invalid %s argument", lowerASCII(function), cypherTypeName(value)),
	)
}

// functionEvaluationFailure records an error returned by a registry function
// as the expression failure of ctx, so it fails the statement instead of
// evaluating to null.
func functionEvaluationFailure(ctx context.Context, err error) {
	recordExpressionFailure(ctx, functionEvaluationError(err))
}

// functionEvaluationError is the statement error for an error a registry
// function returned, with Neo4j's status and message, for either evaluator.
func functionEvaluationError(err error) error {
	var selection *cypherfn.GraphSelectionContextError
	if errors.As(err, &selection) {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", localization.Message{Fallback: selection.Error()})
	}
	var argumentError *cypherfn.ArgumentTypeError
	if errors.As(err, &argumentError) {
		err = invalidFunctionArgument(argumentError.Function, argumentError.Value)
	}
	var unknown *cypherfn.UnknownFunctionError
	if errors.As(err, &unknown) {
		err = graphFunctionUnknownError(unknown.Function)
	}
	var notFound *cypherfn.GraphNotFoundError
	if errors.As(err, &notFound) {
		err = graphNotFoundError(notFound.Name)
	}
	var count *cypherfn.ParameterCountError
	if errors.As(err, &count) {
		err = functionParameterCountError(count.Function, count.TooMany)
	}
	return typeMismatchFromFunctionError(err)
}

// errRowArgumentUnresolved stops a registry function whose argument the row
// evaluator can't resolve; the call is then unresolved, not an error.
var errRowArgumentUnresolved = errors.New("row argument unresolved")

// rowFunctionArgumentFailure aborts only the registry call whose Eval callback
// could not produce a value. Some handlers discard callback errors; an abort
// keeps that failure from becoming a successful result without a captured,
// heap-allocated status cell on every successful function evaluation.
type rowFunctionArgumentFailure struct{ err error }

// evaluateRowGraphFunction evaluates graph.names() / graph.propertiesByName()
// and GDS's functions (isGDSFunctionName) in the row evaluator with their one
// implementation, the function registry (pkg/cypher/fn), and its errors as
// the other evaluator reports them. The flags are (handled, resolved): a
// registry miss never evaluates arguments; an unresolved registered call must
// never be retried as another function implementation.
func (e *StorageExecutor) evaluateRowGraphFunction(function string, argument braceView, values map[string]interface{}) (value interface{}, handled, resolved bool, err error) {
	defer func() {
		if aborted := recover(); aborted != nil {
			failure, ok := aborted.(rowFunctionArgumentFailure)
			if !ok {
				panic(aborted)
			}
			value, handled, resolved, err = nil, true, false, nil
			if !errors.Is(failure.err, errRowArgumentUnresolved) {
				err = functionEvaluationError(failure.err)
			}
		}
	}()
	args := argument.commaViews()
	fnCtx := e.registryFunctionContext(rowStatementContextOrBackground(values), args, func(expression braceView) (interface{}, error) {
		value, resolved, err := e.evaluateRowValue(expression, values)
		if err != nil {
			panic(rowFunctionArgumentFailure{err})
		}
		if !resolved {
			panic(rowFunctionArgumentFailure{errRowArgumentUnresolved})
		}
		return value, nil
	})
	// Some internal row callers carry the language version as a binding rather
	// than a statement context. Keep that binding authoritative when present.
	if version, bound := values[cypher25RowKey].(bool); bound {
		fnCtx.Cypher25 = version
	}
	value, found, err := cypherfn.EvaluateInvocation(function, functionArgumentTexts(args), fnCtx)
	if !found {
		return nil, false, false, nil
	}
	if errors.Is(err, errRowArgumentUnresolved) {
		return nil, true, false, nil
	}
	if err != nil {
		return nil, true, false, functionEvaluationError(err)
	}
	return value, true, true, nil
}

// graphFunctionUnknownError is Neo4j's SyntaxError for a composite graph
// function (graph.names, graph.propertiesByName) outside a composite
// database: "Unknown function '<name>'".
func graphFunctionUnknownError(function string) error {
	return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnknownFunction",
		localization.CypherCommandRoutingGraphFunctionUnknown(function))
}

// functionParameterCountError is Neo4j's SyntaxError for a call with too
// many or too few arguments: "Too many parameters for function '<name>'",
// "Insufficient parameters for function '<name>'".
func functionParameterCountError(function string, tooMany bool) error {
	message := localization.CypherCommandRoutingFunctionInsufficientParameters(function)
	if tooMany {
		message = localization.CypherCommandRoutingFunctionTooManyParameters(function)
	}
	return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "FunctionParameterCount", message)
}

// graphNotFoundError is Neo4j's DatabaseNotFound "Graph not found: <name>"
// for a graph name that isn't one of the composite database's graphs.
func graphNotFoundError(name string) error {
	return localizedStatusError("Neo.ClientError.Database.DatabaseNotFound", "DatabaseNotFound",
		localization.CypherCommandRoutingGraphNotFound(name))
}
