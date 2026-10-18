package cypher

import (
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/localization"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

// sizeArgumentTypes is size()'s accepted argument types as Neo4j names them.
const sizeArgumentTypes = "String or List<T>"

// typeMismatchError is Neo4j's error for a function argument whose type the
// function's signature excludes: Neo.ClientError.Statement.SyntaxError
// "Type mismatch: expected <expected> but was <type>". size() of a map, node,
// relationship, path, number or boolean fails with it when statically known (#600).
func typeMismatchError(expected string, value interface{}) error {
	return typeNameMismatchError(expected, cypherTypeName(value))
}

// typeNameMismatchError is typeMismatchError for an argument whose type is
// known by name. Static checks report the argument signature before execution.
func typeNameMismatchError(expected, typeName string) error {
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"InvalidArgumentType",
		fmt.Sprintf("Type mismatch: expected %s but was %s", expected, typeName),
	)
}

// sizeArgumentError classifies an argument rejected by the shared size owner.
func sizeArgumentError(value interface{}, probe *pipelineCancellation) error {
	// Static validation owns known type mismatches; rejected runtime values
	// retain release's TypeError classification.
	return &classifiedCypherError{
		cause:  localizedError(localization.CypherCoreFunctionArgumentInvalid("size", "a String or List", neo4jValueRepr(value, probe)), nil),
		code:   "Neo.ClientError.Statement.TypeError",
		detail: "InvalidArgumentType",
	}
}

// typeMismatchFromFunctionError converts a registry TypeMismatchError, a
// function argument of the wrong type found while the statement runs (a
// property value: toUpper(n.age)), into Neo4j's run-time TypeError ("Invalid
// input for function 'toUpper()': Expected a String, got: Long(5)"). An
// argument whose type is known when the statement compiles (a literal, a
// variable bound to one) is the static check's "Type mismatch" SyntaxError
// first (staticFunctionArguments). Other errors are returned unchanged.
func typeMismatchFromFunctionError(err error, probe *pipelineCancellation) error {
	var mismatch *cypherfn.TypeMismatchError
	if errors.As(err, &mismatch) {
		if mismatch.Function == "size" {
			return sizeArgumentError(mismatch.Value, probe)
		}
		return localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
			localization.CypherCoreFunctionArgumentInvalid(mismatch.Function, "a "+mismatch.Expected, neo4jValueRepr(mismatch.Value, probe)))
	}
	return err
}
