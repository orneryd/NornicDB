package cypher

import (
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"reflect"

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

// sizeArgumentError is the error for size(value), or nil when size() accepts
// the value (null, a string, a list).
func sizeArgumentError(value interface{}) error {
	if value == nil {
		return nil
	}
	if _, isString := value.(string); isString {
		return nil
	}
	_, remotePath := value.(storage.RemotePathValue)
	if kind := reflect.TypeOf(value).Kind(); !remotePath && (kind == reflect.Slice || kind == reflect.Array) {
		return nil
	}
	// Static validation rejects known invalid argument types before execution.
	// A CASE or other nullable expression may reveal a map/entity only now;
	// every invalid value reaching this runtime owner is a TypeError.
	return &classifiedCypherError{
		cause:  localizedError(localization.CypherCoreFunctionArgumentInvalid("size", "a String or List", neo4jValueRepr(value)), nil),
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
func typeMismatchFromFunctionError(err error) error {
	var mismatch *cypherfn.TypeMismatchError
	if errors.As(err, &mismatch) {
		return localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
			localization.CypherCoreFunctionArgumentInvalid(mismatch.Function, "a "+mismatch.Expected, neo4jValueRepr(mismatch.Value)))
	}
	return err
}
