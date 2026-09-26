package cypher

import (
	"errors"
	"fmt"
	"reflect"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

// sizeArgumentTypes is size()'s accepted argument types as Neo4j names them.
const sizeArgumentTypes = "String or List<T>"

// typeMismatchError is Neo4j's error for a function argument whose type the
// function's signature excludes: Neo.ClientError.Statement.SyntaxError
// "Type mismatch: expected <expected> but was <type>". size() of a map, node,
// relationship, path, number or boolean fails with it on every route (#600).
func typeMismatchError(expected string, value interface{}) error {
	return typeNameMismatchError(expected, cypherTypeName(value))
}

// typeNameMismatchError is typeMismatchError for an argument whose type is
// known by name: the static checks (staticFunctionArguments) and the runtime
// ones report the same error.
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
	if path, isMap := toStringAnyMap(value); isMap {
		if _, isPath := path["_pathResult"]; isPath {
			return typeMismatchError(sizeArgumentTypes, pathTypeMarker{})
		}
	}
	if kind := reflect.TypeOf(value).Kind(); kind == reflect.Slice || kind == reflect.Array {
		return nil
	}
	return typeMismatchError(sizeArgumentTypes, value)
}

// typeMismatchFromFunctionError converts a registry TypeMismatchError into
// the statement error; other errors are returned unchanged.
func typeMismatchFromFunctionError(err error) error {
	var mismatch *cypherfn.TypeMismatchError
	if errors.As(err, &mismatch) {
		return typeMismatchError(mismatch.Expected, mismatch.Value)
	}
	return err
}
