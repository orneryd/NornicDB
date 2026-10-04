package cypher

import (
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/localization"
	"reflect"
	"time"

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
	// A map, node, relationship or path always has a type Neo4j knows when
	// it compiles the statement: its SyntaxError. A scalar of the wrong type
	// that only shows up while the statement runs (a property, an item of a
	// mixed list) is Neo4j's TypeError, naming the value (#893).
	if !isRuntimeNumber(value) && !isStorableScalar(value) {
		return typeMismatchError(sizeArgumentTypes, value)
	}
	return &classifiedCypherError{
		cause:  localizedError(localization.CypherCoreFunctionArgumentInvalid("size", "a String or List", neo4jValueRepr(value)), nil),
		code:   "Neo.ClientError.Statement.TypeError",
		detail: "InvalidArgumentType",
	}
}

// isStorableScalar reports whether value is a boolean, temporal value or
// point: a non-number property value.
func isStorableScalar(value interface{}) bool {
	switch value.(type) {
	case bool, CypherDate, *CypherDate, CypherLocalTime, *CypherLocalTime, CypherTime, *CypherTime,
		CypherLocalDateTime, *CypherLocalDateTime, CypherDateTime, *CypherDateTime,
		CypherDuration, *CypherDuration, CypherPoint, *CypherPoint, time.Time, *time.Time:
		return true
	}
	return false
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
