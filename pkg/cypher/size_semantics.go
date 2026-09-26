package cypher

import (
	"errors"
	"fmt"
	"reflect"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// sizeArgumentTypes is size()'s accepted argument types as Neo4j names them.
const sizeArgumentTypes = "String or List<T>"

// typeMismatchError is Neo4j's error for a function argument whose type the
// function's signature excludes: Neo.ClientError.Statement.SyntaxError
// "Type mismatch: expected <expected> but was <type>". size() of a map, node,
// relationship, path, number or boolean fails with it on every route (#600).
func typeMismatchError(expected string, value interface{}) error {
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		"InvalidArgumentType",
		fmt.Sprintf("Type mismatch: expected %s but was %s", expected, cypherValueTypeName(value)),
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

// pathTypeMarker names a path value (paths are carried as maps holding a
// _pathResult) for cypherValueTypeName.
type pathTypeMarker struct{}

// cypherValueTypeName is the Cypher type name Neo4j uses in type errors.
func cypherValueTypeName(value interface{}) string {
	switch v := value.(type) {
	case pathTypeMarker, *PathResult, PathResult:
		return "Path"
	case *storage.Node:
		return "Node"
	case *storage.Edge:
		return "Relationship"
	case bool:
		return "Boolean"
	case string:
		return "String"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "Integer"
	case float32, float64:
		return "Float"
	case map[string]interface{}:
		if _, isPath := v["_pathResult"]; isPath {
			return "Path"
		}
		return "Map"
	}
	if value == nil {
		return "Null"
	}
	switch reflect.TypeOf(value).Kind() {
	case reflect.Map:
		return "Map"
	case reflect.Slice, reflect.Array:
		return "List"
	}
	return fmt.Sprintf("%T", value)
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
