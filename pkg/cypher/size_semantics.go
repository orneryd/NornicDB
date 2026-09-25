package cypher

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

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
	return typeNameMismatchError(expected, cypherValueTypeName(value))
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

// recordRowSizeArgumentFailure records the size() type error of an
// expression the row evaluator could not resolve: the first size(...) call,
// at any depth, whose argument evaluates to a value size() rejects. The row
// evaluator has no context to record failures itself, so the context-aware
// entry point (evaluateRowExpressionWithContext) calls this when it gets an
// unresolved result, turning "could not parse" into the type error.
func (e *StorageExecutor) recordRowSizeArgumentFailure(ctx context.Context, expression string, values pipelineRow) bool {
	if !containsFold(expression, "size") {
		return false
	}
	for offset := 0; offset < len(expression); {
		index := findKeywordIndexInContext(expression[offset:], "size")
		if index < 0 {
			return false
		}
		index += offset
		open := skipSpaces(expression, index+len("size"))
		if open < len(expression) && expression[open] == '(' {
			if close := findMatchingDelimiter(expression, open, '(', ')'); close > open {
				argument := strings.TrimSpace(expression[open+1 : close])
				if value, resolved := e.evaluateRowExpression(argument, values); resolved {
					if err := sizeArgumentError(value); err != nil {
						recordExpressionFailure(ctx, err)
						return true
					}
				}
			}
		}
		offset = index + len("size")
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
