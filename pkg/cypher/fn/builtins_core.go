package fn

import (
	"reflect"
	"sort"
	"strings"

	cyphertext "github.com/orneryd/nornicdb/pkg/cypher/internal/text"
	"github.com/orneryd/nornicdb/pkg/storage"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

func init() {
	for _, name := range []string{"id", "elementid", "labels", "type"} {
		Register(name, func(ctx Context, args []string) (interface{}, error) { return evaluateEntityScalar(ctx, name, args) })
	}
	Register("keys", evalKeys)
	Register("properties", evalProperties)
	Register("size", evalSize)
	Register("tolower", evalToLower)
	Register("toupper", evalToUpper)
	Register("coalesce", evalCoalesce)
	for _, name := range []string{"file", "linenumber"} {
		Register(name, func(ctx Context, args []string) (interface{}, error) {
			if argumentCount(args) != 0 {
				return nil, &ParameterCountError{Function: name, TooMany: true}
			}
			return nil, nil
		})
	}
}

// entityID returns an entity's existing identifier, null for null, or a type
// error. For example, entityID(&storage.Node{ID: "n1"}) returns "n1".
func entityID(value interface{}) (interface{}, error) {
	switch entity := value.(type) {
	case nil:
		return nil, nil
	case *storage.Node:
		if entity == nil {
			return nil, nil
		}
		return string(entity.ID), nil
	case *storage.Edge:
		if entity == nil {
			return nil, nil
		}
		return string(entity.ID), nil
	case storage.RemoteEntityValue:
		for _, key := range []string{"id", "_id"} {
			if id, ok := entity[key].(string); ok {
				return id, nil
			}
		}
		return nil, nil
	case interface{ GetID() string }:
		return entity.GetID(), nil
	default:
		return nil, &ArgumentTypeError{Function: "id", Value: value}
	}
}

// EntityElementID renders a native value using the caller's execution domain.
func EntityElementID(ctx Context, value interface{}) (interface{}, error) {
	switch entity := value.(type) {
	case nil:
		return nil, nil
	case *storage.Node:
		if entity == nil {
			return nil, nil
		}
		if resolver, ok := ctx.Graphs.(interface{ EntityElementIDDatabase(string, string) string }); ok {
			copy := *entity
			{
				metadata := copy.RuntimeMetadata()
				metadata.Database = resolver.EntityElementIDDatabase(entity.RuntimeMetadata().Database, ctx.Database)
				copy.SetRuntimeMetadata(metadata)
			}
			return copy.ElementID(ctx.Database), nil
		}
		return entity.ElementID(ctx.Database), nil
	case *storage.Edge:
		if entity == nil {
			return nil, nil
		}
		if resolver, ok := ctx.Graphs.(interface{ EntityElementIDDatabase(string, string) string }); ok {
			copy := *entity
			copy.Database = resolver.EntityElementIDDatabase(entity.Database, ctx.Database)
			return copy.ElementID(ctx.Database), nil
		}
		return entity.ElementID(ctx.Database), nil
	case storage.RemoteEntityValue:
		return entity["elementId"], nil
	default:
		return nil, &ArgumentTypeError{Function: "elementid", Value: value}
	}
}

// Labels returns the labels of a node, null for null, or an argument error.
// For example, Labels(&storage.Node{Labels: []string{"N"}}) returns ["N"].
func Labels(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if remote, ok := value.(storage.RemoteEntityValue); ok {
		if remote == nil {
			return nil, nil
		}
		if labels, node := remote["labels"]; node {
			return labels, nil
		}
		return nil, &ArgumentTypeError{Function: "labels", Value: value}
	}
	node, ok := value.(*storage.Node)
	if !ok {
		return nil, &ArgumentTypeError{Function: "labels", Value: value}
	}
	if node == nil {
		return nil, nil
	}
	result := make([]interface{}, len(node.Labels))
	for i, label := range node.Labels {
		result[i] = label
	}
	return result, nil
}

// RelationshipType returns a relationship's type, null for null, or an argument
// error. For example, RelationshipType(&storage.Edge{Type: "R"}) returns "R".
func RelationshipType(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if remote, ok := value.(storage.RemoteEntityValue); ok {
		if remote == nil {
			return nil, nil
		}
		if kind, edge := remote["type"]; edge {
			return kind, nil
		}
		return nil, &ArgumentTypeError{Function: "type", Value: value}
	}
	edge, ok := value.(*storage.Edge)
	if !ok {
		return nil, &ArgumentTypeError{Function: "type", Value: value}
	}
	if edge == nil {
		return nil, nil
	}
	return edge.Type, nil
}

func evalKeys(ctx Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, nil
	}
	inner := strings.TrimSpace(args[0])
	if node, ok := ctx.Nodes[inner]; ok && node != nil {
		keys, _ := PropertyKeys(node)
		return keys, nil
	}
	if rel, ok := ctx.Rels[inner]; ok && rel != nil {
		keys, _ := PropertyKeys(rel)
		return keys, nil
	}
	value, _ := ctx.Eval(inner)
	if keys, ok := PropertyKeys(value); ok {
		return keys, nil
	}
	return nil, nil
}

// PropertyKeys is keys(): the property keys of a node, a relationship or a
// map, sorted, so every route returns them in the same order (#602).
//
//   - a node or relationship: the keys of its properties, all of them
//     (a property may be named "labels", "type", "_x", ...);
//   - a RemoteEngine graph value: the keys of its nested properties;
//   - any other map: all of its keys.
//
// It returns false for any other value.
func PropertyKeys(value interface{}) ([]interface{}, bool) {
	var properties map[string]interface{}
	switch v := value.(type) {
	case *storage.Node:
		if v == nil {
			return nil, false
		}
		properties = v.Properties
	case *storage.Edge:
		if v == nil {
			return nil, false
		}
		properties = v.Properties
	case storage.RemoteEntityValue:
		properties, _ = v["properties"].(map[string]interface{})
	case map[string]interface{}:
		properties = storage.PublicRemoteMap(v)
	default:
		return nil, false
	}
	names := make([]string, 0, len(properties))
	for key := range properties {
		names = append(names, key)
	}
	sort.Strings(names)
	keys := make([]interface{}, len(names))
	for i, name := range names {
		keys[i] = name
	}
	return keys, true
}

func evalProperties(ctx Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, nil
	}
	inner := strings.TrimSpace(args[0])
	if node, ok := ctx.Nodes[inner]; ok && node != nil {
		return EntityProperties(node.Properties), nil
	}
	if rel, ok := ctx.Rels[inner]; ok && rel != nil {
		return EntityProperties(rel.Properties), nil
	}
	value, err := ctx.Eval(inner)
	value = RuntimeValue(value)
	if err != nil {
		return nil, err
	}
	if remote, ok := value.(storage.RemoteEntityValue); ok {
		if remote == nil {
			return nil, nil
		}
		properties, _ := remote["properties"].(map[string]interface{})
		if properties == nil {
			properties = map[string]interface{}{}
		}
		return properties, nil
	}
	if object, ok := value.(map[string]interface{}); ok {
		return storage.PublicRemoteMap(object), nil
	}
	return nil, nil
}

func evalSize(ctx Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return int64(0), nil
	}
	v, _ := ctx.Eval(args[0])
	if v == nil {
		return nil, nil
	}
	if path, ok := v.(storage.RemotePathValue); ok {
		if path == nil {
			return nil, nil
		}
		return nil, &TypeMismatchError{Function: "size", Expected: sizeArgumentTypes, Value: v}
	}
	switch vv := v.(type) {
	case string:
		return int64(cyphertext.Length(vv)), nil
	case []interface{}:
		return int64(len(vv)), nil
	case []string:
		return int64(len(vv)), nil
	}
	if kind := reflect.TypeOf(v).Kind(); kind == reflect.Slice || kind == reflect.Array {
		return int64(reflect.ValueOf(v).Len()), nil
	}
	if sized, ok := v.(Sized); ok {
		return sized.CypherSize(), nil
	}
	// size() takes a String or a List; a map, node, relationship, path,
	// number or boolean is a type error, not null.
	return nil, &TypeMismatchError{Function: "size", Expected: sizeArgumentTypes, Value: v}
}

// Sized is a value size() measures that is neither a string nor a list (a
// VECTOR: its dimension).
type Sized interface {
	CypherSize() int64
}

// sizeArgumentTypes is size()'s accepted argument types as Neo4j names them.
const sizeArgumentTypes = "String or List<T>"

// evalToLower is toLower(string): a non-string is a type mismatch, as in
// Neo4j (toLower(5) is not '5').
func evalToLower(ctx Context, args []string) (interface{}, error) {
	return evalStringCase(ctx, args, "toLower", lowerCaseText)
}

// evalToUpper is toUpper(string), as evalToLower.
func evalToUpper(ctx Context, args []string) (interface{}, error) {
	return evalStringCase(ctx, args, "toUpper", upperCaseText)
}

// lowerCaseText and upperCaseText are toLower's and toUpper's Unicode case
// mappings, as plain functions so passing one doesn't allocate a bound
// method value per call.
func lowerCaseText(text string) string { return cases.Lower(language.Und).String(text) }
func upperCaseText(text string) string { return cases.Upper(language.Und).String(text) }

func evalStringCase(ctx Context, args []string, function string, convert func(string) string) (interface{}, error) {
	if len(args) != 1 {
		return nil, nil
	}
	v, err := ctx.Eval(args[0])
	if err != nil || v == nil {
		return nil, err
	}
	text, isString := v.(string)
	if !isString {
		return nil, &TypeMismatchError{Function: function, Expected: "String", Value: v}
	}
	return convert(text), nil
}

func evalCoalesce(ctx Context, args []string) (interface{}, error) {
	value, _, err := Coalesce(len(args), func(i int) (interface{}, bool, error) {
		value, err := ctx.Eval(args[i])
		return value, true, err
	})
	return value, err
}

// EntityProperties is properties() of a node or relationship: its property
// map, the empty map when it has none (a deleted entity's view, #907), never
// null. The row evaluator uses it too.
func EntityProperties(properties map[string]interface{}) map[string]interface{} {
	if properties == nil {
		return map[string]interface{}{}
	}
	return properties
}

// Coalesce is coalesce over n operands, operand i evaluated by eval(i):
// the first non-null value. An unresolved operand remains unresolved; only
// a successfully evaluated NULL permits evaluation of the next operand.
func Coalesce(n int, eval func(index int) (interface{}, bool, error)) (interface{}, bool, error) {
	for index := 0; index < n; index++ {
		value, resolved, err := eval(index)
		if err != nil {
			return nil, false, err
		}
		if !resolved {
			return nil, false, nil
		}
		if value = RuntimeValue(value); value != nil {
			return value, true, nil
		}
	}
	return nil, true, nil
}

// evaluateEntityScalar is the argument and binding owner for entity scalars.
func evaluateEntityScalar(ctx Context, name string, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, nil
	}
	argument := strings.TrimSpace(args[0])
	var value interface{}
	bound := false
	if name != "type" {
		if node, ok := ctx.Nodes[argument]; ok {
			value, bound = node, true
		}
	}
	if !bound && name != "labels" {
		if edge, ok := ctx.Rels[argument]; ok {
			value, bound = edge, true
		}
	}
	if !bound {
		var err error
		value, err = ctx.Eval(argument)
		if err != nil {
			return nil, err
		}
	}
	switch name {
	case "id":
		return entityID(value)
	case "elementid":
		return EntityElementID(ctx, value)
	case "labels":
		return Labels(value)
	default:
		return RelationshipType(value)
	}
}
