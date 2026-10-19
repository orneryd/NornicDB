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
	Register("id", evalID)
	Register("elementid", evalElementID)
	Register("labels", evalLabels)
	Register("type", evalType)
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

func evalID(ctx Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, nil
	}
	inner := strings.TrimSpace(args[0])
	if node, ok := ctx.Nodes[inner]; ok {
		return entityID(node)
	}
	if edge, ok := ctx.Rels[inner]; ok {
		return entityID(edge)
	}
	value, err := ctx.Eval(inner)
	if err != nil {
		return nil, err
	}
	return entityID(value)
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

func evalElementID(ctx Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, nil
	}
	inner := strings.TrimSpace(args[0])
	if node, ok := ctx.Nodes[inner]; ok {
		return entityElementID(node, ctx.Database)
	}
	if edge, ok := ctx.Rels[inner]; ok {
		return entityElementID(edge, ctx.Database)
	}
	value, err := ctx.Eval(inner)
	if err != nil {
		return nil, err
	}
	return entityElementID(value, ctx.Database)
}

func entityElementID(value interface{}, database string) (interface{}, error) {
	switch entity := value.(type) {
	case nil:
		return nil, nil
	case *storage.Node:
		if entity == nil {
			return nil, nil
		}
		return entity.ElementID(database), nil
	case *storage.Edge:
		if entity == nil {
			return nil, nil
		}
		return entity.ElementID(database), nil
	case storage.RemoteEntityValue:
		return entity["elementId"], nil
	default:
		return nil, &ArgumentTypeError{Function: "elementid", Value: value}
	}
}

func evalLabels(ctx Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, nil
	}
	inner := strings.TrimSpace(args[0])
	if node, ok := ctx.Nodes[inner]; ok {
		return Labels(node)
	}
	value, err := ctx.Eval(inner)
	if err != nil {
		return nil, err
	}
	return Labels(value)
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

func evalType(ctx Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, nil
	}
	inner := strings.TrimSpace(args[0])
	if edge, ok := ctx.Rels[inner]; ok {
		return RelationshipType(edge)
	}
	value, err := ctx.Eval(inner)
	if err != nil {
		return nil, err
	}
	return RelationshipType(value)
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
	value, err := ctx.Eval(args[0])
	if err != nil {
		return nil, err
	}
	return Size(value)
}

// Size returns size(value), including null for null. For example, Size("abc")
// returns int64(3); typed length consumers call SizeInteger without boxing it.
func Size(value interface{}) (interface{}, error) {
	length, nonNull, err := SizeInteger(value)
	if err != nil || !nonNull {
		return nil, err
	}
	return length, nil
}

// SizeInteger returns the same length as Size, with nonNull false for null.
// For example, SizeInteger([]string{"a", "b"}) returns (2, true, nil).
func SizeInteger(value interface{}) (length int64, nonNull bool, err error) {
	if value == nil {
		return 0, false, nil
	}
	if path, ok := value.(storage.RemotePathValue); ok {
		if path == nil {
			return 0, false, nil
		}
		return 0, false, &TypeMismatchError{Function: "size", Expected: sizeArgumentTypes, Value: value}
	}
	if text, ok := value.(string); ok {
		return int64(cyphertext.Length(text)), true, nil
	}
	if sized, ok := value.(Sized); ok {
		return sized.CypherSize(), true, nil
	}
	if kind := reflect.TypeOf(value).Kind(); kind == reflect.Slice || kind == reflect.Array {
		return int64(reflect.ValueOf(value).Len()), true, nil
	}
	return 0, false, &TypeMismatchError{Function: "size", Expected: sizeArgumentTypes, Value: value}
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
// the first resolved non-null value; an unresolved operand counts as null,
// returning null when every operand is null or unresolved.
func Coalesce(n int, eval func(index int) (interface{}, bool, error)) (interface{}, bool, error) {
	for index := 0; index < n; index++ {
		value, resolved, err := eval(index)
		if err != nil {
			return nil, false, err
		}
		if resolved && value != nil {
			return value, true, nil
		}
	}
	return nil, true, nil
}
