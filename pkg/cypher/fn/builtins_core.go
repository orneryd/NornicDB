package fn

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	cyphertext "github.com/orneryd/nornicdb/pkg/cypher/internal/text"
	"github.com/orneryd/nornicdb/pkg/storage"
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
}

func evalID(ctx Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, nil
	}
	inner := strings.TrimSpace(args[0])
	if node, ok := ctx.Nodes[inner]; ok && node != nil {
		return string(node.ID), nil
	}
	if rel, ok := ctx.Rels[inner]; ok && rel != nil {
		return string(rel.ID), nil
	}
	v, _ := ctx.Eval(inner)
	switch vv := v.(type) {
	case *storage.Node:
		if vv != nil {
			return string(vv.ID), nil
		}
	case *storage.Edge:
		if vv != nil {
			return string(vv.ID), nil
		}
	case map[string]interface{}:
		// A node or relationship projected as a computed-row map.
		for _, key := range []string{"id", "_id"} {
			if id, ok := vv[key]; ok {
				if text, isString := id.(string); isString {
					return text, nil
				}
			}
		}
	case interface{ GetID() string }:
		return vv.GetID(), nil
	}
	return nil, nil
}

func evalElementID(ctx Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, nil
	}
	inner := strings.TrimSpace(args[0])
	if node, ok := ctx.Nodes[inner]; ok && node != nil {
		return storage.NodeElementID(ctx.Database, node.ID), nil
	}
	if rel, ok := ctx.Rels[inner]; ok && rel != nil {
		return storage.RelationshipElementID(ctx.Database, rel.ID), nil
	}
	v, _ := ctx.Eval(inner)
	if m, ok := v.(map[string]interface{}); ok {
		// A node projected as a computed-row map carries its elementId.
		if elementID, present := m["elementId"]; present {
			return elementID, nil
		}
	}
	return nil, nil
}

func evalLabels(ctx Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, nil
	}
	inner := strings.TrimSpace(args[0])
	if node, ok := ctx.Nodes[inner]; ok && node != nil {
		result := make([]interface{}, len(node.Labels))
		for i, label := range node.Labels {
			result[i] = label
		}
		return result, nil
	}
	v, _ := ctx.Eval(inner)
	switch value := v.(type) {
	case nil:
		return nil, nil
	case *storage.Node:
		if value == nil {
			return nil, nil
		}
		result := make([]interface{}, len(value.Labels))
		for i, label := range value.Labels {
			result[i] = label
		}
		return result, nil
	case map[string]interface{}:
		// A node projected as a map: the projected label list is the
		// contract, with or without the nodeToMap _nodeId marker.
		if labels, ok := value["labels"]; ok {
			return labels, nil
		}
		if _, isNode := value["_nodeId"]; isNode {
			return value["labels"], nil
		}
	}
	return nil, &ArgumentTypeError{Function: "labels", Value: v}
}

func evalType(ctx Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, nil
	}
	inner := strings.TrimSpace(args[0])
	if rel, ok := ctx.Rels[inner]; ok && rel != nil {
		return rel.Type, nil
	}
	v, _ := ctx.Eval(inner)
	if m, ok := v.(map[string]interface{}); ok {
		if t, ok := m["type"]; ok {
			return t, nil
		}
	}
	return nil, nil
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
//   - a node or relationship converted to a map (nodeToMap / edgeToMap:
//     "_nodeId" / "_edgeId" with the properties under "properties"): the
//     keys of those properties;
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
	case map[string]interface{}:
		properties = v
		if isConvertedEntityMap(v) {
			if nested, ok := v["properties"].(map[string]interface{}); ok {
				properties = nested
			}
		}
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

// isConvertedEntityMap reports whether m is a node or relationship converted
// to a result map (it carries the internal "_nodeId" / "_edgeId").
func isConvertedEntityMap(m map[string]interface{}) bool {
	if id, ok := m["_nodeId"].(string); ok && id != "" {
		return true
	}
	id, ok := m["_edgeId"].(string)
	return ok && id != ""
}

func evalProperties(ctx Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, nil
	}
	inner := strings.TrimSpace(args[0])
	if node, ok := ctx.Nodes[inner]; ok && node != nil {
		return node.Properties, nil
	}
	if rel, ok := ctx.Rels[inner]; ok && rel != nil {
		return rel.Properties, nil
	}
	value, _ := ctx.Eval(inner)
	if object, ok := value.(map[string]interface{}); ok {
		// A node projected as a map: properties live in the "properties"
		// sub-map, which takes precedence over same-named top-level keys.
		if props, nested := object["properties"].(map[string]interface{}); nested {
			return props, nil
		}
		return object, nil
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
	// size() takes a String or a List; a map, node, relationship, path,
	// number or boolean is a type error, not null.
	return nil, &TypeMismatchError{Function: "size", Expected: sizeArgumentTypes, Value: v}
}

// sizeArgumentTypes is size()'s accepted argument types as Neo4j names them.
const sizeArgumentTypes = "String or List<T>"

func evalToLower(ctx Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, nil
	}
	v, _ := ctx.Eval(args[0])
	if v == nil {
		return nil, nil
	}
	return strings.ToLower(fmt.Sprintf("%v", v)), nil
}

func evalToUpper(ctx Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, nil
	}
	v, _ := ctx.Eval(args[0])
	if v == nil {
		return nil, nil
	}
	return strings.ToUpper(fmt.Sprintf("%v", v)), nil
}

func evalCoalesce(ctx Context, args []string) (interface{}, error) {
	for _, a := range args {
		v, _ := ctx.Eval(a)
		if v != nil {
			return v, nil
		}
	}
	return nil, nil
}
