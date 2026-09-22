package fn

import (
	"fmt"
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
	if node, ok := v.(*storage.Node); ok && node != nil {
		result := make([]interface{}, len(node.Labels))
		for i, label := range node.Labels {
			result[i] = label
		}
		return result, nil
	}
	return nil, nil
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
		keys := make([]interface{}, 0, len(node.Properties))
		for k := range node.Properties {
			keys = append(keys, k)
		}
		return keys, nil
	}
	if rel, ok := ctx.Rels[inner]; ok && rel != nil {
		keys := make([]interface{}, 0, len(rel.Properties))
		for k := range rel.Properties {
			keys = append(keys, k)
		}
		return keys, nil
	}
	value, _ := ctx.Eval(inner)
	if object, ok := value.(map[string]interface{}); ok {
		keys := make([]interface{}, 0, len(object))
		for key := range object {
			keys = append(keys, key)
		}
		return keys, nil
	}
	return nil, nil
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
		return object, nil
	}
	return nil, nil
}

func evalSize(ctx Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return int64(0), nil
	}
	v, _ := ctx.Eval(args[0])
	switch vv := v.(type) {
	case string:
		return int64(cyphertext.Length(vv)), nil
	case []interface{}:
		return int64(len(vv)), nil
	case []string:
		return int64(len(vv)), nil
	}
	return int64(0), nil
}

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
