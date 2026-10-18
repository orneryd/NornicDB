package cypher

import (
	"context"
	"reflect"
	"runtime"
	"strings"

	"github.com/orneryd/nornicdb/pkg/apocmeta"
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Native APOC functions read the query's graph, not the plugin package's
// independent in-memory graph. Compatible explicitly registered handlers
// retain precedence; this owner supplies the native-value fallback.
func nativeAPOCFunctionCatalog() []cypherFunctionSpec {
	var catalog []cypherFunctionSpec
	for _, function := range apocmeta.NativeFunctions() {
		var arguments []ProcedureParam
		for _, argument := range function.Arguments {
			arguments = append(arguments, ProcedureParam{Name: argument.Name, Type: argument.Type, Description: argument.Description, Default: argument.Default, Optional: argument.Optional, IsDeprecated: argument.IsDeprecated})
		}
		volatility := functionStable
		if function.Volatile {
			volatility = functionVolatile
		}
		catalog = append(catalog, cypherFunctionSpec{cacheVolatility: volatility, name: function.Name, category: function.Category, signature: function.Signature, description: function.Description, argumentDescriptions: function.ArgumentDescriptions, arguments: arguments, listed: true, userDefined: true})
	}
	return catalog
}

func isNativeAPOCFunction(name string) bool { return apocmeta.IsNativeValue(name) }
func nativeAPOCCaseMismatch(name string) bool {
	return apocmeta.IsNativeValueCaseMismatch(name)
}
func nativeAPOCCallFailure(name string, value interface{}) error {
	return newSemanticError("Neo.ClientError.Procedure.ProcedureCallFailed", "ProcedureCallFailed", invalidFunctionArgument(name, value).Error())
}

func (e *StorageExecutor) evaluateNativeAPOCFunction(ctx context.Context, name string, args []interface{}) (interface{}, bool, error) {
	if !isNativeAPOCFunction(name) {
		return nil, false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	// Arity is also checked before execution by the shared function catalog.
	min, max := 1, 1
	switch name {
	case "apoc.node.degree", "apoc.temporal.format":
		max = 2
	case "apoc.nodes.connected":
		min, max = 2, 3
	case "apoc.path.slice":
		max = 3
	case "apoc.path.combine":
		min, max = 2, 2
	}
	if len(args) < min || len(args) > max {
		return nil, true, nativeAPOCCallFailure(name, args)
	}
	value := args[0]
	switch name {
	case "apoc.node.id", "apoc.rel.id", "apoc.node.labels", "apoc.rel.type":
		expected := valueKindNode
		if strings.HasPrefix(name, "apoc.rel.") {
			expected = valueKindRelationship
		}
		if value != nil && cypherValueKindOf(value) != expected {
			return nil, true, invalidFunctionArgument(name, value)
		}
		builtin := "id"
		if name == "apoc.node.labels" {
			builtin = "labels"
		}
		if name == "apoc.rel.type" {
			builtin = "type"
		}
		v, _, err := cypherfn.EvaluateFunction(builtin, []string{"value"}, cypherfn.Context{Eval: func(string) (interface{}, error) { return value, nil }, Storage: e.storage, Database: e.executionDatabaseName(ctx)})
		if err != nil {
			err = functionEvaluationError(err)
		}
		return v, true, err
	case "apoc.rel.startNode", "apoc.rel.endNode":
		v, err := e.relationshipEndpointValue(value, name == "apoc.rel.endNode")
		return v, true, err
	case "apoc.node.degree", "apoc.nodes.connected":
		v, err := e.nativeAPOCAdjacency(ctx, name, args)
		return v, true, err
	case "apoc.path.elements":
		if value == nil {
			return nil, true, nil
		}
		v, ok := cypherPathElements(value)
		if !ok {
			return nil, true, invalidFunctionArgument(name, value)
		}
		return v, true, nil
	case "apoc.path.slice", "apoc.path.combine":
		v, err := e.nativeAPOCPath(name, args)
		return v, true, err
	case "apoc.temporal.format":
		v, err := nativeAPOCTemporalFormat(args)
		return v, true, err
	}
	return nil, false, nil
}

func (e *StorageExecutor) nativeAPOCAdjacency(ctx context.Context, name string, args []interface{}) (interface{}, error) {
	connected := name == "apoc.nodes.connected"
	if args[0] == nil {
		if connected {
			return false, nil
		}
		return nil, nativeAPOCCallFailure(name, args[0])
	}
	node, ok := args[0].(*storage.Node)
	if !ok || node == nil {
		return nil, invalidFunctionArgument(name, args[0])
	}
	var target *storage.Node
	filterIndex := 1
	if connected {
		filterIndex = 2
		if args[1] == nil {
			return false, nil
		}
		target, ok = args[1].(*storage.Node)
		if !ok || target == nil {
			return nil, invalidFunctionArgument(name, args[1])
		}
	}
	filter := ""
	if len(args) > filterIndex {
		filter, ok = args[filterIndex].(string)
		if !ok {
			return nil, invalidFunctionArgument(name, args[filterIndex])
		}
	}
	steps := parseApocRelationshipFilter(filter)
	matches := func(edge *storage.Edge, direction string) bool {
		if edge == nil {
			return false
		}
		if len(steps) == 0 {
			return true
		}
		for _, step := range steps {
			if (step.relType == "" || step.relType == edge.Type) && (step.direction == "both" || step.direction == direction) {
				return true
			}
		}
		return false
	}
	outgoing, incoming, err := incidentEdgeListsContext(ctx, e.storage, node.ID, "both")
	if err != nil {
		return nil, err
	}
	var count int64
	for _, pair := range []struct {
		edges     []*storage.Edge
		direction string
	}{{outgoing, "out"}, {incoming, "in"}} {
		for i, edge := range pair.edges {
			if i&255 == 0 && ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if !matches(edge, pair.direction) {
				continue
			}
			if connected {
				other := edge.EndNode
				if pair.direction == "in" {
					other = edge.StartNode
				}
				if other == target.ID {
					return true, nil
				}
			} else {
				count++
			}
		}
	}
	if connected {
		return false, nil
	}
	return count, nil
}

// nativeAPOCPath preserves genuine typed local and remote path carriers.
func (e *StorageExecutor) nativeAPOCPath(name string, args []interface{}) (interface{}, error) {
	if name == "apoc.path.combine" {
		if args[0] == nil {
			return args[1], nil
		}
		if args[1] == nil {
			return args[0], nil
		}
	} else if args[0] == nil {
		return nil, nil
	}
	elements, ok := cypherPathElements(args[0])
	if !ok || len(elements) == 0 {
		return nil, invalidFunctionArgument(name, args[0])
	}
	if name == "apoc.path.slice" {
		offset, length := 0, -1
		for i, dst := range []*int{&offset, &length} {
			if len(args) > i+1 {
				var valid bool
				*dst, valid = toInt(args[i+1])
				if !valid {
					return nil, invalidFunctionArgument(name, args[i+1])
				}
			}
		}
		hops := (len(elements) - 1) / 2
		if offset < 0 {
			offset = 0
		}
		if offset > hops {
			offset = hops
		}
		if length == -1 || length > hops-offset {
			length = hops - offset
		}
		if length < 0 {
			length = 0
		}
		elements = elements[offset*2 : (offset+length)*2+1]
	} else {
		second, valid := cypherPathElements(args[1])
		if !valid || len(second) == 0 {
			return nil, invalidFunctionArgument(name, args[1])
		}
		if equal, ok := cypherEqualityValue(elements[len(elements)-1], second[0]).(bool); !ok || !equal {
			return nil, nativeAPOCCallFailure(name, args)
		}
		elements = append(append([]interface{}{}, elements...), second[1:]...)
	}
	if _, remote := args[0].(storage.RemotePathValue); remote {
		out := make(storage.RemotePathValue, len(elements))
		for i, v := range elements {
			entity, ok := v.(storage.RemoteEntityValue)
			if !ok {
				return nil, invalidFunctionArgument(name, v)
			}
			out[i] = entity
		}
		return out, nil
	}
	path := PathResult{Length: (len(elements) - 1) / 2}
	for i, value := range elements {
		if i%2 == 0 {
			node, ok := value.(*storage.Node)
			if !ok {
				return nil, invalidFunctionArgument(name, value)
			}
			path.Nodes = append(path.Nodes, node)
		} else {
			edge, ok := value.(*storage.Edge)
			if !ok {
				return nil, invalidFunctionArgument(name, value)
			}
			path.Relationships = append(path.Relationships, edge)
		}
	}
	return e.pathToMap(path), nil
}

// APOC's standalone Go graph uses integer IDs and its own global store. It
// cannot consume native query entities, including a nil entity: allowing nil
// through reflection would invoke the foreign helper and panic instead of
// applying the function's null contract. Native/custom compatible handlers
// remain eligible for ordinary plugin precedence.
func nativeAPOCForeignGraphHandler(name string, handler interface{}) bool {
	if !isNativeAPOCFunction(name) {
		return false
	}
	if name == "apoc.temporal.format" && handler != nil {
		v := reflect.ValueOf(handler)
		if v.Kind() == reflect.Func && !v.IsNil() {
			if fn := runtime.FuncForPC(v.Pointer()); fn != nil && fn.Name() == "github.com/orneryd/nornicdb/apoc/temporal.Format" {
				return true
			}
		}
	}
	typ := reflect.TypeOf(handler)
	if typ == nil || typ.Kind() != reflect.Func {
		return false
	}
	for i := 0; i < typ.NumIn(); i++ {
		arg := typ.In(i)
		for arg.Kind() == reflect.Pointer || arg.Kind() == reflect.Slice || arg.Kind() == reflect.Array {
			arg = arg.Elem()
		}
		if arg.PkgPath() == "github.com/orneryd/nornicdb/apoc/storage" {
			return true
		}
	}
	return false
}
