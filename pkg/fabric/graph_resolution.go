package fabric

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// GraphArgumentEvaluator evaluates the argument expressions of a dynamic
// graph reference (USE graph.byName(g)) with the statement's parameters and
// the current row's variables (params holds both). graph.byName and
// graph.byElementId take a STRING: the evaluator returns one string per
// expression, or Neo4j's error for an argument that isn't one (the Cypher
// executor owns value types and their names).
type GraphArgumentEvaluator func(ctx context.Context, expressions []string, params map[string]interface{}) ([]string, error)

// GraphNotFoundError is Neo4j's Neo.ClientError.Database.DatabaseNotFound
// "Graph not found: <name>" for a graph reference that names no graph.
type GraphNotFoundError struct {
	Name string
}

func (e *GraphNotFoundError) Error() string { return "Graph not found: " + e.Name }

// GraphReferenceError is a dynamic graph reference that can't be used: an
// unknown graph function (a SyntaxError), an argument of the wrong type (a
// TypeError) or a malformed element id. Code is the Neo4j status.
type GraphReferenceError struct {
	Code    string
	Message localization.Message
}

func (e *GraphReferenceError) Error() string { return e.Message.Fallback }

// SetGraphArgumentEvaluator installs the evaluator for dynamic graph
// reference arguments. Without one, a dynamic reference can't run.
func (e *FabricExecutor) SetGraphArgumentEvaluator(evaluate GraphArgumentEvaluator) {
	e.evaluateGraphArguments = evaluate
}

// resolveExecGraph returns the graph a fragment runs on: its static name,
// or its dynamic reference evaluated with params (the statement's
// parameters and the row's variables).
func (e *FabricExecutor) resolveExecGraph(ctx context.Context, f *FragmentExec, params map[string]interface{}) (string, Location, error) {
	name := f.GraphName
	if f.Graph != nil && f.Graph.IsDynamic() {
		resolved, err := e.resolveDynamicGraph(ctx, f, params)
		if err != nil {
			return "", nil, err
		}
		name = resolved
	}
	loc, err := e.catalog.Resolve(name)
	if err != nil {
		if f.Graph != nil {
			return "", nil, &GraphNotFoundError{Name: name}
		}
		return "", nil, fmt.Errorf("cannot route query: %w", err)
	}
	return name, loc, nil
}

// resolveDynamicGraph evaluates a dynamic graph reference to the catalog
// name of a constituent of f.Scope, as Neo4j does on a composite database
// (Cypher Manual, "Composite databases"):
//   - graph.byName(name): the graph with that name, a constituent's
//     qualified name ("cmp.alias", as graph.names() lists it);
//   - graph.byElementId(id): the constituent holding the node or
//     relationship with that element id, whose database the id names.
func (e *FabricExecutor) resolveDynamicGraph(ctx context.Context, f *FragmentExec, params map[string]interface{}) (string, error) {
	function := strings.ToLower(f.Graph.Function)
	if function != "graph.byname" && function != "graph.byelementid" {
		return "", &GraphReferenceError{Code: "Neo.ClientError.Statement.SyntaxError",
			Message: localization.CypherCommandRoutingGraphFunctionUnknown(f.Graph.Function)}
	}
	if len(f.Graph.Args) != 1 {
		return "", &GraphReferenceError{Code: "Neo.ClientError.Statement.SyntaxError",
			Message: graphFunctionParameterCount(f.Graph.Function, len(f.Graph.Args))}
	}
	if e.evaluateGraphArguments == nil {
		return "", fmt.Errorf("dynamic graph reference %s can't be evaluated", f.Graph.Text())
	}
	values, err := e.evaluateGraphArguments(ctx, f.Graph.Args, params)
	if err != nil {
		return "", err
	}
	if len(values) != len(f.Graph.Args) {
		return "", fmt.Errorf("dynamic graph reference %s: %d arguments evaluated to %d values", f.Graph.Text(), len(f.Graph.Args), len(values))
	}
	value := values[0]
	if function == "graph.byname" {
		if !inScope(value, f.Scope) {
			return "", &GraphNotFoundError{Name: value}
		}
		return value, nil
	}
	return e.constituentForElementID(value, f.Scope)
}

// constituentForElementID returns the constituent of scope whose database
// the element id names ("4:<database>:<id>" for a node, "5:…" for a
// relationship, storage.NodeElementID / RelationshipElementID).
func (e *FabricExecutor) constituentForElementID(elementID, scope string) (string, error) {
	parts := strings.SplitN(elementID, ":", 3)
	if len(parts) != 3 || (parts[0] != "4" && parts[0] != "5") || parts[1] == "" {
		return "", &GraphReferenceError{Code: "Neo.ClientError.Statement.ArgumentError",
			Message: localization.CypherCommandRoutingGraphElementIDInvalid(elementID)}
	}
	database := parts[1]
	names := e.catalog.ListGraphs()
	sort.Strings(names)
	for _, name := range names {
		if !strings.HasPrefix(name, strings.ToLower(scope)+".") {
			continue
		}
		loc, err := e.catalog.Resolve(name)
		if err == nil && strings.EqualFold(loc.DatabaseName(), database) {
			return name, nil
		}
	}
	return "", &GraphNotFoundError{Name: elementID}
}

// inScope reports whether graph is a constituent of the composite scope.
func inScope(graph, scope string) bool {
	return scope != "" && len(graph) > len(scope)+1 && strings.EqualFold(graph[:len(scope)+1], scope+".")
}

// graphFunctionParameterCount is Neo4j's message for a graph function called
// with other than one argument.
func graphFunctionParameterCount(function string, count int) localization.Message {
	if count > 1 {
		return localization.CypherCommandRoutingFunctionTooManyParameters(function)
	}
	return localization.CypherCommandRoutingFunctionInsufficientParameters(function)
}
