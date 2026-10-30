package fn

import (
	"fmt"
	"strings"
)

// UnknownFunctionError is a call of a function that isn't available where
// the query runs (graph.names() outside a composite database). Callers turn
// it into Neo4j's SyntaxError "Unknown function '<name>'".
type UnknownFunctionError struct {
	Function string
}

func (e *UnknownFunctionError) Error() string {
	return fmt.Sprintf("Unknown function '%s'", e.Function)
}

// GraphNotFoundError is a graph function's argument that names no graph of
// the composite database. Callers turn it into Neo4j's DatabaseNotFound
// "Graph not found: <name>".
type GraphNotFoundError struct {
	Name string
}

func (e *GraphNotFoundError) Error() string { return "Graph not found: " + e.Name }

// GraphSelectionContextError rejects graph selectors outside a USE clause.
// For example, RETURN graph.byName('tenant') fails with this diagnostic;
// USE graph.byName('tenant') is resolved by the USE planner instead.
type GraphSelectionContextError struct {
	Function string
}

func (e *GraphSelectionContextError) Error() string {
	return fmt.Sprintf("`%s` is only allowed at the first position of a USE clause.", e.Function)
}

// ParameterCountError is a call with more arguments than the function takes
// (TooMany) or fewer. Callers turn it into Neo4j's SyntaxError "Too many
// parameters for function '<name>'" / "Insufficient parameters for function
// '<name>'".
type ParameterCountError struct {
	Function string
	TooMany  bool
}

func (e *ParameterCountError) Error() string {
	if e.TooMany {
		return fmt.Sprintf("Too many parameters for function '%s'", e.Function)
	}
	return fmt.Sprintf("Insufficient parameters for function '%s'", e.Function)
}

// argumentCount is the number of arguments in a call's argument list, where
// an empty list may reach a function as one empty argument.
func argumentCount(args []string) int {
	if len(args) == 1 && strings.TrimSpace(args[0]) == "" {
		return 0
	}
	return len(args)
}

func init() {
	for _, name := range []string{"graph.byName", "graph.byElementId"} {
		Register(name, func(ctx Context, args []string) (interface{}, error) {
			return nil, &GraphSelectionContextError{Function: name}
		})
	}
	// graph.names() lists the graphs of the composite database the query
	// runs on, as their qualified names (composite.alias), the names
	// USE graph.byName(…) takes (Cypher Manual, "Composite databases").
	// Neo4j knows the function only on a composite database.
	Register("graph.names", func(ctx Context, args []string) (interface{}, error) {
		graphs, composite := compositeGraphs(ctx)
		if !composite {
			return nil, &UnknownFunctionError{Function: "graph.names"}
		}
		if argumentCount(args) > 0 {
			return nil, &ParameterCountError{Function: "graph.names", TooMany: true}
		}
		out := make([]interface{}, len(graphs))
		for i, graph := range graphs {
			out[i] = graph
		}
		return out, nil
	})
	// graph.propertiesByName(name) is the map of properties of the alias
	// that added the graph to the composite database. NornicDB aliases have
	// no properties, so it is an empty map for each of the composite's
	// graphs.
	Register("graph.propertiesByName", func(ctx Context, args []string) (interface{}, error) {
		graphs, composite := compositeGraphs(ctx)
		if !composite {
			return nil, &UnknownFunctionError{Function: "graph.propertiesByName"}
		}
		if count := argumentCount(args); count != 1 {
			return nil, &ParameterCountError{Function: "graph.propertiesByName", TooMany: count > 1}
		}
		value, err := ctx.Eval(args[0])
		if err != nil {
			return nil, err
		}
		name, ok := value.(string)
		if !ok {
			return nil, &ArgumentTypeError{Function: "graph.propertiesByName", Value: value}
		}
		for _, graph := range graphs {
			if strings.EqualFold(graph, name) {
				return map[string]interface{}{}, nil
			}
		}
		return nil, &GraphNotFoundError{Name: name}
	})
}

func compositeGraphs(ctx Context) ([]string, bool) {
	if ctx.Graphs == nil {
		return nil, false
	}
	return ctx.Graphs.CompositeGraphs()
}
