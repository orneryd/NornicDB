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

func init() {
	// graph.names() lists the graphs of the composite database the query
	// runs on, as their qualified names (composite.alias), the names
	// USE graph.byName(…) takes (Cypher Manual, "Composite databases").
	// Neo4j knows the function only on a composite database.
	Register("graph.names", func(ctx Context, args []string) (interface{}, error) {
		graphs, composite := compositeGraphs(ctx)
		if !composite {
			return nil, &UnknownFunctionError{Function: "graph.names"}
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
		if len(args) != 1 {
			return nil, fmt.Errorf("graph.propertiesByName takes 1 argument, got %d", len(args))
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
	return ctx.Graphs()
}
