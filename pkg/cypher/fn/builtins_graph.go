package fn

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/queryprofile"
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

// profileKindCompositeGraphCatalog names the one catalog acquisition
// graph.names() and graph.propertiesByName() perform: the graphs this
// database's catalog listed for the composite database the statement runs on,
// read before either function filters them by the statement's visibility. Rows
// is that listed population and Hits the one acquisition - abstract logical
// catalog operations, not disk reads, page-cache hits or promised Neo4j
// numbers.
// (https://neo4j.com/docs/cypher-manual/current/planning-and-tuning/execution-plans/)
const profileKindCompositeGraphCatalog = "CompositeGraphCatalog"

func init() {
	for _, name := range []string{"graph.byName", "graph.byElementId"} {
		Register(name, func(workCtx context.Context, ctx Context, args []string) (interface{}, error) {
			return nil, &GraphSelectionContextError{Function: name}
		}, CanonicalReads)
	}
	// graph.names() lists the graphs of the composite database the query
	// runs on, as their qualified names (composite.alias), the names
	// USE graph.byName(…) takes (Cypher Manual, "Composite databases").
	// Neo4j knows the function only on a composite database.
	//
	// A call's whole database access is the one catalog acquisition
	// compositeGraphs performs and records, so it is observed without the
	// opaque-callback hook; the visibility filter that follows reads
	// permission metadata, not the database.
	Register("graph.names", func(workCtx context.Context, ctx Context, args []string) (interface{}, error) {
		graphs, composite := compositeGraphs(workCtx, ctx)
		if !composite {
			return nil, &UnknownFunctionError{Function: "graph.names"}
		}
		if argumentCount(args) > 0 {
			return nil, &ParameterCountError{Function: "graph.names", TooMany: true}
		}
		out := make([]interface{}, 0, len(graphs))
		for _, graph := range graphs {
			if ctx.CatalogVisibility.CanSeeDatabase(graph) {
				out = append(out, graph)
			}
		}
		return out, nil
	}, CanonicalReads)
	// graph.propertiesByName(name) is the map of properties of the alias
	// that added the graph to the composite database. NornicDB aliases have
	// no properties, so it is an empty map for each of the composite's
	// graphs. Its whole database access is the same one catalog acquisition.
	Register("graph.propertiesByName", func(workCtx context.Context, ctx Context, args []string) (interface{}, error) {
		graphs, composite := compositeGraphs(workCtx, ctx)
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
			if strings.EqualFold(graph, name) && ctx.CatalogVisibility.CanSeeDatabase(graph) {
				return map[string]interface{}{}, nil
			}
		}
		return nil, &GraphNotFoundError{Name: name}
	}, CanonicalReads)
}

// compositeGraphs acquires the graphs of the composite database the statement
// runs on: the one catalog request both graph functions make, recorded before
// either filters what the catalog listed. A context that carries no catalog
// lists nothing and performs no request, so it records none.
func compositeGraphs(workCtx context.Context, ctx Context) ([]string, bool) {
	if ctx.Graphs == nil {
		return nil, false
	}
	graphs, composite := ctx.Graphs.CompositeGraphs()
	queryprofile.Access(workCtx, profileKindCompositeGraphCatalog, "").Add(int64(len(graphs)), 1)
	return graphs, composite
}
