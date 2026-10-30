package fn

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Context provides runtime access for function evaluation without importing the parent
// `cypher` package (avoids import cycles).
//
// Eval is the external compatibility boundary. Expression hosts evaluate its
// text as a fresh standalone expression in the current row scope; equal strings
// do not claim occurrence identity in the enclosing statement. A host that
// already supplied a value (native APOC's entity aliases) retains release's
// value-bound callback. Internal functions use retained argument positions in
// InvocationContext instead.
// Author: #1663 implementation lane. Compatibility allocation: orchestrator Nornic2.
type Context struct {
	Nodes map[string]*storage.Node
	Rels  map[string]*storage.Edge
	// Storage is the graph the statement reads, for functions over the
	// graph (GDS's link-prediction functions, gds.util.asNode).
	Storage storage.Engine
	// Database identifies the graph namespace used to construct elementId()
	// values. Empty uses the storage package's default database name.
	Database string

	Eval func(expr string) (interface{}, error)
	Now  func() time.Time

	// Graphs lists the graphs of the composite database the query runs on.
	// Nil means not a composite database.
	Graphs GraphCatalog

	// Cypher25 is set while a Cypher 25 statement runs: a function whose
	// results or errors Neo4j 2026.09 changed follows Neo4j 2026.09 then, and
	// Neo4j 5.26 for a Cypher 5 statement (#907).
	Cypher25 bool
}

// InvocationContext carries the host's retained argument identities. Eval
// selects an existing argument position; the host evaluates its carried Source
// window. Shadowing Context.Eval makes a string-taking internal call a compiler
// error. Context.Eval is reserved for the external Func compatibility adapter.
type InvocationContext struct {
	Context
	Eval func(index int) (interface{}, error)
	// EvalSpan selects an extent emitted by an argument's own grammar (for
	// example TRIM's FROM form), relative to that retained argument window.
	EvalSpan func(index int, span cyphertext.Span) (interface{}, error)
}

// GraphCatalog lists the graphs of the composite database a query runs on:
// their qualified names (composite.alias) and whether it is a composite
// database. It is an interface rather than a function so that a context
// built per row can carry its executor without allocating (a method value
// would build a closure each time).
type GraphCatalog interface {
	CompositeGraphs() (graphs []string, composite bool)
}

// GraphStorage resolves the execution selection only for a function that reads
// graph data. Pure value/catalog functions retain the record fragment path.
func (ctx Context) GraphStorage() (storage.Engine, error) {
	if resolver, ok := ctx.Graphs.(interface {
		FunctionStorage(string) (storage.Engine, error)
	}); ok {
		return resolver.FunctionStorage(ctx.Database)
	}
	return ctx.Storage, nil
}
