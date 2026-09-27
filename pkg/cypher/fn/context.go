package fn

import (
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Context provides runtime access for function evaluation without importing the parent
// `cypher` package (avoids import cycles).
//
// Eval must evaluate a Cypher expression in the caller's scope (row bindings).
// Functions can use Eval for lazy argument evaluation (e.g. coalesce).
type Context struct {
	Nodes map[string]*storage.Node
	Rels  map[string]*storage.Edge
	// Database identifies the graph namespace used to construct elementId()
	// values. Empty uses the storage package's default database name.
	Database string

	Eval func(expr string) (interface{}, error)
	Now  func() time.Time

	// Graphs lists the graphs of the composite database the query runs on.
	// Nil means not a composite database.
	Graphs GraphCatalog
}

// GraphCatalog lists the graphs of the composite database a query runs on:
// their qualified names (composite.alias) and whether it is a composite
// database. It is an interface rather than a function so that a context
// built per row can carry its executor without allocating (a method value
// would build a closure each time).
type GraphCatalog interface {
	CompositeGraphs() (graphs []string, composite bool)
}
