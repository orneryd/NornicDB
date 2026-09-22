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
}
