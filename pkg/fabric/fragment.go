// Package fabric implements a Neo4j Fabric-compatible distributed query layer.
//
// It provides the Fragment plan tree, Catalog graph registry, FabricPlanner for
// decomposing Cypher queries at USE-clause boundaries, FabricExecutor for dispatching
// fragments to local or remote constituents, and FabricTransaction for coordinating
// per-shard sub-transactions with one-write-shard enforcement.
//
// This package mirrors the architecture of org.neo4j.fabric in the Neo4j source,
// mapped to Go idioms and wired into NornicDB's existing storage and multidb layers.
package fabric

// Fragment is a sealed interface representing a node in the query plan tree
// produced by decomposing a Cypher statement at USE-clause boundaries.
//
// The Fragment tree is the core data structure of the fabric planner.
// It directly mirrors Neo4j's Fragment.scala ADT.
type Fragment interface {
	// fragment is a marker method preventing external implementations.
	fragment()

	// OutputColumns returns the column names produced by this fragment.
	OutputColumns() []string
}

// FragmentInit is the entry point of a Fragment tree.
// It produces a single empty row (like a SQL "dual" table) and defines
// the initial variable scope for downstream fragments.
type FragmentInit struct {
	// Columns lists the argument columns available to downstream fragments.
	Columns []string

	// ImportColumns lists columns imported from an outer scope (correlated subquery).
	ImportColumns []string
}

func (*FragmentInit) fragment() {}

// OutputColumns returns the initial columns.
func (f *FragmentInit) OutputColumns() []string {
	return f.Columns
}

// FragmentLeaf is an intermediate fragment representing raw Cypher clauses
// before they are bound to a specific graph location.
type FragmentLeaf struct {
	// Input is the fragment that feeds rows into this leaf.
	Input Fragment

	// Clauses contains the raw Cypher clause text.
	Clauses string

	// Columns lists the output column names produced by this leaf.
	Columns []string
}

func (*FragmentLeaf) fragment() {}

// OutputColumns returns the leaf's output columns.
func (f *FragmentLeaf) OutputColumns() []string {
	return f.Columns
}

// FragmentExec is an executable fragment bound to a specific graph location.
// It represents a unit of work that can be dispatched to either a local or remote executor.
type FragmentExec struct {
	// Input is the fragment that feeds rows into this executable unit.
	Input Fragment

	// Query is the Cypher query string to execute against the target graph.
	Query string

	// GraphName identifies the target graph (e.g. "nornic.tr").
	// For dotted names, the part before the dot is the composite database
	// and the part after the dot is the constituent alias.
	GraphName string

	// Graph is a dynamic graph reference (USE graph.byName(…) or
	// graph.byElementId(…)), resolved each time the fragment runs from the
	// statement's parameters and the input row; GraphName is then empty.
	Graph *UseClause

	// Scope is the composite database whose constituents a dynamic graph
	// reference may name.
	Scope string

	// Columns lists the output column names produced by this execution.
	Columns []string

	// IsWrite indicates whether this fragment performs write operations.
	IsWrite bool
}

func (*FragmentExec) fragment() {}

// OutputColumns returns the exec's output columns.
func (f *FragmentExec) OutputColumns() []string {
	return f.Columns
}

// FragmentApply represents sequential execution: for each row from Input,
// execute Inner and concatenate results. This models correlated subqueries
// where the inner query references variables from the outer scope.
type FragmentApply struct {
	// Input is the outer fragment producing driver rows.
	Input Fragment

	// Inner is the fragment executed once per input row.
	Inner Fragment

	// Columns lists the combined output columns from Input and Inner.
	Columns []string
}

func (*FragmentApply) fragment() {}

// OutputColumns returns the apply's output columns.
func (f *FragmentApply) OutputColumns() []string {
	return f.Columns
}

// FragmentUnion represents parallel execution of two branches with result merging.
// If Distinct is true, duplicate rows are removed from the merged result.
type FragmentUnion struct {
	// Init is the shared entry point for both branches.
	Init *FragmentInit

	// LHS is the left-hand branch.
	LHS Fragment

	// RHS is the right-hand branch.
	RHS Fragment

	// Distinct controls whether duplicate rows are eliminated.
	Distinct bool

	// Columns lists the output column names (must match between LHS and RHS).
	Columns []string
}

func (*FragmentUnion) fragment() {}

// OutputColumns returns the union's output columns.
func (f *FragmentUnion) OutputColumns() []string {
	return f.Columns
}
