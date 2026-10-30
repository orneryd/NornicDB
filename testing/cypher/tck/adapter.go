package tck

import (
	"context"
	"fmt"
)

// Backend is the database-independent contract used by the TCK bindings. A
// production Bolt implementation and deterministic test doubles use the same
// surface so comparison behavior is tested independently from transport code.
type Backend interface {
	Reset(context.Context) error
	LoadNamedGraph(context.Context, string) error
	Execute(context.Context, string, map[string]any) (QueryResult, error)
	Snapshot(context.Context) (GraphSnapshot, error)
}

// BackendFactory creates an isolated backend for one scenario.
type BackendFactory func(context.Context) (Backend, error)

// BackendCloser is implemented by backends that own per-scenario resources.
type BackendCloser interface {
	Close(context.Context) error
}

// ProcedureRegistrar is implemented by backends that support TCK procedure
// fixtures. The signature and rows are retained in their upstream form.
type ProcedureRegistrar interface {
	RegisterProcedure(context.Context, string, [][]string) error
}

// QueryResult is a fully consumed query result. Columns retain projection order
// and every row must have the same width as Columns.
type QueryResult struct {
	Columns []string
	Rows    [][]any
}

// QueryError classifies an execution failure using the TCK error vocabulary.
type QueryError struct {
	Type   string
	Phase  string
	Detail string
	Cause  error
}

func (e *QueryError) Error() string {
	if e == nil {
		return "<nil>"
	}
	message := fmt.Sprintf("%s at %s: %s", e.Type, e.Phase, e.Detail)
	if e.Cause != nil {
		return message + ": " + e.Cause.Error()
	}
	return message
}

func (e *QueryError) Unwrap() error { return e.Cause }

// NodeValue is the transport-neutral representation of a returned node.
type NodeValue struct {
	Identity   string
	Labels     []string
	Properties map[string]any
}

// RelationshipValue is the transport-neutral representation of a returned
// relationship. Endpoint identities allow paths to preserve topology.
type RelationshipValue struct {
	Identity      string
	Type          string
	StartIdentity string
	EndIdentity   string
	Properties    map[string]any
}

// PathSegment records one directed relationship as traversed by a path.
type PathSegment struct {
	Relationship RelationshipValue
	Forward      bool
}

// PathValue contains nodes in traversal order and one segment between each
// adjacent pair.
type PathValue struct {
	Nodes    []NodeValue
	Segments []PathSegment
}

// GraphSnapshot contains the observable graph state used for side-effect
// accounting. Identities need only be stable for the lifetime of a scenario.
type GraphSnapshot struct {
	Nodes         []NodeValue
	Relationships []RelationshipValue
}

// SideEffects is the net observable additions and removals defined by the TCK.
type SideEffects struct {
	AddedNodes           int
	RemovedNodes         int
	AddedRelationships   int
	RemovedRelationships int
	AddedProperties      int
	RemovedProperties    int
	AddedLabels          int
	RemovedLabels        int
}
