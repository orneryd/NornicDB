package resultmetadata

import (
	"slices"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// Scope is a set of visible, decoded variable identities. Its zero value is unknown.
type Scope struct{ names Names }

// KnownScope copies, sorts and deduplicates a scope; an empty scope is known.
func KnownScope(names []string) Scope {
	values := append([]string{}, names...)
	slices.Sort(values)
	return Scope{names: Known(slices.Compact(values))}
}

// UnknownScope has no authoritative variable set.
func UnknownScope() Scope { return Scope{} }

// Visit exposes fresh sorted names or explicitly transfers unresolved handling.
func (s Scope) Visit(known func([]string), unknown func()) { s.names.Visit(known, unknown) }

// Columns is an ordered result schema, distinct from a variable scope.
type Columns struct{ names Names }

// KnownColumns copies a header in projection order, including a known empty header.
func KnownColumns(names []string) Columns { return Columns{names: Known(names)} }

// UnknownColumns has no authoritative result schema.
func UnknownColumns() Columns { return Columns{} }

// Visit exposes a fresh ordered header or explicitly transfers unresolved handling.
func (c Columns) Visit(known func([]string), unknown func()) { c.names.Visit(known, unknown) }

// ClauseKind identifies an execution operator without reading its source again.
type ClauseKind uint8

const (
	ClauseOther ClauseKind = iota
	ClauseMatch
	ClauseOptionalMatch
	ClauseWith
	ClauseReturn
	ClauseUnwind
	ClauseCallSubquery
	ClauseProcedure
	ClauseUse
	ClauseWrite
	ClauseFinish
	ClauseLet
	ClauseFilter
)

// CallKind distinguishes exporting calls from unit calls independently of rows.
type CallKind uint8

const (
	CallUnit CallKind = iota + 1
	CallReturning
)

// ImportMode describes a scope clause; legacy WITH imports are not retained.
type ImportMode uint8

const (
	ImportNone ImportMode = iota
	ImportListed
	ImportAll
)

// Clause carries execution's scope transfer and original-query byte span.
type Clause struct {
	Kind                ClauseKind
	Start, End          int
	Before, After       Scope
	Projection          *Projection
	Call                *Call
	ParticipantResolved bool
}

// Call resolves imports and exports once at the point the subquery stands in.
type Call struct {
	Mode       ImportMode
	Imports    []string
	Retained   Scope
	Body       Columns
	Kind       CallKind
	Optional   bool
	After      Scope
	Statement  *Statement
	Resolution Resolution
}

// Resolution records which signature owner completed the procedure facts.
type Resolution uint8

const (
	ResolvedLocally Resolution = iota
	ResolvedByParticipant
)

// SchemaObligation is resolved synchronously by the participant selected by USE.
// All spans address the original query. Incoming supplies declared argument
// variables without making the transport infer them from procedure source text.
type SchemaObligation struct {
	Procedure        string
	Start, End       int
	UseStart, UseEnd int
	Incoming         Scope
}

// ParticipantSchema reads the participant's own signature/protocol metadata.
// An unknown answer or error is terminal before any statement effects.
type ParticipantSchema func(SchemaObligation) (Columns, error)

// Statement is a complete execution scope program, including nested statements.
// Every span addresses the original query, not a rewritten or USE-free copy.
type Statement struct {
	Start, End                int
	Incoming, Retained, After Scope
	Clauses                   []Clause
	Branches                  []Statement
	UnionAll                  []bool
	Columns                   Columns
}

// UnresolvedReason is a typed explanation for a terminal owner failure.
type UnresolvedReason uint8

const (
	ReasonUnspecified UnresolvedReason = iota
	ReasonUndefinedVariable
	ReasonInvalidImport
	ReasonUnsupportedSyntax
	ReasonParserDecline
	ReasonUnknownProcedure
	ReasonDynamicSchema
)

// Unresolved cannot represent a successful or empty schema. Err is always set
// when delivered by Facts.Visit, including for a zero Facts value.
type Unresolved struct {
	Reason   UnresolvedReason
	Position int
	Err      error
}

// Facts is an immutable owner result. Its zero value is terminal unresolved.
type Facts struct {
	statement  *Statement
	declined   bool
	unresolved Unresolved
}

// FactsProvider is the scope and result-schema owner's exact neutral interface.
type FactsProvider interface {
	StatementFacts(query string, incoming Scope, cypher25 bool) Facts
}

// KnownFacts freezes a complete program. Unknown fields and cyclic subqueries
// are rejected, rather than becoming authoritative empty schemas.
func KnownFacts(statement Statement) (Facts, error) {
	if !completeStatement(&statement, make(map[*Statement]bool)) {
		return Facts{}, unresolvedError()
	}
	frozen := cloneStatement(statement)
	return Facts{statement: &frozen}, nil
}

// NotApplicableFacts is only an adapter's decline to an actual continuation.
// StatementFacts itself never returns this outcome.
func NotApplicableFacts() Facts { return Facts{declined: true} }

// UnresolvedFacts preserves a definite error or supplies a localized terminal
// error when the caller has not established one.
func UnresolvedFacts(unresolved Unresolved) Facts {
	if unresolved.Err == nil {
		unresolved.Err = unresolvedError()
	}
	return Facts{unresolved: unresolved}
}

// Visit requires separate handling for a known program, a continuation and a
// terminal error. The known callback receives a deep copy, never cached storage.
func (f Facts) Visit(known func(Statement), notApplicable func(), unresolved func(Unresolved)) {
	if f.statement != nil {
		known(cloneStatement(*f.statement))
	} else if f.declined {
		notApplicable()
	} else {
		reason := f.unresolved
		if reason.Err == nil {
			reason.Position = -1
			reason.Err = unresolvedError()
		}
		unresolved(reason)
	}
}

func unresolvedError() error {
	return localization.NewLocalizedError("Neo.ClientError.Statement.FeatureNotSupported", localization.CypherResultMetadataUnresolved(), nil)
}

func completeStatement(statement *Statement, visiting map[*Statement]bool) bool {
	if visiting[statement] || !statement.Incoming.names.known || !statement.Retained.names.known || !statement.After.names.known || !statement.Columns.names.known || len(statement.UnionAll) != max(0, len(statement.Branches)-1) {
		return false
	}
	visiting[statement] = true
	defer delete(visiting, statement)
	for _, clause := range statement.Clauses {
		if !clause.Before.names.known || !clause.After.names.known || (clause.Projection != nil && !clause.Projection.Names.known) {
			return false
		}
		if call := clause.Call; call != nil {
			if !call.Retained.names.known || !call.Body.names.known || !call.After.names.known || (call.Kind != CallUnit && call.Kind != CallReturning) || call.Statement == nil || !completeStatement(call.Statement, visiting) {
				return false
			}
		}
	}
	for index := range statement.Branches {
		if !completeStatement(&statement.Branches[index], visiting) {
			return false
		}
	}
	return true
}

func cloneStatement(statement Statement) Statement {
	statement.Clauses = append([]Clause(nil), statement.Clauses...)
	for index := range statement.Clauses {
		clause := &statement.Clauses[index]
		if clause.Projection != nil {
			projection := *clause.Projection
			projection.Expressions = append([]string(nil), projection.Expressions...)
			clause.Projection = &projection
		}
		if clause.Call != nil {
			call := *clause.Call
			if call.Imports != nil {
				call.Imports = append([]string{}, call.Imports...)
			}
			if call.Statement != nil {
				body := cloneStatement(*call.Statement)
				call.Statement = &body
			}
			clause.Call = &call
		}
	}
	statement.Branches = append([]Statement(nil), statement.Branches...)
	for index := range statement.Branches {
		statement.Branches[index] = cloneStatement(statement.Branches[index])
	}
	statement.UnionAll = append([]bool(nil), statement.UnionAll...)
	return statement
}
