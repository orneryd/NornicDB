package resultmetadata

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"slices"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// BindingID identifies one declaration within a compiled statement.
type BindingID uint32

// Scope is a set of visible, decoded variable identities. Its zero value is unknown.
type Scope struct {
	names    Names
	bindings map[string]BindingID
}

// KnownScope copies, sorts and deduplicates a scope; an empty scope is known.
func KnownScope(names []string) Scope {
	values := append([]string{}, names...)
	slices.Sort(values)
	return Scope{names: Known(slices.Compact(values))}
}

// KnownBindingScope freezes the compiler's identities at one scope horizon.
// Names-only scopes remain available for structural plans and external inputs.
func KnownBindingScope(names []string, identities map[string]BindingID) Scope {
	scope := KnownScope(names)
	scope.bindings = make(map[string]BindingID, len(names))
	for _, name := range names {
		if id, ok := identities[name]; ok {
			scope.bindings[name] = id
		}
	}
	return scope
}

// Binding returns the canonical declaration of name at this horizon, if known.
func (s Scope) Binding(name string) (BindingID, bool) {
	id, ok := s.bindings[name]
	return id, ok
}

// UnknownScope has no authoritative variable set.
func UnknownScope() Scope { return Scope{} }

// Visit exposes fresh sorted names or explicitly transfers unresolved handling.
func (s Scope) Visit(known func([]string), unknown func()) { s.names.Visit(known, unknown) }

// Columns is an ordered result schema, distinct from a variable scope.
type Columns struct {
	names        Names
	alternatives [][]string
}

// KnownColumns copies a header in projection order, including a known empty header.
func KnownColumns(names []string) Columns { return Columns{names: Known(names)} }

// KnownColumnAlternatives declares a closed set of ordered procedure headers.
// A value-dependent procedure can produce one declared shape, never an observed
// header that was absent from this contract. Single-header consumers remain unresolved.
func KnownColumnAlternatives(headers [][]string) Columns {
	if len(headers) == 1 {
		return KnownColumns(headers[0])
	}
	alternatives := make([][]string, len(headers))
	for i, header := range headers {
		alternatives[i] = append([]string{}, header...)
	}
	return Columns{alternatives: alternatives}
}

// UnknownColumns has no authoritative result schema.
func UnknownColumns() Columns { return Columns{} }

// Visit exposes a fresh ordered header or explicitly transfers unresolved handling.
func (c Columns) Visit(known func([]string), unknown func()) { c.names.Visit(known, unknown) }

// Verify checks actual stream fields against the established ordered schema.
// Unknown columns are terminal; observed fields never complete missing facts.
func (c Columns) Verify(actual []string) error {
	if len(c.alternatives) > 0 {
		for _, header := range c.alternatives {
			if slices.Equal(header, actual) {
				return nil
			}
		}
		return localization.NewLocalizedError("Neo.DatabaseError.Statement.ExecutionFailed", localization.CypherResultMetadataMismatch(), nil)
	}
	if !c.names.known {
		return unresolvedError()
	}
	if !slices.Equal(c.names.values, actual) {
		return localization.NewLocalizedError("Neo.DatabaseError.Statement.ExecutionFailed", localization.CypherResultMetadataMismatch(), nil)
	}
	return nil
}

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

// BindingHorizon is one frozen semantic transition view. Current is wildcard
// membership; Readable also includes carried inputs. Weak may be shadowed by a
// declaration. Dependencies identifies entry values actually read, never rows.
type BindingHorizon struct {
	Readable, Current, Retained, Weak, Dependencies Scope
}

// StreamMode declares whether an operator consumes a complete input stream or
// invokes a nested query per input row. Empty input retains its declared schema.
type StreamMode uint8

const (
	CompleteStream StreamMode = iota + 1
	PerInputRow
)

// Clause carries execution's scope transfer and original-query byte span.
type Clause struct {
	Kind                ClauseKind
	Start, End          int
	Before, After       Scope
	Entry, Exit         *BindingHorizon
	Projection          *Projection
	Call                *Call
	ParticipantResolved bool
	// Routing is the enclosing horizon visible to a USE selector. It is not
	// the participant body's Before/After scope.
	Routing Scope
	Graph   *GraphSelection
}

// GraphSelection retains the admitted USE selector from the compiler's CST.
// Fabric owns its evaluation, participant lookup and access checks.
type GraphSelection struct {
	Name, Function string
	Arguments      []string
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
	// Routing is the canonical set of enclosing inputs needed by this CALL's
	// USE selectors, separate from participant-body imports.
	Routing Scope
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
	Routing          Scope
	Selection        GraphSelection
}

// Statement is a complete execution scope program, including nested statements.
// Every span addresses the original query, not a rewritten or USE-free copy.
type Statement struct {
	// Command is an admitted non-graph command's typed continuation. Its
	// participant owns its result; it has no query binding horizons.
	Command                   *Command
	Start, End                int
	Incoming, Retained, After Scope
	Clauses                   []Clause
	Branches                  []Statement
	UnionAll                  []bool
	Columns                   Columns
	// Routing preserves the selected USE horizon in a fragment view after
	// that routing clause has been consumed. Original programs keep it on USE.
	Routing Scope
	// Fragments is the compiler's lowering program. Query steps carry complete
	// horizons; USE and CALL steps refer to their canonical clause by index.
	Fragments   []BoundFragment
	Entry, Exit *BindingHorizon
}

// Command preserves the existing command-owner boundary when USE routes DDL or
// SHOW to a participant. It never stands in for unresolved graph-query facts.
type Command struct {
	Text  string
	Graph *GraphSelection
}

// BoundFragment is one compiler-owned fragment boundary. Fabric may serialize
// its source clauses and choose participants, but never reconstruct its scope,
// result schema, retained-value carriage or continuation multiplicity.
type BoundFragment struct {
	Kind                      ClauseKind
	First, End                int
	Incoming, Retained, After Scope
	Columns                   Columns
	// ParentInputs carry unchanged retained declarations from the CALL entry.
	// RowInputs carry current stream values, including rebound retained names.
	ParentInputs, RowInputs []string
	WholeStream             bool
	Multiplicity            StreamMode
	Entry, Exit             *BindingHorizon
	// Returning identifies an existing terminal RETURN to serialize as WITH
	// before adding hygienic transport exports. Prefixes have no RETURN.
	Returning bool
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
	StatementFacts(query string, incoming Scope, cypher25 bool, stage cyphertext.SourceStage) Facts
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

func completeHorizon(horizon *BindingHorizon) bool {
	return horizon != nil && horizon.Readable.names.known && horizon.Current.names.known && horizon.Retained.names.known && horizon.Weak.names.known && horizon.Dependencies.names.known
}

func completeStatement(statement *Statement, visiting map[*Statement]bool) bool {
	if statement.Command != nil {
		return statement.Command.Text != "" && statement.Entry == nil && statement.Exit == nil && len(statement.Clauses) == 0 && len(statement.Branches) == 0 && len(statement.Fragments) == 0 && (statement.Columns.names.known || len(statement.Columns.alternatives) > 0)
	}
	if !completeHorizon(statement.Entry) || !completeHorizon(statement.Exit) {
		return false
	}
	if visiting[statement] || !statement.Incoming.names.known || !statement.Retained.names.known || !statement.After.names.known || !(statement.Columns.names.known || len(statement.Columns.alternatives) > 0) || len(statement.UnionAll) != max(0, len(statement.Branches)-1) {
		return false
	}
	visiting[statement] = true
	defer delete(visiting, statement)
	for _, fragment := range statement.Fragments {
		if !completeHorizon(fragment.Entry) || !completeHorizon(fragment.Exit) {
			return false
		}
		if fragment.First < 0 || fragment.End <= fragment.First || fragment.End > len(statement.Clauses) {
			return false
		}
		if fragment.Kind == ClauseOther && (!fragment.Incoming.names.known || !fragment.Retained.names.known || !fragment.After.names.known || !(fragment.Columns.names.known || len(fragment.Columns.alternatives) > 0)) {
			return false
		}
	}
	for _, clause := range statement.Clauses {
		if !completeHorizon(clause.Entry) || !completeHorizon(clause.Exit) {
			return false
		}
		if !clause.Before.names.known || !clause.After.names.known || (clause.Projection != nil && !clause.Projection.Names.known) {
			return false
		}
		if call := clause.Call; call != nil {
			if !call.Retained.names.known || !(call.Body.names.known || len(call.Body.alternatives) > 0) || !call.After.names.known || (call.Kind != CallUnit && call.Kind != CallReturning) || call.Statement == nil || !completeStatement(call.Statement, visiting) {
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

func cloneHorizon(horizon *BindingHorizon) *BindingHorizon {
	if horizon == nil {
		return nil
	}
	copy := *horizon
	return &copy
}
func cloneStatement(statement Statement) Statement {
	statement.Entry, statement.Exit = cloneHorizon(statement.Entry), cloneHorizon(statement.Exit)
	if statement.Command != nil {
		command := *statement.Command
		if command.Graph != nil {
			graph := *command.Graph
			graph.Arguments = append([]string(nil), graph.Arguments...)
			command.Graph = &graph
		}
		statement.Command = &command
	}
	statement.Fragments = append([]BoundFragment(nil), statement.Fragments...)
	for index := range statement.Fragments {
		fragment := &statement.Fragments[index]
		fragment.Entry, fragment.Exit = cloneHorizon(fragment.Entry), cloneHorizon(fragment.Exit)
		fragment.ParentInputs = append([]string(nil), fragment.ParentInputs...)
		fragment.RowInputs = append([]string(nil), fragment.RowInputs...)
	}
	statement.Clauses = append([]Clause(nil), statement.Clauses...)
	for index := range statement.Clauses {
		clause := &statement.Clauses[index]
		clause.Entry, clause.Exit = cloneHorizon(clause.Entry), cloneHorizon(clause.Exit)
		if clause.Graph != nil {
			graph := *clause.Graph
			graph.Arguments = append([]string(nil), graph.Arguments...)
			clause.Graph = &graph
		}
		if clause.Projection != nil {
			projection := *clause.Projection
			projection.Expressions = append([]string(nil), projection.Expressions...)
			projection.Retention = append([]RetainedProjection(nil), projection.Retention...)
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

// SourceView projects retained byte spans to the caller's original source.
// Accepted products stay immutable and share their identity view unchanged.
func (f Facts) SourceView(source *cyphertext.Source) Facts {
	if source == nil || source.Text == source.Original {
		return f
	}
	valid := true
	span := func(start, end int) (int, int) {
		if start < 0 || end < 0 {
			return start, end
		}
		mapped, ok := source.OriginalSpan(cyphertext.Span{Start: start, End: end}, cyphertext.SourceExtent)
		if !ok {
			valid = false
			return 0, 0
		}
		return mapped.Start, mapped.End
	}
	if f.statement != nil {
		statement := cloneStatement(*f.statement)
		var project func(*Statement)
		project = func(s *Statement) {
			s.Start, s.End = span(s.Start, s.End)
			for i := range s.Clauses {
				c := &s.Clauses[i]
				c.Start, c.End = span(c.Start, c.End)
				if c.Call != nil && c.Call.Statement != nil {
					project(c.Call.Statement)
				}
			}
			for i := range s.Branches {
				project(&s.Branches[i])
			}
		}
		project(&statement)
		f.statement = &statement
	} else if f.unresolved.Position >= 0 {
		if position, ok := source.OriginalOffset(f.unresolved.Position, true, cyphertext.SourceExtent); ok {
			f.unresolved.Position = position
		}
	}
	if !valid {
		return UnresolvedFacts(Unresolved{Reason: ReasonParserDecline, Position: -1, Err: unresolvedError()})
	}
	return f
}
