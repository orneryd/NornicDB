package fabric

import (
	"errors"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
)

// FabricPlanner decomposes a Cypher query into a Fragment tree by splitting
// at USE-clause boundaries. Without USE clauses, it produces a single
// FragmentExec targeting the session database — identical to community behavior.
//
// This mirrors Neo4j's FabricPlanner.scala. The planner reads no Cypher: the
// statement owner's facts (resultmetadata.FactsProvider) give every UNION
// branch, clause, USE clause and CALL subquery with its span in the query,
// and every scope and column the fragments carry.
type FabricPlanner struct {
	catalog *Catalog
}

// ErrMixedUnion is returned for a query that combines UNION and UNION ALL at
// one level. Cypher rejects it before any branch is planned or runs.
var ErrMixedUnion = errors.New("UNION and UNION ALL cannot be combined in the same query")

// errFactsDeclined is a plan without the owner's facts: the owner answers a
// statement with facts or its error, so an adapter's decline never plans.
var errFactsDeclined = errors.New("fabric: the statement owner did not describe the statement")

// errFactsIncomplete is a scope the owner left unknown in known facts, which
// KnownFacts rules out.
var errFactsIncomplete = errors.New("fabric: the statement owner's facts are incomplete")

// NewFabricPlanner creates a planner backed by the given catalog.
func NewFabricPlanner(catalog *Catalog) *FabricPlanner {
	return &FabricPlanner{catalog: catalog}
}

// Plan decomposes query into a Fragment tree by the statement owner's facts
// for query. An unresolved statement fails the plan with the owner's error,
// before any participant is opened.
//
// Parameters:
//   - query: the full Cypher query string the facts describe
//   - sessionDB: the default database for the session (used when no USE clause is present)
//   - facts: the owner's facts for query
//
// Returns a Fragment tree ready for execution by FabricExecutor.
func (p *FabricPlanner) Plan(query string, sessionDB string, facts resultmetadata.Facts) (Fragment, error) {
	if strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("empty query")
	}
	var statement resultmetadata.Statement
	var err error
	facts.Visit(func(known resultmetadata.Statement) { statement = known }, func() { err = errFactsDeclined }, func(unresolved resultmetadata.Unresolved) { err = unresolved.Err })
	if err != nil {
		return nil, err
	}
	plan := statementPlan{planner: p, source: query}
	return plan.statement(statement, planTarget{name: sessionDB}, compositeScopeRoot(sessionDB), nil)
}

// statementPlan plans the statements of one query, reading each clause at
// its span in source.
type statementPlan struct {
	planner *FabricPlanner
	source  string
}

// statement plans statement, running on current unless a USE clause selects
// another graph; call is the CALL subquery statement is the body of, nil at
// the query's top level. UNION branches each select their own graph.
func (s statementPlan) statement(statement resultmetadata.Statement, current planTarget, scope string, call *resultmetadata.Call) (Fragment, error) {
	if len(statement.Branches) == 0 {
		return s.branch(statement, current, scope, call)
	}
	var root Fragment
	for i, branch := range statement.Branches {
		planned, err := s.statement(branch, current, scope, call)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			root = planned
			continue
		}
		root = &FragmentUnion{
			Init:     &FragmentInit{Columns: nil},
			LHS:      root,
			RHS:      planned,
			Distinct: !statement.UnionAll[i-1],
			Columns:  factColumnNames(statement.Columns),
		}
	}
	return root, nil
}

// branch plans a statement without UNION. Its clauses run as one fragment
// on the graph they run on; a USE clause later in the statement, and a CALL
// subquery whose body selects a graph, split it: the clauses before run
// first and return their scope, which the rest runs for.
func (s statementPlan) branch(statement resultmetadata.Statement, current planTarget, scope string, call *resultmetadata.Call) (Fragment, error) {
	clauses := statement.Clauses
	target := current
	// A leading USE selects the graph the statement runs on: the statement's
	// first clause, or a CALL body's after its importing WITH.
	lead := 0
	if call != nil && call.Mode == resultmetadata.ImportNone && len(clauses) > 1 && clauses[0].Kind == resultmetadata.ClauseWith && clauses[1].Kind == resultmetadata.ClauseUse {
		lead = 1
	}
	if len(clauses) > lead && clauses[lead].Kind == resultmetadata.ClauseUse {
		selected, err := s.use(clauses[lead], call != nil, scope)
		if err != nil {
			return nil, err
		}
		target = selected
		if target.dynamic == nil {
			scope = compositeScopeRoot(target.name)
		}
		clauses = append(append([]resultmetadata.Clause(nil), clauses[:lead]...), clauses[lead+1:]...)
	}
	// Keep the original importing WITH. The owner's bound imports seed its
	// input record; preserving the clause also preserves lookup optimization.

	var input Fragment
	var run []resultmetadata.Clause
	// flush plans run, ending the clauses run on target before a split, or
	// the statement's last clauses when final.
	flush := func(final bool) error {
		if len(run) == 0 {
			return nil
		}
		exec, err := s.exec(run, target, scope, call, statement.Columns, final)
		run = nil
		if err != nil {
			return err
		}
		if input == nil {
			input = exec
			return nil
		}
		input = &FragmentApply{Input: input, Inner: exec, Columns: exec.Columns, Projection: true}
		return nil
	}
	for _, clause := range clauses {
		switch {
		case clause.Kind == resultmetadata.ClauseUse:
			if err := flush(false); err != nil {
				return nil, err
			}
			selected, err := s.use(clause, true, scope)
			if err != nil {
				return nil, err
			}
			target = selected
		case clause.Kind == resultmetadata.ClauseCallSubquery && clause.Call != nil && clause.Call.Statement != nil && selectsGraph(*clause.Call.Statement):
			if err := flush(false); err != nil {
				return nil, err
			}
			body, err := s.statement(*clause.Call.Statement, target, scope, clause.Call)
			if err != nil {
				return nil, err
			}
			columns, err := scopeColumns(clause.After)
			if err != nil {
				return nil, err
			}
			if input == nil {
				input = &FragmentInit{Columns: nil}
			}
			input = &FragmentApply{Input: input, Inner: body, Call: clause.Call, Columns: columns, Optional: clause.Call.Optional, Unit: clause.Call.Kind == resultmetadata.CallUnit}
		default:
			run = append(run, clause)
		}
	}
	if len(run) > 0 {
		if err := flush(true); err != nil {
			return nil, err
		}
		return input, nil
	}
	if input == nil {
		return nil, fmt.Errorf("empty query")
	}
	// The statement ends with a CALL whose body selects a graph: a unit CALL,
	// which returns the statement's own columns.
	if apply, ok := input.(*FragmentApply); ok {
		ended := *apply
		ended.Columns = factColumnNames(statement.Columns)
		return &ended, nil
	}
	return input, nil
}

// exec is the fragment running clauses on target. Clauses that end the
// statement return its columns; earlier clauses return their scope, under
// generated and quoted names, so the clauses after them run for its
// variables, or one marker row per row when no variable is in scope. In a
// body whose scope clause retains its imports, the retained imports are the
// record's throughout the body and are not returned: a CALL body can't
// return a variable it imports.
func (s statementPlan) exec(clauses []resultmetadata.Clause, target planTarget, scope string, call *resultmetadata.Call, columns resultmetadata.Columns, final bool) (*FragmentExec, error) {
	var text strings.Builder
	for i, clause := range clauses {
		if i > 0 {
			text.WriteByte(' ')
		}
		if importing, ok := serializeImportProjection(clause, call); ok {
			text.WriteString(importing)
		} else {
			text.WriteString(strings.TrimSpace(s.source[clause.Start:clause.End]))
		}
	}
	if !final {
		names, err := scopeNames(clauses[len(clauses)-1].After)
		if err != nil {
			return nil, err
		}
		if retainsImports(call) {
			retained, err := scopeNames(call.Retained)
			if err != nil {
				return nil, err
			}
			names = withoutNames(names, retained)
		}
		columns = resultmetadata.KnownColumns(names)
		if len(names) == 0 {
			text.WriteString(" RETURN 1 AS " + freeName("__fabric_row", text.String()))
		} else {
			quoted := make([]string, len(names))
			for i, name := range names {
				quoted[i] = quoteCypherIdentifier(name)
			}
			text.WriteString(" RETURN " + strings.Join(quoted, ", "))
		}
	}
	exec := &FragmentExec{
		Input:     &FragmentInit{Columns: nil},
		Query:     text.String(),
		GraphName: target.name,
		Call:      call,
		In:        clauses[0].Before,
		Columns:   factColumnNames(columns),
		IsWrite:   clausesWrite(clauses),
	}
	if target.dynamic != nil {
		exec.Graph = target.dynamic
		exec.Scope = scope
	}
	return exec, nil
}

// use is the graph the USE clause selects, checked against the composite
// scope when static. The USE grammar reads the clause with the statement
// after it, which a USE clause never ends.
func (s statementPlan) use(clause resultmetadata.Clause, inSubquery bool, scope string) (planTarget, error) {
	parsed, _, found, err := ParseUseClause(s.source[clause.Start:], inSubquery)
	if err != nil {
		return planTarget{}, err
	}
	if !found {
		return planTarget{}, fmt.Errorf("invalid USE clause")
	}
	target := useTarget(parsed)
	if err := s.planner.validatePlanTarget(scope, target); err != nil {
		return planTarget{}, err
	}
	return target, nil
}

// withoutNames is names without the ones in drop, in order.
func withoutNames(names, drop []string) []string {
	dropped := make(map[string]bool, len(drop))
	for _, name := range drop {
		dropped[name] = true
	}
	kept := make([]string, 0, len(names))
	for _, name := range names {
		if !dropped[name] {
			kept = append(kept, name)
		}
	}
	return kept
}

// selectsGraph reports whether statement, or a CALL body or UNION branch in
// it, has a USE clause.
func selectsGraph(statement resultmetadata.Statement) bool {
	for _, branch := range statement.Branches {
		if selectsGraph(branch) {
			return true
		}
	}
	for _, clause := range statement.Clauses {
		if clause.Kind == resultmetadata.ClauseUse {
			return true
		}
		if clause.Call != nil && clause.Call.Statement != nil && selectsGraph(*clause.Call.Statement) {
			return true
		}
	}
	return false
}

// clausesWrite reports whether clauses write, in a CALL body they run too.
func clausesWrite(clauses []resultmetadata.Clause) bool {
	for _, clause := range clauses {
		if clause.Kind == resultmetadata.ClauseWrite {
			return true
		}
		if clause.Call != nil && clause.Call.Statement != nil && statementWrites(*clause.Call.Statement) {
			return true
		}
	}
	return false
}

func statementWrites(statement resultmetadata.Statement) bool {
	for _, branch := range statement.Branches {
		if statementWrites(branch) {
			return true
		}
	}
	return clausesWrite(statement.Clauses)
}

// scopeNames is the variables of a scope known facts carry.
func scopeNames(scope resultmetadata.Scope) (names []string, err error) {
	scope.Visit(func(known []string) { names = known }, func() { err = errFactsIncomplete })
	return names, err
}

// scopeColumns is a scope's variables as the columns of the rows binding them.
func scopeColumns(scope resultmetadata.Scope) ([]string, error) {
	names, err := scopeNames(scope)
	if err != nil {
		return nil, err
	}
	return names, nil
}

// validatePlanTarget checks a static USE target at plan time; a dynamic
// reference is checked when it resolves (FabricExecutor.resolveExecGraph).
func (p *FabricPlanner) validatePlanTarget(scope string, target planTarget) error {
	if target.dynamic != nil {
		return nil
	}
	return p.validateUseTarget(scope, target.name)
}

func (p *FabricPlanner) validateUseTarget(sessionDB string, targetDB string) error {
	target := strings.TrimSpace(targetDB)
	if target == "" {
		return fmt.Errorf("USE clause requires a database name")
	}
	if p.catalog != nil {
		if _, err := p.catalog.Resolve(target); err != nil {
			return fmt.Errorf("invalid USE target '%s': %w", target, err)
		}
	}

	scopeRoot := compositeScopeRoot(sessionDB)
	targetRoot := compositeScopeRoot(target)
	if strings.Contains(target, ".") && p.inCompositeScope(sessionDB) && scopeRoot != "" && !strings.EqualFold(scopeRoot, targetRoot) {
		return fmt.Errorf("invalid USE target '%s': target is out of scope for composite '%s'", target, scopeRoot)
	}
	return nil
}

func (p *FabricPlanner) inCompositeScope(sessionDB string) bool {
	db := strings.TrimSpace(sessionDB)
	if db == "" {
		return false
	}
	if strings.Contains(db, ".") {
		return true
	}
	if p.catalog == nil {
		return false
	}
	prefix := strings.ToLower(db) + "."
	return p.catalog.HasGraphWithPrefix(prefix)
}

func compositeScopeRoot(graph string) string {
	graph = strings.TrimSpace(graph)
	if graph == "" {
		return ""
	}
	if idx := strings.IndexByte(graph, '.'); idx >= 0 {
		return graph[:idx]
	}
	return graph
}

// planTarget is the graph a planned fragment runs on: a static graph name,
// or a dynamic reference resolved each time the fragment runs.
type planTarget struct {
	name    string
	dynamic *UseClause
}

// useTarget is the plan target a USE clause selects.
func useTarget(clause UseClause) planTarget {
	if clause.IsDynamic() {
		c := clause
		return planTarget{dynamic: &c}
	}
	return planTarget{name: clause.Name}
}

// startsWithFold checks if s starts with prefix (case-insensitive).
func startsWithFold(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return strings.EqualFold(s[:len(prefix)], prefix)
}

func containsFold(s, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	if len(needle) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(needle); i++ {
		if strings.EqualFold(s[i:i+len(needle)], needle) {
			return true
		}
	}
	return false
}

func isWhitespace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

func isIdentChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9') || b == '_'
}

func queryIsWrite(query string) bool {
	for i := 0; i < len(query); i++ {
		if hasKeywordAt(query, i, "CREATE") ||
			hasKeywordAt(query, i, "MERGE") ||
			hasKeywordAt(query, i, "DETACH DELETE") ||
			hasKeywordAt(query, i, "DELETE") ||
			hasKeywordAt(query, i, "SET") ||
			hasKeywordAt(query, i, "REMOVE") {
			return true
		}
	}
	return false
}

// startsWithFold checks if s starts with prefix (case-insensitive).
