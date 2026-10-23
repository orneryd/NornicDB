package fabric

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/stretchr/testify/require"
)

// factsQuery builds a statement owner's facts for a query by hand: each
// clause is read at the next occurrence of its text, with the scope before
// and after it.
type factsQuery struct {
	query string
	at    int
}

func (q *factsQuery) clause(kind resultmetadata.ClauseKind, text string, before, after []string) resultmetadata.Clause {
	start := strings.Index(q.query[q.at:], text)
	if start < 0 {
		panic(fmt.Sprintf("%q not in %q after %d", text, q.query, q.at))
	}
	start += q.at
	q.at = start + len(text)
	return resultmetadata.Clause{Kind: kind, Start: start, End: start + len(text), Before: resultmetadata.KnownScope(before), After: resultmetadata.KnownScope(after)}
}

func knownFacts(t *testing.T, statement resultmetadata.Statement) resultmetadata.Facts {
	t.Helper()
	facts, err := resultmetadata.KnownFacts(statement)
	require.NoError(t, err)
	return facts
}

func statementOf(columns []string, clauses ...resultmetadata.Clause) resultmetadata.Statement {
	return resultmetadata.Statement{
		Incoming: resultmetadata.KnownScope(nil), Retained: resultmetadata.KnownScope(nil),
		After: clauses[len(clauses)-1].After, Clauses: clauses, Columns: resultmetadata.KnownColumns(columns),
	}
}

func columnsOf(t *testing.T, c []string) []string {
	t.Helper()
	names, err := c, error(nil)
	require.NoError(t, err)
	return names
}

// Without USE, the statement runs as it is on the session graph.
func TestPlanByFactsSingleGraph(t *testing.T) {
	q := &factsQuery{query: "MATCH (n) RETURN n.x AS x"}
	statement := statementOf([]string{"x"},
		q.clause(resultmetadata.ClauseMatch, "MATCH (n)", nil, []string{"n"}),
		q.clause(resultmetadata.ClauseReturn, "RETURN n.x AS x", []string{"n"}, []string{"x"}),
	)
	fragment, err := NewFabricPlanner(nil).Plan(q.query, "db", knownFacts(t, statement))
	require.NoError(t, err)
	exec := fragment.(*FragmentExec)
	require.Equal(t, "MATCH (n) RETURN n.x AS x", exec.Query)
	require.Equal(t, "db", exec.GraphName)
	require.Equal(t, []string{"x"}, columnsOf(t, exec.Columns))
	require.False(t, exec.IsWrite)
}

// A leading USE selects the statement's graph; the USE clause itself is not
// sent. A write clause marks the fragment a write.
func TestPlanByFactsLeadingUse(t *testing.T) {
	q := &factsQuery{query: "USE comp.a CREATE (n:N) RETURN n"}
	statement := statementOf([]string{"n"},
		q.clause(resultmetadata.ClauseUse, "USE comp.a", nil, nil),
		q.clause(resultmetadata.ClauseWrite, "CREATE (n:N)", nil, []string{"n"}),
		q.clause(resultmetadata.ClauseReturn, "RETURN n", []string{"n"}, []string{"n"}),
	)
	fragment, err := NewFabricPlanner(nil).Plan(q.query, "comp", knownFacts(t, statement))
	require.NoError(t, err)
	exec := fragment.(*FragmentExec)
	require.Equal(t, "CREATE (n:N) RETURN n", exec.Query)
	require.Equal(t, "comp.a", exec.GraphName)
	require.True(t, exec.IsWrite)
}

// The planner's statement facts give a CALL subquery whose body selects a
// graph: the clauses before it run first and return their scope under
// quoted names, the body runs for each of their rows with the CALL's facts
// (the original importing WITH is kept), and the clauses after it run for the
// combined rows.
func TestPlanByFactsCallSelectingGraph(t *testing.T) {
	q := &factsQuery{query: "USE comp WITH 1 AS `x y` CALL { WITH `x y` USE comp.r RETURN `x y` + 1 AS z } RETURN `x y`, z"}
	use := q.clause(resultmetadata.ClauseUse, "USE comp", nil, nil)
	with := q.clause(resultmetadata.ClauseWith, "WITH 1 AS `x y`", nil, []string{"x y"})
	callStart := strings.Index(q.query, "CALL {")
	importWith := q.clause(resultmetadata.ClauseWith, "WITH `x y`", []string{"x y"}, []string{"x y"})
	importWith.Projection = &resultmetadata.Projection{Expressions: []string{"`x y`"}, Names: resultmetadata.Known([]string{"x y"}), PassThrough: true}
	bodyUse := q.clause(resultmetadata.ClauseUse, "USE comp.r", []string{"x y"}, []string{"x y"})
	bodyReturn := q.clause(resultmetadata.ClauseReturn, "RETURN `x y` + 1 AS z", []string{"x y"}, []string{"z"})
	body := statementOf([]string{"z"}, importWith, bodyUse, bodyReturn)
	body.Incoming = resultmetadata.KnownScope([]string{"x y"})
	call := &resultmetadata.Call{Mode: resultmetadata.ImportNone, Imports: []string{"x y"}, Retained: resultmetadata.KnownScope(nil),
		Body: resultmetadata.KnownColumns([]string{"z"}), Kind: resultmetadata.CallReturning, After: resultmetadata.KnownScope([]string{"x y", "z"}), Statement: &body}
	callClause := resultmetadata.Clause{Kind: resultmetadata.ClauseCallSubquery, Start: callStart, End: q.at + 2, Before: resultmetadata.KnownScope([]string{"x y"}), After: resultmetadata.KnownScope([]string{"x y", "z"}), Call: call}
	q.at += 2
	final := q.clause(resultmetadata.ClauseReturn, "RETURN `x y`, z", []string{"x y", "z"}, []string{"x y", "z"})
	statement := statementOf([]string{"x y", "z"}, use, with, callClause, final)

	fragment, err := NewFabricPlanner(nil).Plan(q.query, "comp", knownFacts(t, statement))
	require.NoError(t, err)
	continuation := fragment.(*FragmentApply)
	require.Nil(t, continuation.Call)
	tail := continuation.Inner.(*FragmentExec)
	require.Equal(t, "RETURN `x y`, z", tail.Query)
	require.Equal(t, []string{"x y", "z"}, columnsOf(t, tail.Columns))
	apply := continuation.Input.(*FragmentApply)
	require.Equal(t, call, apply.Call)
	require.Equal(t, []string{"x y", "z"}, columnsOf(t, apply.Columns))
	prefix := apply.Input.(*FragmentExec)
	require.Equal(t, "WITH 1 AS `x y` RETURN `x y`", prefix.Query)
	require.Equal(t, "comp", prefix.GraphName)
	inner := apply.Inner.(*FragmentExec)
	require.Equal(t, "WITH `x y` RETURN `x y` + 1 AS z", inner.Query, "the owner binds the original importing WITH")
	require.Equal(t, "comp.r", inner.GraphName)
	require.Equal(t, call, inner.Call)
}

// Clauses before a split with no variable in scope return one marker row per
// row, which the next fragment drops.
func TestPlanByFactsEmptyScopeMarker(t *testing.T) {
	q := &factsQuery{query: "CREATE (:A) USE comp.b MATCH (m) RETURN m"}
	statement := statementOf([]string{"m"},
		q.clause(resultmetadata.ClauseWrite, "CREATE (:A)", nil, nil),
		q.clause(resultmetadata.ClauseUse, "USE comp.b", nil, nil),
		q.clause(resultmetadata.ClauseMatch, "MATCH (m)", nil, []string{"m"}),
		q.clause(resultmetadata.ClauseReturn, "RETURN m", []string{"m"}, []string{"m"}),
	)
	fragment, err := NewFabricPlanner(nil).Plan(q.query, "comp.a", knownFacts(t, statement))
	require.NoError(t, err)
	apply := fragment.(*FragmentApply)
	prefix := apply.Input.(*FragmentExec)
	require.Equal(t, "CREATE (:A) RETURN 1 AS __fabric_row", prefix.Query)
	require.Empty(t, columnsOf(t, prefix.Columns))
	require.True(t, prefix.IsWrite)
	require.Equal(t, "comp.b", apply.Inner.(*FragmentExec).GraphName)
}

// UNION branches plan on their own graphs and combine with the union's
// columns; UNION ALL keeps duplicates.
func TestPlanByFactsUnion(t *testing.T) {
	q := &factsQuery{query: "USE comp.a RETURN 1 AS v UNION ALL USE comp.b RETURN 2 AS v"}
	left := statementOf([]string{"v"}, q.clause(resultmetadata.ClauseUse, "USE comp.a", nil, nil), q.clause(resultmetadata.ClauseReturn, "RETURN 1 AS v", nil, []string{"v"}))
	right := statementOf([]string{"v"}, q.clause(resultmetadata.ClauseUse, "USE comp.b", nil, nil), q.clause(resultmetadata.ClauseReturn, "RETURN 2 AS v", nil, []string{"v"}))
	statement := resultmetadata.Statement{Incoming: resultmetadata.KnownScope(nil), Retained: resultmetadata.KnownScope(nil), After: resultmetadata.KnownScope([]string{"v"}),
		Branches: []resultmetadata.Statement{left, right}, UnionAll: []bool{true}, Columns: resultmetadata.KnownColumns([]string{"v"})}
	fragment, err := NewFabricPlanner(nil).Plan(q.query, "comp", knownFacts(t, statement))
	require.NoError(t, err)
	union := fragment.(*FragmentUnion)
	require.False(t, union.Distinct)
	require.Equal(t, "comp.a", union.LHS.(*FragmentExec).GraphName)
	require.Equal(t, "comp.b", union.RHS.(*FragmentExec).GraphName)
	require.Equal(t, []string{"v"}, columnsOf(t, union.Columns))
}

// A statement the owner can't describe fails the plan with the owner's
// error; neither a decline nor the zero facts ever plans.
func TestPlanByFactsUnresolved(t *testing.T) {
	ownerErr := errors.New("Variable `y` not defined")
	_, err := NewFabricPlanner(nil).Plan("CALL (y) { RETURN 1 AS z } RETURN z", "db", resultmetadata.UnresolvedFacts(resultmetadata.Unresolved{Reason: resultmetadata.ReasonUndefinedVariable, Err: ownerErr}))
	require.ErrorIs(t, err, ownerErr)
	_, err = NewFabricPlanner(nil).Plan("RETURN 1", "db", resultmetadata.NotApplicableFacts())
	require.ErrorIs(t, err, errFactsDeclined)
	_, err = NewFabricPlanner(nil).Plan("RETURN 1", "db", resultmetadata.Facts{})
	require.Error(t, err)
}

func TestPlanByFactsNestedRetainedImport(t *testing.T) {
	q := &factsQuery{query: "USE comp WITH 7 AS `use` CALL (`use`) { WITH DISTINCT 1 AS ignored CALL () { USE comp.one RETURN 2 AS marker } RETURN `use` AS k } RETURN k"}
	use := q.clause(resultmetadata.ClauseUse, "USE comp", nil, nil)
	with := q.clause(resultmetadata.ClauseWith, "WITH 7 AS `use`", nil, []string{"use"})
	outerStart := strings.Index(q.query, "CALL (`use`)")
	prefix := q.clause(resultmetadata.ClauseWith, "WITH DISTINCT 1 AS ignored", []string{"use"}, []string{"ignored", "use"})
	innerStart := strings.Index(q.query, "CALL ()")
	inner := statementOf([]string{"marker"},
		q.clause(resultmetadata.ClauseUse, "USE comp.one", nil, nil),
		q.clause(resultmetadata.ClauseReturn, "RETURN 2 AS marker", nil, []string{"marker"}))
	innerCall := &resultmetadata.Call{Mode: resultmetadata.ImportListed, Retained: resultmetadata.KnownScope(nil),
		Body: resultmetadata.KnownColumns([]string{"marker"}), Kind: resultmetadata.CallReturning,
		After: resultmetadata.KnownScope([]string{"ignored", "marker", "use"}), Statement: &inner}
	innerClause := resultmetadata.Clause{Kind: resultmetadata.ClauseCallSubquery, Start: innerStart, End: q.at + 2,
		Before: prefix.After, After: innerCall.After, Call: innerCall}
	q.at += 2
	returned := q.clause(resultmetadata.ClauseReturn, "RETURN `use` AS k", []string{"ignored", "marker", "use"}, []string{"k", "use"})
	body := statementOf([]string{"k"}, prefix, innerClause, returned)
	body.Incoming, body.Retained = resultmetadata.KnownScope([]string{"use"}), resultmetadata.KnownScope([]string{"use"})
	outerCall := &resultmetadata.Call{Mode: resultmetadata.ImportListed, Imports: []string{"use"}, Retained: body.Retained,
		Body: body.Columns, Kind: resultmetadata.CallReturning, After: resultmetadata.KnownScope([]string{"k", "use"}), Statement: &body}
	outerClause := resultmetadata.Clause{Kind: resultmetadata.ClauseCallSubquery, Start: outerStart, End: q.at + 2,
		Before: with.After, After: outerCall.After, Call: outerCall}
	q.at += 2
	statement := statementOf([]string{"k"}, use, with, outerClause, q.clause(resultmetadata.ClauseReturn, "RETURN k", []string{"k", "use"}, []string{"k"}))
	fragment, err := NewFabricPlanner(nil).Plan(q.query, "comp", knownFacts(t, statement))
	require.NoError(t, err)
	outerApply := fragment.(*FragmentApply).Input.(*FragmentApply)
	bodyTail := outerApply.Inner.(*FragmentApply)
	innerApply := bodyTail.Input.(*FragmentApply)
	require.Empty(t, innerApply.Call.Imports, "CALL () cannot inherit its parent's imports")
	beforeNested := innerApply.Input.(*FragmentExec)
	require.Equal(t, "WITH DISTINCT 1 AS ignored RETURN `ignored`", beforeNested.Query)
	require.Equal(t, []string{"ignored"}, beforeNested.Columns, "retained imports stay in the record, not the body's exports")
	require.Equal(t, []string{"ignored", "marker", "use"}, innerApply.Columns, "the join still retains the scoped import")
	require.Equal(t, "comp.one", innerApply.Inner.(*FragmentExec).GraphName)
	require.Equal(t, "RETURN `use` AS k", bodyTail.Inner.(*FragmentExec).Query)
	require.Equal(t, []string{"use"}, bodyTail.Inner.(*FragmentExec).Call.Imports)
}

func TestPlanByFactsUnitCallAtEnd(t *testing.T) {
	q := &factsQuery{query: "USE comp CALL () { USE comp.one CREATE (:KeywordNameSeed {v:7}) }"}
	use := q.clause(resultmetadata.ClauseUse, "USE comp", nil, nil)
	start := strings.Index(q.query, "CALL")
	body := statementOf(nil,
		q.clause(resultmetadata.ClauseUse, "USE comp.one", nil, nil),
		q.clause(resultmetadata.ClauseWrite, "CREATE (:KeywordNameSeed {v:7})", nil, nil))
	call := &resultmetadata.Call{Mode: resultmetadata.ImportListed, Retained: resultmetadata.KnownScope(nil),
		Body: resultmetadata.KnownColumns(nil), Kind: resultmetadata.CallUnit, After: resultmetadata.KnownScope(nil), Statement: &body}
	clause := resultmetadata.Clause{Kind: resultmetadata.ClauseCallSubquery, Start: start, End: len(q.query),
		Before: resultmetadata.KnownScope(nil), After: call.After, Call: call}
	fragment, err := NewFabricPlanner(nil).Plan(q.query, "comp", knownFacts(t, statementOf(nil, use, clause)))
	require.NoError(t, err)
	apply := fragment.(*FragmentApply)
	require.True(t, apply.Unit)
	require.Empty(t, apply.Columns)
	require.IsType(t, &FragmentInit{}, apply.Input)
	require.Equal(t, "CREATE (:KeywordNameSeed {v:7})", apply.Inner.(*FragmentExec).Query)
	require.True(t, apply.Inner.(*FragmentExec).IsWrite)
}

func TestPlanByFactsLocalNestedUnionWrites(t *testing.T) {
	q := &factsQuery{query: "USE comp.one CALL () { RETURN 1 AS v UNION ALL CALL () { CREATE (:N) } RETURN 2 AS v } RETURN v"}
	use := q.clause(resultmetadata.ClauseUse, "USE comp.one", nil, nil)
	start := strings.Index(q.query, "CALL")
	left := statementOf([]string{"v"}, q.clause(resultmetadata.ClauseReturn, "RETURN 1 AS v", nil, []string{"v"}))
	innerStart := strings.Index(q.query[start+1:], "CALL") + start + 1
	write := statementOf(nil, q.clause(resultmetadata.ClauseWrite, "CREATE (:N)", nil, nil))
	unit := &resultmetadata.Call{Mode: resultmetadata.ImportListed, Retained: resultmetadata.KnownScope(nil), Body: resultmetadata.KnownColumns(nil),
		Kind: resultmetadata.CallUnit, After: resultmetadata.KnownScope(nil), Statement: &write}
	unitClause := resultmetadata.Clause{Kind: resultmetadata.ClauseCallSubquery, Start: innerStart, End: q.at + 2,
		Before: resultmetadata.KnownScope(nil), After: unit.After, Call: unit}
	q.at += 2
	right := statementOf([]string{"v"}, unitClause, q.clause(resultmetadata.ClauseReturn, "RETURN 2 AS v", nil, []string{"v"}))
	body := resultmetadata.Statement{Incoming: resultmetadata.KnownScope(nil), Retained: resultmetadata.KnownScope(nil), After: resultmetadata.KnownScope([]string{"v"}),
		Columns: resultmetadata.KnownColumns([]string{"v"}), Branches: []resultmetadata.Statement{left, right}, UnionAll: []bool{true}}
	call := &resultmetadata.Call{Mode: resultmetadata.ImportListed, Retained: resultmetadata.KnownScope(nil), Body: body.Columns,
		Kind: resultmetadata.CallReturning, After: body.After, Statement: &body}
	clause := resultmetadata.Clause{Kind: resultmetadata.ClauseCallSubquery, Start: start, End: q.at + 2,
		Before: resultmetadata.KnownScope(nil), After: call.After, Call: call}
	q.at += 2
	statement := statementOf([]string{"v"}, use, clause, q.clause(resultmetadata.ClauseReturn, "RETURN v", []string{"v"}, []string{"v"}))
	fragment, err := NewFabricPlanner(nil).Plan(q.query, "comp", knownFacts(t, statement))
	require.NoError(t, err)
	exec := fragment.(*FragmentExec)
	require.True(t, exec.IsWrite, "nested writes must select the write transaction path even without a Fabric split")
	require.Equal(t, strings.TrimPrefix(q.query, "USE comp.one "), exec.Query)
}

func TestPlanByFactsUnknownConstituentFailsBeforeExecution(t *testing.T) {
	q := &factsQuery{query: "USE comp CALL () { USE comp.missing RETURN 1 AS v } RETURN v"}
	use := q.clause(resultmetadata.ClauseUse, "USE comp", nil, nil)
	start := strings.Index(q.query, "CALL")
	body := statementOf([]string{"v"}, q.clause(resultmetadata.ClauseUse, "USE comp.missing", nil, nil), q.clause(resultmetadata.ClauseReturn, "RETURN 1 AS v", nil, []string{"v"}))
	call := &resultmetadata.Call{Mode: resultmetadata.ImportListed, Retained: resultmetadata.KnownScope(nil), Body: body.Columns,
		Kind: resultmetadata.CallReturning, After: body.After, Statement: &body}
	clause := resultmetadata.Clause{Kind: resultmetadata.ClauseCallSubquery, Start: start, End: q.at + 2,
		Before: resultmetadata.KnownScope(nil), After: call.After, Call: call}
	q.at += 2
	statement := statementOf([]string{"v"}, use, clause, q.clause(resultmetadata.ClauseReturn, "RETURN v", []string{"v"}, []string{"v"}))
	catalog := NewCatalog()
	catalog.Register("comp", &LocationLocal{DBName: "comp"})
	_, err := NewFabricPlanner(catalog).Plan(q.query, "comp", knownFacts(t, statement))
	require.ErrorContains(t, err, "comp.missing")
}
