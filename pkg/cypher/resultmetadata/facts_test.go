package resultmetadata

import (
	"errors"
	"reflect"
	"testing"
)

func TestFactsOutcomesAndCopies(t *testing.T) {
	for _, facts := range []Facts{{}, UnresolvedFacts(Unresolved{Reason: ReasonParserDecline})} {
		facts.Visit(func(Statement) { t.Fatal("unresolved became known") }, func() { t.Fatal("owner failure became a decline") }, func(reason Unresolved) {
			if reason.Err == nil {
				t.Fatal("terminal unresolved has no error")
			}
		})
	}
	NotApplicableFacts().Visit(func(Statement) { t.Fatal("decline became known") }, func() {}, func(Unresolved) { t.Fatal("decline became terminal") })
	sentinel := errors.New("sentinel")
	UnresolvedFacts(Unresolved{Reason: ReasonInvalidImport, Position: 12, Err: sentinel}).Visit(func(Statement) { t.Fatal("error became known") }, func() { t.Fatal("error declined") }, func(reason Unresolved) {
		if reason.Err != sentinel || reason.Position != 12 || reason.Reason != ReasonInvalidImport {
			t.Fatal("lost unresolved identity")
		}
	})
	scope := KnownScope([]string{"z", "a", "z"})
	scope.Visit(func(names []string) {
		if !reflect.DeepEqual(names, []string{"a", "z"}) {
			t.Fatal(names)
		}
		names[0] = "mutated"
	}, func() { t.Fatal("known scope unknown") })
	statement := Statement{Incoming: scope, Retained: KnownScope(nil), After: scope, Columns: KnownColumns([]string{"z", "a"})}
	statement.Clauses = []Clause{{Before: scope, After: scope, Projection: &Projection{Expressions: []string{"z", "a"}, Names: Known([]string{"z", "a"})}}}
	facts, err := KnownFacts(statement)
	if err != nil {
		t.Fatal(err)
	}
	statement.Clauses[0].Projection.Expressions[0] = "outside mutation"
	for round := 0; round < 2; round++ {
		facts.Visit(func(copy Statement) {
			if copy.Clauses[0].Projection.Expressions[0] != "z" {
				t.Fatal("mutable cached program")
			}
			copy.Clauses[0].Projection.Expressions[0] = "callback mutation"
			copy.Columns.Visit(func(names []string) {
				if !reflect.DeepEqual(names, []string{"z", "a"}) {
					t.Fatal("scope sorting changed columns", names)
				}
			}, func() { t.Fatal("known columns unknown") })
		}, func() { t.Fatal("known program declined") }, func(reason Unresolved) { t.Fatal(reason.Err) })
	}
	if _, err := KnownFacts(Statement{}); err == nil {
		t.Fatal("unknown components became known")
	}
	statement.After = UnknownScope()
	if _, err := KnownFacts(statement); err == nil {
		t.Fatal("unknown outgoing scope became known")
	}
	UnknownScope().Visit(func([]string) { t.Fatal("unknown scope exposed names") }, func() {})
	UnknownColumns().Visit(func([]string) { t.Fatal("unknown columns exposed names") }, func() {})
}

func TestFactsNestedProgramsRemainImmutable(t *testing.T) {
	empty := KnownScope(nil)
	leaf := Statement{Incoming: empty, Retained: empty, After: KnownScope([]string{"value"}), Columns: KnownColumns([]string{"value"})}
	leaf.Clauses = []Clause{{Kind: ClauseReturn, Before: empty, After: leaf.After, Projection: &Projection{Expressions: []string{"1"}, Names: Known([]string{"value"})}}}
	body := leaf
	body.Clauses = nil
	body.Branches = []Statement{leaf, leaf}
	body.UnionAll = []bool{true}
	parent := Statement{Incoming: empty, Retained: empty, After: leaf.After, Columns: leaf.Columns}
	parent.Clauses = []Clause{{Kind: ClauseCallSubquery, Before: empty, After: leaf.After, Call: &Call{Mode: ImportListed, Imports: []string{}, Retained: empty, Body: leaf.Columns, Kind: CallReturning, After: leaf.After, Statement: &body}}}
	facts, err := KnownFacts(parent)
	if err != nil {
		t.Fatal(err)
	}
	body.UnionAll[0] = false
	body.Branches[0].Clauses[0].Projection.Expressions[0] = "mutated"
	for round := 0; round < 2; round++ {
		facts.Visit(func(copy Statement) {
			call := copy.Clauses[0].Call
			if call.Mode != ImportListed || call.Imports == nil || len(call.Imports) != 0 {
				t.Fatal("CALL () lost its distinct import mode", call)
			}
			if !call.Statement.UnionAll[0] || call.Statement.Branches[0].Clauses[0].Projection.Expressions[0] != "1" {
				t.Fatal("nested program shares mutable storage")
			}
			call.Imports = append(call.Imports, "changed")
			call.Statement.UnionAll[0] = false
			call.Statement.Branches[0].Clauses[0].Projection.Expressions[0] = "changed"
		}, func() { t.Fatal("complete program declined") }, func(reason Unresolved) { t.Fatal(reason.Err) })
	}
	parent.Clauses[0].Call.Body = UnknownColumns()
	if _, err := KnownFacts(parent); err == nil {
		t.Fatal("unknown body schema became complete")
	}
	parent.Clauses[0].Call.Body = leaf.Columns
	parent.Clauses[0].Call.Statement = &parent
	if _, err := KnownFacts(parent); err == nil {
		t.Fatal("cyclic program became complete")
	}
}
func TestColumnsVerifyKnownAndUnknown(t *testing.T) {
	for _, sample := range []struct {
		columns Columns
		actual  []string
		valid   bool
	}{
		{KnownColumns([]string{"a", "b"}), []string{"a", "b"}, true},
		{KnownColumns(nil), nil, true},
		{KnownColumns([]string{"a", "b"}), []string{"b", "a"}, false},
		{UnknownColumns(), nil, false},
	} {
		if err := sample.columns.Verify(sample.actual); (err == nil) != sample.valid {
			t.Errorf("columns valid=%v: %v; observed fields cannot complete an unknown schema", sample.valid, err)
		}
	}
}

func TestClosedProcedureColumnAlternatives(t *testing.T) {
	headers := [][]string{{"node", "score"}, {"page"}}
	columns := KnownColumnAlternatives(headers)
	headers[0][0] = "changed"
	for _, valid := range [][]string{{"node", "score"}, {"page"}} {
		if err := columns.Verify(valid); err != nil {
			t.Fatal(err)
		}
	}
	for _, invalid := range [][]string{nil, {"node"}, {"score", "node"}, {"node", "score", "page"}, {"changed", "score"}} {
		if err := columns.Verify(invalid); err == nil {
			t.Fatalf("undeclared header admitted: %v", invalid)
		}
	}
	columns.Visit(func([]string) { t.Fatal("conditional columns reported one fixed header") }, func() {})
	scope := KnownScope(nil)
	facts, err := KnownFacts(Statement{Incoming: scope, Retained: scope, After: scope, Columns: columns})
	if err != nil {
		t.Fatal(err)
	}
	facts.Visit(func(statement Statement) {
		if err := statement.Columns.Verify([]string{"page"}); err != nil {
			t.Fatal(err)
		}
	}, func() { t.Fatal("declined declared variants") }, func(Unresolved) { t.Fatal("lost declared variants") })
}
