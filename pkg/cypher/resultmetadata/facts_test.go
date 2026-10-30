package resultmetadata

import (
	"errors"
	"reflect"
	"testing"
)

func TestFactsDeclaredHorizons(t *testing.T) {
	empty := KnownScope(nil)
	horizon := &BindingHorizon{Readable: empty, Current: empty, Retained: empty, Weak: empty, Dependencies: empty}
	statement := Statement{Incoming: empty, Retained: empty, After: empty, Columns: KnownColumns(nil), Entry: horizon, Exit: horizon,
		Clauses:   []Clause{{Before: empty, After: empty, Entry: horizon, Exit: horizon}},
		Fragments: []BoundFragment{{Kind: ClauseOther, First: 0, End: 1, Incoming: empty, Retained: empty, After: empty, Columns: KnownColumns(nil), Multiplicity: CompleteStream, Entry: horizon, Exit: horizon}},
	}
	facts, err := KnownFacts(statement)
	if err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 2; round++ {
		facts.Visit(func(copy Statement) {
			if !completeHorizon(copy.Entry) || !completeHorizon(copy.Clauses[0].Entry) || !completeHorizon(copy.Fragments[0].Entry) {
				t.Fatal("mutable horizon escaped")
			}
			copy.Entry.Current = UnknownScope()
			copy.Clauses[0].Entry.Readable = UnknownScope()
			copy.Fragments[0].Entry.Dependencies = UnknownScope()
		}, func() { t.Fatal("declined") }, func(reason Unresolved) { t.Fatal(reason.Err) })
	}
	statement.Entry.Current = UnknownScope()
	if _, err := KnownFacts(statement); err == nil {
		t.Fatal("unknown current membership admitted")
	}
}

func TestFactsCommandContinuationCopy(t *testing.T) {
	empty := KnownScope(nil)
	facts, err := KnownFacts(Statement{Incoming: empty, Retained: empty, After: empty, Columns: KnownColumns(nil), Command: &Command{Text: "CREATE INDEX example FOR (n:Item) ON (n.id)", Graph: &GraphSelection{Function: "graph.byName", Arguments: []string{"$graph"}}}})
	if err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 2; round++ {
		facts.Visit(func(statement Statement) {
			if statement.Command.Graph.Arguments[0] != "$graph" {
				t.Fatal("cached command route was mutated")
			}
			statement.Command.Graph.Arguments[0] = "corrupted"
			statement.Command.Text = "corrupted"
		}, func() { t.Fatal("known command declined") }, func(reason Unresolved) { t.Fatal(reason.Err) })
	}
}

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
	identities := map[string]BindingID{"z": 1, "a": 2}
	scope := KnownBindingScope([]string{"z", "a", "z"}, identities)
	identities["z"] = 3
	if id, ok := scope.Binding("z"); !ok || id != 1 {
		t.Fatal("scope shares mutable declaration identities")
	}
	scope.Visit(func(names []string) {
		if !reflect.DeepEqual(names, []string{"a", "z"}) {
			t.Fatal(names)
		}
		names[0] = "mutated"
	}, func() { t.Fatal("known scope unknown") })
	statement := Statement{Incoming: scope, Retained: KnownScope(nil), After: scope, Columns: KnownColumns([]string{"z", "a"}), Entry: testHorizon(scope), Exit: testHorizon(scope)}
	statement.Clauses = []Clause{{Before: scope, After: scope, Entry: testHorizon(scope), Exit: testHorizon(scope), Projection: &Projection{Expressions: []string{"z", "a"}, Names: Known([]string{"z", "a"})}}}
	facts, err := KnownFacts(statement)
	if err != nil {
		t.Fatal(err)
	}
	statement.Clauses[0].Projection.Expressions[0] = "outside mutation"
	for round := 0; round < 2; round++ {
		facts.Visit(func(copy Statement) {
			if id, ok := copy.Incoming.Binding("z"); !ok || id != 1 {
				t.Fatal("cached scope lost declaration identity")
			}
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
	leaf := Statement{Incoming: empty, Retained: empty, After: KnownScope([]string{"value"}), Columns: KnownColumns([]string{"value"}), Entry: testHorizon(empty), Exit: testHorizon(KnownScope([]string{"value"}))}
	leaf.Clauses = []Clause{{Kind: ClauseReturn, Before: empty, After: leaf.After, Entry: testHorizon(empty), Exit: testHorizon(leaf.After), Projection: &Projection{Expressions: []string{"1"}, Names: Known([]string{"value"})}}}
	body := leaf
	body.Clauses = nil
	body.Branches = []Statement{leaf, leaf}
	body.UnionAll = []bool{true}
	parent := Statement{Incoming: empty, Retained: empty, After: leaf.After, Columns: leaf.Columns, Entry: testHorizon(empty), Exit: testHorizon(leaf.After)}
	parent.Clauses = []Clause{{Kind: ClauseCallSubquery, Before: empty, After: leaf.After, Entry: testHorizon(empty), Exit: testHorizon(leaf.After), Call: &Call{Mode: ImportListed, Imports: []string{}, Retained: empty, Body: leaf.Columns, Kind: CallReturning, After: leaf.After, Statement: &body}}}
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
	facts, err := KnownFacts(Statement{Incoming: scope, Retained: scope, After: scope, Columns: columns, Entry: testHorizon(scope), Exit: testHorizon(scope)})
	if err != nil {
		t.Fatal(err)
	}
	facts.Visit(func(statement Statement) {
		if err := statement.Columns.Verify([]string{"page"}); err != nil {
			t.Fatal(err)
		}
	}, func() { t.Fatal("declined declared variants") }, func(Unresolved) { t.Fatal("lost declared variants") })
}

func testHorizon(scope Scope) *BindingHorizon {
	empty := KnownScope(nil)
	return &BindingHorizon{Readable: scope, Current: scope, Retained: empty, Weak: empty, Dependencies: empty}
}

func TestFactsRejectMissingHorizons(t *testing.T) {
	empty := KnownScope(nil)
	graph := Statement{Incoming: empty, Retained: empty, After: empty, Columns: KnownColumns(nil), Entry: testHorizon(empty), Exit: testHorizon(empty), Clauses: []Clause{{Before: empty, After: empty, Entry: testHorizon(empty), Exit: testHorizon(empty)}}, Fragments: []BoundFragment{{First: 0, End: 1, Incoming: empty, Retained: empty, After: empty, Columns: KnownColumns(nil), Entry: testHorizon(empty), Exit: testHorizon(empty)}}}
	for _, clear := range []func(*Statement){func(s *Statement) { s.Entry = nil }, func(s *Statement) { s.Exit = nil }, func(s *Statement) { s.Clauses[0].Entry = nil }, func(s *Statement) { s.Clauses[0].Exit = nil }, func(s *Statement) { s.Fragments[0].Entry = nil }, func(s *Statement) { s.Fragments[0].Exit = nil }} {
		copy := cloneStatement(graph)
		clear(&copy)
		if _, err := KnownFacts(copy); err == nil {
			t.Fatal("missing graph horizon admitted")
		}
	}
	if _, err := KnownFacts(graph); err != nil {
		t.Fatal(err)
	}
	graph.Command = &Command{Text: "SHOW USERS"}
	if _, err := KnownFacts(graph); err == nil {
		t.Fatal("command with graph schedule admitted")
	}
}
