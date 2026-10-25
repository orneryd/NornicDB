package cypher

import (
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
)

func boundGraphSelection(reference cypherantlr.UseReference) *resultmetadata.GraphSelection {
	return &resultmetadata.GraphSelection{
		Name:      reference.Name,
		Function:  reference.Function,
		Arguments: append([]string(nil), reference.Args...),
	}
}

// compileFragmentHorizons publishes the existing binding walk's boundaries.
// Every query step consumes a complete stream. Only a CALL step invokes its
// nested program per input row; USE merely changes the participant of a stream.
func compileFragmentHorizons(statement *resultmetadata.Statement) {
	first := 0
	flush := func(end int, final bool) {
		if first == end {
			return
		}
		clauses := statement.Clauses[first:end]
		last := clauses[len(clauses)-1]
		fragment := resultmetadata.BoundFragment{
			Kind: resultmetadata.ClauseOther, First: first, End: end,
			Incoming: boundFragmentValues(statement, clauses[0].Before), Retained: statement.Retained,
			After: boundFragmentValues(statement, last.After), WholeStream: true, Multiplicity: resultmetadata.CompleteStream, Entry: clauses[0].Entry, Exit: last.Exit,
			Returning: last.Kind == resultmetadata.ClauseReturn,
		}
		fragment.Columns = resultmetadata.KnownColumns(scopeValues(fragment.After))
		if final {
			fragment.Columns = statement.Columns
		}
		fragment.Incoming.Visit(func(names []string) {
			for _, name := range names {
				current, known := fragment.Incoming.Binding(name)
				entry, retained := fragment.Retained.Binding(name)
				if known && retained && current == entry {
					fragment.ParentInputs = append(fragment.ParentInputs, name)
				} else {
					fragment.RowInputs = append(fragment.RowInputs, name)
				}
			}
		}, func() {})
		statement.Fragments = append(statement.Fragments, fragment)
	}
	for index, clause := range statement.Clauses {
		if clause.Kind != resultmetadata.ClauseUse && (clause.Call == nil || !boundStatementHasUse(clause.Call.Statement)) {
			continue
		}
		flush(index, false)
		columns := resultmetadata.KnownColumns(scopeValues(boundFragmentValues(statement, clause.After)))
		if index == len(statement.Clauses)-1 {
			columns = statement.Columns
		}
		mode := resultmetadata.CompleteStream
		if clause.Call != nil {
			mode = resultmetadata.PerInputRow
		}
		statement.Fragments = append(statement.Fragments, resultmetadata.BoundFragment{Kind: clause.Kind, First: index, End: index + 1, Columns: columns, Incoming: boundFragmentValues(statement, clause.Before), Retained: statement.Retained, After: boundFragmentValues(statement, clause.After), Entry: clause.Entry, Exit: clause.Exit, Multiplicity: mode})
		first = index + 1
	}
	flush(len(statement.Clauses), true)
}

func boundCallRouting(statement *resultmetadata.Statement, enclosing resultmetadata.Scope) resultmetadata.Scope {
	names := map[string]struct{}{}
	identities := map[string]statementBindingID{}
	var visit func(*resultmetadata.Statement)
	visit = func(statement *resultmetadata.Statement) {
		for index := range statement.Branches {
			visit(&statement.Branches[index])
		}
		for _, clause := range statement.Clauses {
			if clause.Kind == resultmetadata.ClauseUse {
				for _, name := range scopeValues(clause.Routing) {
					current, exists := clause.Routing.Binding(name)
					outer, visible := enclosing.Binding(name)
					if exists && visible && current == outer {
						names[name] = struct{}{}
						identities[name] = current
					}
				}
			}
			if clause.Call != nil {
				visit(clause.Call.Statement)
			}
		}
	}
	visit(statement)
	// Routing dependencies cross the CALL boundary only when already visible
	// there. Names declared by its body travel in later clause horizons.
	return boundBindingScope(projectedMapNames(names), identities)
}

func scopeValues(scope resultmetadata.Scope) (names []string) {
	scope.Visit(func(values []string) { names = values }, func() {})
	return names
}

func boundStatementHasUse(statement *resultmetadata.Statement) bool {
	for _, branch := range statement.Branches {
		if boundStatementHasUse(&branch) {
			return true
		}
	}
	for _, clause := range statement.Clauses {
		if clause.Kind == resultmetadata.ClauseUse || clause.Call != nil && boundStatementHasUse(clause.Call.Statement) {
			return true
		}
	}
	return false
}

// Weak candidates remain readable during admission. Only entry identities the
// branch actually consumes cross an execution cut; a same-name local remains.
func boundFragmentValues(statement *resultmetadata.Statement, scope resultmetadata.Scope) resultmetadata.Scope {
	if statement.Entry == nil {
		return scope
	}
	resolved := false
	var names []string
	identities := make(map[string]statementBindingID)
	scope.Visit(func(values []string) {
		resolved = true
		for _, name := range values {
			identity, known := scope.Binding(name)
			weak, candidate := statement.Entry.Weak.Binding(name)
			_, required := statement.Incoming.Binding(name)
			if known && candidate && weak == identity && !required {
				continue
			}
			names = append(names, name)
			if known {
				identities[name] = identity
			}
		}
	}, func() {})
	if !resolved {
		return scope
	}
	return resultmetadata.KnownBindingScope(names, identities)
}
