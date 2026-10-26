package callscopetest

import (
	"fmt"
	"slices"
	"strings"
)

// TransitionCase carries independent scope expectations shared by the engine
// and protocol tests. Code is the expected terminal Neo4j status, if any.
type TransitionCase struct {
	DesignCase
	Code string

	// NornicDB's bare implicit inputs and unchanged import exports extend
	// Neo4j. Only statements without those extensions share its expectation.
	Neo4jKnown bool

	// Body and Steps are the independent expected transition trace of the
	// outer CALL. Nested CALLs are checked recursively by the same consumers.
	Call, Body  string
	BodyColumns []string
	Steps       []TransitionStep

	Incoming, Retained, After []string

	BodyIncoming []string
}

// TransitionStep records membership and readability before the clause, after
// its own lexical reads, and before its declarations/projection. A child
// reports importing-WITH reads when CALL exports back to this scope.
type TransitionStep struct {
	Text              string
	Current, Readable []string
}

// TransitionRegressions are the three counterexamples from the scope review.
// Their expected outcomes follow the approved Neo4j 5.26 contract.
func TransitionRegressions() []TransitionCase {
	return []TransitionCase{
		{DesignCase: DesignCase{"delete_retained", "CREATE (n:R73Delete) WITH n CALL (n) { WITH 1 AS marker DELETE n } RETURN count(*) AS kept", []string{"kept"}, [][]interface{}{{int64(1)}}}, Neo4jKnown: true},
		{DesignCase: DesignCase{"read_union", "WITH 1 AS a CALL (a) { UNWIND [a] AS z RETURN * UNION RETURN 2 AS z } RETURN a,z", nil, nil}, Code: "Neo.ClientError.Statement.SyntaxError", Neo4jKnown: true},
		{DesignCase: DesignCase{"nested_local", "WITH 1 AS a CALL (a) { CALL () { WITH 2 AS a RETURN a AS b } RETURN * UNION RETURN 3 AS b } RETURN a,b ORDER BY b", []string{"a", "b"}, [][]interface{}{{int64(1), int64(2)}, {int64(1), int64(3)}}}, Neo4jKnown: true},
	}
}

// TransitionCases generates a bounded Cartesian product of import forms,
// reads/declarations, projection horizons, UNION and empty input. This model
// uses only sets and explicit events; it imports no production parser/owner.
func TransitionCases() []TransitionCase {
	cases := TransitionRegressions()
	type action struct {
		name, text string
		reads      []string
		value      interface{}
		mutation   bool
	}
	actions := []action{
		{"with", "WITH 2 AS x", nil, int64(2), false},
		{"unwind", "UNWIND [2] AS x", nil, int64(2), false},
		{"read", "UNWIND [a] AS x", []string{"a"}, int64(1), false},
		{"nested_local", "CALL () { WITH 2 AS a RETURN a AS x }", nil, int64(2), false},
		{"nested_import", "CALL { WITH a RETURN a AS x }", []string{"a"}, int64(1), false},
		{"nested_listed", "CALL (a) { RETURN a AS x }", nil, int64(1), false},
		{"nested_all", "CALL (*) { RETURN a AS x }", nil, int64(1), false},
		{"optional", "OPTIONAL CALL () { WITH 2 AS x WHERE false RETURN x }", nil, nil, false},
		{"delete", "DELETE n", []string{"n"}, int64(2), true},
		{"set", "SET n.seen = x", []string{"n", "x"}, int64(2), true},
		{"create", "CREATE (:ScopeTransitionCreated {seen:n.value})", []string{"n"}, int64(2), true},
		{"merge", "MERGE (:ScopeTransitionMerged {seen:n.value})", []string{"n"}, int64(2), true},
	}
	for _, form := range []string{"listed", "all", "empty", "bare", "legacy"} {
		for _, action := range actions {
			for _, horizon := range []string{"star", "with_star", "with_x", "union"} {
				// UNION neighbours concentrate on read/locality/horizon changes;
				// repeating each write adds no different transition boundary.
				if horizon == "union" && action.mutation {
					continue
				}
				for _, empty := range []bool{false, true} {
					outer := "UNWIND [1,1] AS a "
					inputs := []string{"a"}
					if action.mutation {
						outer = "UNWIND [1,1] AS a CREATE (n:ScopeTransition {value:1}) WITH n, a "
						inputs = append(inputs, "n")
					}
					if empty {
						outer = strings.Replace(outer, "UNWIND [1,1] AS a", "UNWIND [] AS a", 1)
					}
					header := "CALL (" + strings.Join(inputs, ",") + ")"
					if form == "all" {
						header = "CALL (*)"
					}
					if form == "empty" {
						header = "CALL ()"
					}
					if form == "bare" || form == "legacy" {
						header = "CALL"
					}
					current, carried := map[string]bool{}, map[string]bool{}
					for _, name := range inputs {
						if form != "empty" {
							carried[name] = true
						}
						if form == "bare" || form == "legacy" {
							current[name] = true
						}
					}
					if form == "legacy" {
						carried = map[string]bool{}
					}
					item := TransitionCase{DesignCase: DesignCase{Name: fmt.Sprintf("%s/%s/%s/empty=%v", form, action.name, horizon, empty), Columns: []string{"a", "x"}}}
					var clauses []string
					read := func(reads []string) {
						for _, name := range reads {
							if !current[name] && !carried[name] {
								item.Code = "Neo.ClientError.Statement.SyntaxError"
							}
							if carried[name] && (form == "listed" || form == "all") {
								current[name] = true
							}
						}
					}
					step := func(text string, reads []string, declarations []string, replace bool) {
						read(reads)
						readable := map[string]bool{}
						for name := range carried {
							readable[name] = true
						}
						for name := range current {
							readable[name] = true
						}
						item.Steps = append(item.Steps, TransitionStep{text, transitionNames(current), transitionNames(readable)})
						clauses = append(clauses, text)
						if replace {
							current = map[string]bool{}
						}
						for _, name := range declarations {
							current[name] = true
						}
					}
					if form == "legacy" {
						step("WITH "+strings.Join(inputs, ","), inputs, inputs, true)
					}
					if action.mutation {
						step("WITH 2 AS x", nil, []string{"x"}, true)
					}
					// A scope header transports an available identity. The
					// child's local expression reads do not become parent reads.
					if (action.name == "nested_listed" || action.name == "nested_all") && !current["a"] && !carried["a"] {
						item.Code = "Neo.ClientError.Statement.SyntaxError"
					}
					if action.name == "nested_import" {
						step(action.text, nil, nil, false)
						read(action.reads)
					} else {
						step(action.text, action.reads, nil, false)
					}
					if !action.mutation {
						current["x"] = true
					}
					if action.name == "with" {
						current = map[string]bool{"x": true}
					}
					if horizon == "with_star" {
						step("WITH *", nil, nil, false)
					}
					if horizon == "with_x" {
						step("WITH x", []string{"x"}, []string{"x"}, true)
					}
					step("RETURN *", nil, nil, false)
					item.BodyColumns = transitionNames(current)
					item.Body = strings.Join(clauses, " ")
					if horizon == "union" {
						item.Body += " UNION RETURN 5 AS x"
						if !slices.Equal(item.BodyColumns, []string{"x"}) {
							item.Code = "Neo.ClientError.Statement.SyntaxError"
						}
					}
					item.Call = header + " { " + item.Body + " }"
					item.Query = outer + item.Call + " RETURN a,x ORDER BY x"
					item.Incoming = slices.Clone(inputs)
					item.BodyIncoming = []string{}
					// Incoming describes readable inputs, rather than the
					// current/star membership recorded separately in Steps.
					if form != "empty" {
						item.BodyIncoming = slices.Clone(inputs)
					}
					item.Retained = []string{}
					if form == "listed" || form == "all" {
						item.Retained = slices.Clone(inputs)
					}
					after := map[string]bool{}
					for _, name := range append(slices.Clone(inputs), item.BodyColumns...) {
						after[name] = true
					}
					item.After = transitionNames(after)
					item.Neo4jKnown = form != "bare"
					if item.Code == "" {
						for _, name := range inputs {
							if slices.Contains(item.BodyColumns, name) {
								item.Neo4jKnown = false
							}
						}
					}
					if !empty {
						item.Rows = [][]interface{}{{int64(1), action.value}, {int64(1), action.value}}
						if horizon == "union" {
							item.Rows = append(item.Rows, []interface{}{int64(1), int64(5)}, []interface{}{int64(1), int64(5)})
							if action.value == nil {
								// Ascending ORDER BY puts null after ordinary values.
								item.Rows[0], item.Rows[2] = item.Rows[2], item.Rows[0]
								item.Rows[1], item.Rows[3] = item.Rows[3], item.Rows[1]
							}
						}
					} else {
						item.Rows = [][]interface{}{}
					}
					cases = append(cases, item)
				}
			}
		}
	}
	return cases
}

func transitionNames(names map[string]bool) []string {
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	slices.Sort(result)
	return result
}
