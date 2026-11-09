package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR25 actual-read membership for CALL subquery imports.
//
// Neo4j 5.26.30 was probed directly (task-owned isolated container, pure
// scalar/read queries, every expectation below recorded from its output).
// The verified rule is:
//
//   - An imported name is readable inside the body but is not a current
//     wildcard name until the body reads it. A read in a non-projecting
//     clause expression (UNWIND's list, MATCH's WHERE, a RETURN item, a
//     top-level expression) makes the import a member of the body's current
//     name set, so RETURN * / WITH * then expand to it.
//   - A WITH clause replaces the current set with its own projection: the
//     names it lists, or, for WITH *, the current set at that point. Reads in
//     a WITH's own projection or in its WHERE/ORDER tail are evaluated
//     against the input set but do not join the WITH's output set.
//   - An expression (scalar) body captures enclosing bindings as constants.
//     They are readable in expressions but are not members of the body's
//     star, and a nested scalar read does not make the enclosing name a
//     member of the CALL body's current set.
//
// NornicDB keeps three documented #907 extension points that this file
// pins down separately: an unscoped body may read outer locals without an
// importing WITH, an unchanged export of an outer name is accepted, and a
// scoped body may declare an imported name again (the new value stays inside
// the body). The unchanged-export extension is why several queries below are
// errors on Neo4j but have product rows here; the union mismatch controls
// isolate membership from that extension.

type membershipRowCase struct {
	name    string
	query   string
	columns []string
	rows    [][]interface{}
	// empty asserts a successful, column-declaring, row-less result.
	empty bool
}

type membershipErrorCase struct {
	name    string
	query   string
	message string // optional asserted substring
}

// newMembershipExecutor returns the fresh per-case fixture: a namespaced
// in-memory engine and an executor over it, so no case can read another's
// data or metadata.
func newMembershipExecutor(t *testing.T, label string) *StorageExecutor {
	t.Helper()
	namespace := "scoped_import_membership_" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()) + "_" + label
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), namespace))
}

func runMembershipRowCases(t *testing.T, cases []membershipRowCase) {
	t.Helper()
	for index, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			exec := newMembershipExecutor(t, fmt.Sprintf("rows_%d", index))
			result, err := exec.Execute(context.Background(), testCase.query, nil)
			require.NoError(t, err, testCase.query)
			require.Equal(t, testCase.columns, result.Columns, testCase.query)
			if testCase.empty {
				require.Empty(t, result.Rows, testCase.query)
				return
			}
			require.Equal(t, testCase.rows, result.Rows, testCase.query)
		})
	}
}

func runMembershipErrorCases(t *testing.T, cases []membershipErrorCase) {
	t.Helper()
	for index, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			exec := newMembershipExecutor(t, fmt.Sprintf("errors_%d", index))
			_, err := exec.Execute(context.Background(), testCase.query, nil)
			require.Error(t, err, testCase.query)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, testCase.query)
			if testCase.message != "" {
				require.Contains(t, err.Error(), testCase.message, testCase.query)
			}
		})
	}
}

// TestScopedImportReadTransitionsMembership: reading a scoped import in a
// body clause expression makes it a current wildcard name. The union
// mismatch controls show the promoted name in the branch's column set; the
// matching branch shows the promoted name carrying the outer value.
func TestScopedImportReadTransitionsMembership(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		t.Run("rows", func(t *testing.T) {
			runMembershipRowCases(t, []membershipRowCase{
				{
					// Read in a RETURN item promotes a into RETURN *; the
					// unchanged export is accepted (#907).
					name:    "return item promotes imported name",
					query:   "WITH 1 AS a CALL (a) { RETURN *, a + 1 AS v } RETURN a, v",
					columns: []string{"a", "v"},
					rows:    [][]interface{}{{int64(1), int64(2)}},
				},
				{
					// Read in UNWIND's list promotes a; the explicit star
					// items keep their declared order.
					name:    "unwind list promotes imported name",
					query:   "WITH 1 AS a CALL (a) { UNWIND [a] AS z RETURN *, 0 AS w } RETURN a, z, w",
					columns: []string{"a", "z", "w"},
					rows:    [][]interface{}{{int64(1), int64(1), int64(0)}},
				},
				{
					// The promoted import is the outer value in both branches.
					name:    "promoted import matches explicit branch columns",
					query:   "WITH 1 AS a CALL (a) { UNWIND [a] AS z RETURN * UNION RETURN a, 3 AS z } RETURN a, z",
					columns: []string{"a", "z"},
					rows:    [][]interface{}{{int64(1), int64(1)}, {int64(1), int64(3)}},
				},
				{
					// WITH * keeps the promoted name; the next RETURN * is
					// therefore still two columns short of the local branch.
					name:    "with star after read keeps imported name",
					query:   "WITH 1 AS a CALL (a) { UNWIND [a] AS z WITH * RETURN z } RETURN a, z",
					columns: []string{"a", "z"},
					rows:    [][]interface{}{{int64(1), int64(1)}},
				},
				{
					// CALL (*) imports the same way.
					name:    "scope star read promotes imported name",
					query:   "WITH 1 AS a CALL (*) { UNWIND [a] AS z RETURN * } RETURN a, z",
					columns: []string{"a", "z"},
					rows:    [][]interface{}{{int64(1), int64(1)}},
				},
				{
					// A MATCH clause does not replace the current set, so its
					// WHERE read promotes the import as well.
					name:    "match where read promotes imported name",
					query:   "WITH 1 AS a CALL (a) { MATCH (n:Missing) WHERE n.x = a RETURN * UNION RETURN a, 3 AS n } RETURN a, n",
					columns: []string{"a", "n"},
					rows:    [][]interface{}{{int64(1), int64(3)}},
				},
				{
					// A read in a RETURN item does not export the import by
					// itself; it only makes it a wildcard name.
					name:    "read in return item does not export by itself",
					query:   "WITH 1 AS a CALL (a) { RETURN a + 1 AS b } RETURN a, b",
					columns: []string{"a", "b"},
					rows:    [][]interface{}{{int64(1), int64(2)}},
				},
				{
					name:    "outer wildcard keeps outer value beside new result",
					query:   "WITH 1 AS a CALL (a) { RETURN a + 1 AS b } RETURN *",
					columns: []string{"a", "b"},
					rows:    [][]interface{}{{int64(1), int64(2)}},
				},
			})
		})
		t.Run("errors", func(t *testing.T) {
			runMembershipErrorCases(t, []membershipErrorCase{
				{
					// Membership, isolated from the unchanged-export
					// extension: the first branch's star is {a,z} while the
					// second branch returns only z.
					name:  "unwind read star union local only branch",
					query: "WITH 1 AS a CALL (a) { UNWIND [a] AS z RETURN * UNION RETURN 2 AS z } RETURN a, z",
				},
				{
					// Same membership with WITH * between the read and the
					// star.
					name:  "with star after read union local only branch",
					query: "WITH 1 AS a CALL (a) { UNWIND [a] AS z WITH * RETURN * UNION RETURN 2 AS z } RETURN a, z",
				},
				{
					// The promoted name is present on both sides here, so the
					// membership verdict is a row result (see rows above);
					// this control pins the incompatible local-only shape.
					name:  "unwind read star union unmatched local",
					query: "WITH 1 AS a CALL (a) { UNWIND [a] AS z RETURN * UNION RETURN 2 AS z, 3 AS w } RETURN a, z, w",
				},
				{
					name:  "scope star match where read union local only branch",
					query: "WITH 1 AS a CALL (*) { MATCH (n:Missing) WHERE n.x = a RETURN * UNION RETURN 2 AS n } RETURN a, n",
				},
			})
		})
	})
}

// TestScopedImportUnreadNamesStayOutOfWildcard: an import that the body never
// reads is readable but not a current wildcard name. This is the rule that
// forbids restoring the historical unread-all-outer wildcard extension.
func TestScopedImportUnreadNamesStayOutOfWildcard(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		t.Run("rows", func(t *testing.T) {
			runMembershipRowCases(t, []membershipRowCase{
				{
					name:    "unread import stays out of unwind star",
					query:   "WITH 1 AS a CALL (a) { UNWIND [1] AS z RETURN * } RETURN a, z",
					columns: []string{"a", "z"},
					rows:    [][]interface{}{{int64(1), int64(1)}},
				},
				{
					// Both branches are the single local z, so the union is
					// accepted and the outer a passes through.
					name:    "unread import star union local only branch",
					query:   "WITH 1 AS a CALL (a) { UNWIND [1] AS z RETURN * UNION RETURN 2 AS z } RETURN *",
					columns: []string{"a", "z"},
					rows:    [][]interface{}{{int64(1), int64(1)}, {int64(1), int64(2)}},
				},
				{
					name:    "unread import stays out of with star",
					query:   "WITH 1 AS a CALL (a) { UNWIND [1] AS z WITH * RETURN * } RETURN a, z",
					columns: []string{"a", "z"},
					rows:    [][]interface{}{{int64(1), int64(1)}},
				},
				{
					name:    "empty with star projection still runs",
					query:   "WITH 1 AS a CALL (a) { WITH * RETURN 1 AS x } RETURN *",
					columns: []string{"a", "x"},
					rows:    [][]interface{}{{int64(1), int64(1)}},
				},
				{
					// A WITH WHERE tail read sees the import but does not
					// make it a member of the WITH's output set.
					name:    "with where tail read does not promote",
					query:   "WITH 1 AS a CALL (a) { WITH 5 AS v WHERE a > 0 RETURN * } RETURN a, v",
					columns: []string{"a", "v"},
					rows:    [][]interface{}{{int64(1), int64(5)}},
				},
				{
					name:    "with order tail read does not promote",
					query:   "WITH 1 AS a CALL (a) { WITH 5 AS v RETURN * ORDER BY a } RETURN a, v",
					columns: []string{"a", "v"},
					rows:    [][]interface{}{{int64(1), int64(5)}},
				},
				{
					name:    "with projection expression reads import without promoting",
					query:   "WITH 1 AS a CALL (a) { WITH a + 1 AS v RETURN * } RETURN a, v",
					columns: []string{"a", "v"},
					rows:    [][]interface{}{{int64(1), int64(2)}},
				},
				{
					name:    "return order tail read does not promote",
					query:   "WITH 1 AS a CALL (a) { WITH 2 AS v RETURN * ORDER BY a } RETURN *",
					columns: []string{"a", "v"},
					rows:    [][]interface{}{{int64(1), int64(2)}},
				},
				{
					name:    "return order tail with limit does not promote",
					query:   "WITH 1 AS a CALL (a) { WITH 2 AS v RETURN * ORDER BY a LIMIT 1 } RETURN v",
					columns: []string{"v"},
					rows:    [][]interface{}{{int64(2)}},
				},
				{
					// A nested scalar capture is not an own lexical read.
					name:    "nested scalar capture does not promote",
					query:   "WITH 1 AS a CALL (a) { WITH * RETURN COLLECT { RETURN a } AS c } RETURN *",
					columns: []string{"a", "c"},
					rows:    [][]interface{}{{int64(1), []interface{}{int64(1)}}},
				},
				{
					name:    "nested scalar result beside outer expression",
					query:   "WITH 1 AS a CALL (a) { RETURN COLLECT { RETURN a AS x } AS c } RETURN *",
					columns: []string{"a", "c"},
					rows:    [][]interface{}{{int64(1), []interface{}{int64(1)}}},
				},
				{
					name:    "outer expression reads import beside nested scalar",
					query:   "WITH 1 AS a CALL (a) { RETURN a + size(COLLECT { RETURN 1 AS x }) AS c } RETURN *",
					columns: []string{"a", "c"},
					rows:    [][]interface{}{{int64(1), int64(2)}},
				},
				{
					name:    "with alias of import replaces scope",
					query:   "WITH 1 AS a CALL (a) { WITH a AS v WHERE v > 0 RETURN * } RETURN v",
					columns: []string{"v"},
					rows:    [][]interface{}{{int64(1)}},
				},
				{
					name:    "with alias of import ordered by alias",
					query:   "WITH 1 AS a CALL (a) { WITH a AS v ORDER BY v RETURN * } RETURN v",
					columns: []string{"v"},
					rows:    [][]interface{}{{int64(1)}},
				},
				{
					name:    "scope star unread import star",
					query:   "WITH 1 AS a, 2 AS b CALL (*) { UNWIND [1] AS z RETURN * } RETURN *",
					columns: []string{"a", "b", "z"},
					rows:    [][]interface{}{{int64(1), int64(2), int64(1)}},
				},
				{
					name:    "scope star with where tail read",
					query:   "WITH 1 AS a CALL (*) { WITH 5 AS v WHERE a > 0 RETURN * } RETURN a, v",
					columns: []string{"a", "v"},
					rows:    [][]interface{}{{int64(1), int64(5)}},
				},
			})
		})
		t.Run("errors", func(t *testing.T) {
			runMembershipErrorCases(t, []membershipErrorCase{
				{
					// No body-local name and no read: the star set is empty.
					name:    "unread import only star is empty",
					query:   "WITH 1 AS a CALL (a) { RETURN * } RETURN *",
					message: "in scope",
				},
				{
					name:    "unread imports only star is empty",
					query:   "WITH 1 AS a, 2 AS b CALL (a) { RETURN * } RETURN *",
					message: "in scope",
				},
				{
					name:    "empty with star then star is empty",
					query:   "WITH 1 AS a CALL (a) { WITH * RETURN * } RETURN *",
					message: "in scope",
				},
				{
					// The unread import must not be resurrected into WITH *.
					name:  "unread import absent from with star union",
					query: "WITH 1 AS a CALL (a) { UNWIND [1] AS z WITH * RETURN * UNION RETURN a, 3 AS z } RETURN *",
				},
				{
					// The WITH WHERE tail read must not join the output set.
					name:  "where tail read star union outer name branch",
					query: "WITH 1 AS a CALL (a) { WITH 5 AS v WHERE a > 0 RETURN * UNION RETURN 2 AS a, 3 AS v } RETURN *",
				},
				{
					name:  "order tail read star union outer name branch",
					query: "WITH 1 AS a CALL (a) { WITH 5 AS v RETURN * ORDER BY a UNION RETURN 2 AS a, 3 AS v } RETURN *",
				},
				{
					// A nested scalar capture is not an own lexical read, so
					// the star set is still empty.
					name:    "nested scalar capture leaves star empty",
					query:   "WITH 1 AS a CALL (a) { RETURN *, COLLECT { RETURN a } AS c } RETURN *",
					message: "in scope",
				},
				{
					// Nested CALL scopes do not inherit the grandparent
					// import list.
					name:    "nested call cannot read unlisted grandparent",
					query:   "WITH 1 AS a CALL (a) { WITH 2 AS b CALL (b) { RETURN a AS x } RETURN x } RETURN x",
					message: "Variable `a` not defined",
				},
				{
					name:    "nested call star with no local names",
					query:   "WITH 1 AS a CALL (a) { WITH 2 AS b CALL (a, b) { RETURN * } RETURN * } RETURN *",
					message: "in scope",
				},
			})
		})
	})
}

// TestScopedImportExportExtensionBoundaries: the documented #907 unchanged
// export is accepted, exporting another value under an outer name is not.
func TestScopedImportExportExtensionBoundaries(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		t.Run("rows", func(t *testing.T) {
			runMembershipRowCases(t, []membershipRowCase{
				{
					name:    "unchanged explicit export",
					query:   "WITH 1 AS a CALL (a) { RETURN a } RETURN a",
					columns: []string{"a"},
					rows:    [][]interface{}{{int64(1)}},
				},
				{
					name:    "unchanged aliased export",
					query:   "WITH 1 AS a CALL (a) { RETURN a AS a } RETURN *",
					columns: []string{"a"},
					rows:    [][]interface{}{{int64(1)}},
				},
				{
					name:    "unchanged explicit export beside local",
					query:   "WITH 1 AS a CALL (a) { UNWIND [a] AS z RETURN a AS a, z } RETURN a, z",
					columns: []string{"a", "z"},
					rows:    [][]interface{}{{int64(1), int64(1)}},
				},
				{
					name:    "unchanged star export after read",
					query:   "WITH 1 AS a CALL (a) { UNWIND [a] AS z RETURN * } RETURN a, z",
					columns: []string{"a", "z"},
					rows:    [][]interface{}{{int64(1), int64(1)}},
				},
				{
					name:    "unchanged explicit export beside with local",
					query:   "WITH 1 AS a CALL (a) { WITH 2 AS v RETURN a, v } RETURN a, v",
					columns: []string{"a", "v"},
					rows:    [][]interface{}{{int64(1), int64(2)}},
				},
				{
					// Scoped rebind keeps the new value inside the body.
					name:    "scoped rebind with alias export",
					query:   "WITH 1 AS a CALL (a) { WITH 2 AS a RETURN a AS x } RETURN x",
					columns: []string{"x"},
					rows:    [][]interface{}{{int64(2)}},
				},
				{
					name:    "scoped unwind rebind with alias export",
					query:   "WITH 1 AS a CALL (a) { UNWIND [2] AS a RETURN a AS x } RETURN x",
					columns: []string{"x"},
					rows:    [][]interface{}{{int64(2)}},
				},
				{
					name:    "scoped same value alias rebind",
					query:   "WITH 1 AS a CALL (a) { WITH a AS a RETURN a AS x } RETURN x",
					columns: []string{"x"},
					rows:    [][]interface{}{{int64(1)}},
				},
				{
					name:    "rebound value exported under new name beside local",
					query:   "WITH 1 AS a CALL (a) { WITH 2 AS a RETURN a AS x, 1 AS z } RETURN *",
					columns: []string{"a", "x", "z"},
					rows:    [][]interface{}{{int64(1), int64(2), int64(1)}},
				},
			})
		})
		t.Run("errors", func(t *testing.T) {
			runMembershipErrorCases(t, []membershipErrorCase{
				{
					// A new value under the outer name is rejected.
					name:    "expression under outer name",
					query:   "WITH 1 AS a CALL (a) { RETURN a + 1 AS a } RETURN *",
					message: "already declared in outer scope",
				},
				{
					name:    "rebound local exported under outer name",
					query:   "WITH 1 AS a CALL (a) { WITH 2 AS a RETURN a AS a, 1 AS z } RETURN *",
					message: "already declared in outer scope",
				},
				{
					// The scoped rebind holds inside the body only, so a
					// promoted rebound name cannot leave through RETURN *.
					name:    "rebound local promoted by star",
					query:   "WITH 1 AS a CALL (a) { WITH 2 AS a RETURN * } RETURN *",
					message: "already declared in outer scope",
				},
				{
					name:    "unwind rebound local promoted by star",
					query:   "WITH 1 AS a CALL (a) { UNWIND [2] AS a RETURN * } RETURN *",
					message: "already declared in outer scope",
				},
			})
		})
	})
}

// TestScopedImportListsNameResolution: modern scope headers list exact
// readable names; legacy importing WITH lists simple references only and its
// unlisted outer locals stay readable (#907).
func TestScopedImportListsNameResolution(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		t.Run("rows", func(t *testing.T) {
			runMembershipRowCases(t, []membershipRowCase{
				{
					name:    "empty scope header returns local",
					query:   "WITH 1 AS a CALL () { RETURN 1 AS v } RETURN a, v",
					columns: []string{"a", "v"},
					rows:    [][]interface{}{{int64(1), int64(1)}},
				},
				{
					name:    "duplicate import listed twice",
					query:   "WITH 1 AS a CALL (a, a) { RETURN 1 AS v } RETURN v",
					columns: []string{"v"},
					rows:    [][]interface{}{{int64(1)}},
				},
				{
					name:    "duplicate import quoted and plain",
					query:   "WITH 1 AS a CALL (`a`, a) { RETURN 1 AS v } RETURN v",
					columns: []string{"v"},
					rows:    [][]interface{}{{int64(1)}},
				},
				{
					name:    "quoted import name",
					query:   "WITH 1 AS `a b` CALL (`a b`) { RETURN `a b` + 1 AS v } RETURN v",
					columns: []string{"v"},
					rows:    [][]interface{}{{int64(2)}},
				},
				{
					name:    "escaped backtick import name",
					query:   "WITH 1 AS `a``b` CALL (`a``b`) { RETURN `a``b` + 1 AS v } RETURN v",
					columns: []string{"v"},
					rows:    [][]interface{}{{int64(2)}},
				},
				{
					name:    "quoted reference to plain import",
					query:   "WITH 1 AS a CALL (`a`) { RETURN a + 1 AS v } RETURN v",
					columns: []string{"v"},
					rows:    [][]interface{}{{int64(2)}},
				},
				{
					name:    "outer wildcard after local call result",
					query:   "WITH 1 AS a CALL (a) { RETURN 1 AS v } RETURN *",
					columns: []string{"a", "v"},
					rows:    [][]interface{}{{int64(1), int64(1)}},
				},
				{
					name:    "nested call with own listed import and local",
					query:   "WITH 1 AS a CALL (a) { WITH 1 AS z CALL (a, z) { RETURN a AS x } RETURN * } RETURN *",
					columns: []string{"a", "x", "z"},
					rows:    [][]interface{}{{int64(1), int64(1), int64(1)}},
				},
				{
					// Legacy importing WITH projects the listed name, so the
					// unchanged export leaves the body (#907).
					name:    "legacy importing with star",
					query:   "WITH 1 AS a CALL { WITH a RETURN * } RETURN *",
					columns: []string{"a"},
					rows:    [][]interface{}{{int64(1)}},
				},
				{
					name:    "legacy importing with wildcard",
					query:   "WITH 1 AS a CALL { WITH * RETURN * } RETURN *",
					columns: []string{"a"},
					rows:    [][]interface{}{{int64(1)}},
				},
				{
					name:    "legacy importing with wildcard explicit return",
					query:   "WITH 1 AS a CALL { WITH * RETURN a AS x } RETURN x",
					columns: []string{"x"},
					rows:    [][]interface{}{{int64(1)}},
				},
				{
					name:    "legacy importing with explicit return",
					query:   "WITH 1 AS a CALL { WITH a RETURN a AS x } RETURN x",
					columns: []string{"x"},
					rows:    [][]interface{}{{int64(1)}},
				},
				{
					// Unlisted outer local stays readable inside a legacy body.
					name:    "legacy unlisted read beside importing with",
					query:   "WITH 1 AS a, 2 AS b CALL { WITH a RETURN b AS x } RETURN x",
					columns: []string{"x"},
					rows:    [][]interface{}{{int64(2)}},
				},
				{
					name:    "legacy unlisted read in unwind",
					query:   "WITH 1 AS a, 2 AS b CALL { UNWIND [a] AS z RETURN b AS x } RETURN x",
					columns: []string{"x"},
					rows:    [][]interface{}{{int64(2)}},
				},
				{
					// The legacy read is also a membership transition.
					name:    "legacy read promotes into star",
					query:   "WITH 1 AS a CALL { UNWIND [a] AS z RETURN * } RETURN a, z",
					columns: []string{"a", "z"},
					rows:    [][]interface{}{{int64(1), int64(1)}},
				},
				{
					name:    "legacy unread star is local only",
					query:   "WITH 1 AS a CALL { UNWIND [1] AS z RETURN * UNION RETURN 2 AS z } RETURN *",
					columns: []string{"a", "z"},
					rows:    [][]interface{}{{int64(1), int64(1)}, {int64(1), int64(2)}},
				},
			})
		})
		t.Run("errors", func(t *testing.T) {
			runMembershipErrorCases(t, []membershipErrorCase{
				{
					name:    "undefined import in scope header",
					query:   "WITH 1 AS a CALL (a, b) { RETURN 1 AS x } RETURN x",
					message: "Variable `b` not defined",
				},
				{
					name:    "reading outside empty scope header",
					query:   "WITH 1 AS a CALL () { RETURN a AS v } RETURN v",
					message: "Variable `a` not defined",
				},
				{
					// A modern scope header is exact: unlisted outer names
					// are not implicit imports.
					name:    "scoped unlisted outer name",
					query:   "WITH 1 AS a, 2 AS b CALL (a) { RETURN b AS x } RETURN x",
					message: "Variable `b` not defined",
				},
				{
					name:    "scoped unlisted outer name in expression",
					query:   "WITH 1 AS a, 2 AS b CALL (a) { RETURN a + b AS x } RETURN x",
					message: "Variable `b` not defined",
				},
				{
					name:    "importing with alias",
					query:   "WITH 1 AS a CALL { WITH a AS v RETURN * } RETURN *",
					message: "Importing WITH",
				},
				{
					name:    "importing with expression",
					query:   "WITH 1 AS a CALL { WITH a, 2 AS b RETURN * } RETURN *",
					message: "Importing WITH",
				},
				{
					// The legacy read membership mirrors a modern read: the
					// first branch is {a,z}, the second only z.
					name:  "legacy read star union local only branch",
					query: "WITH 1 AS a CALL { UNWIND [a] AS z RETURN * UNION RETURN 2 AS z } RETURN *",
				},
			})
		})
	})
}

// TestExpressionSubqueryStarCaptures: an expression body's enclosing
// bindings are constants. They are readable in expressions, are not members
// of the body's star, and do not become members through a nested read.
func TestExpressionSubqueryStarCaptures(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		t.Run("rows", func(t *testing.T) {
			runMembershipRowCases(t, []membershipRowCase{
				{
					name:    "collect captured value",
					query:   "WITH 1 AS a RETURN COLLECT { RETURN a } AS c",
					columns: []string{"c"},
					rows:    [][]interface{}{{[]interface{}{int64(1)}}},
				},
				{
					name:    "collect captured expression",
					query:   "WITH 1 AS a RETURN COLLECT { RETURN a + 1 AS b } AS c",
					columns: []string{"c"},
					rows:    [][]interface{}{{[]interface{}{int64(2)}}},
				},
				{
					name:    "collect captured value under its own name",
					query:   "WITH 1 AS a RETURN COLLECT { RETURN a AS a } AS c",
					columns: []string{"c"},
					rows:    [][]interface{}{{[]interface{}{int64(1)}}},
				},
				{
					name:    "collect local star is the local name",
					query:   "WITH 1 AS a RETURN COLLECT { UNWIND [1] AS z RETURN z } AS c",
					columns: []string{"c"},
					rows:    [][]interface{}{{[]interface{}{int64(1)}}},
				},
				{
					name:    "collect captured value after local star",
					query:   "WITH 1 AS a RETURN COLLECT { UNWIND [a] AS z RETURN z } AS c",
					columns: []string{"c"},
					rows:    [][]interface{}{{[]interface{}{int64(1)}}},
				},
				{
					name:    "collect with local projection",
					query:   "WITH 1 AS a RETURN COLLECT { WITH 1 AS z RETURN z } AS c",
					columns: []string{"c"},
					rows:    [][]interface{}{{[]interface{}{int64(1)}}},
				},
				{
					name:    "collect with star before single local return",
					query:   "WITH 1 AS a RETURN COLLECT { UNWIND [1] AS z WITH * RETURN z } AS c",
					columns: []string{"c"},
					rows:    [][]interface{}{{[]interface{}{int64(1)}}},
				},
				{
					name:    "count local star",
					query:   "WITH 1 AS a RETURN COUNT { UNWIND [1] AS z RETURN * } AS c",
					columns: []string{"c"},
					rows:    [][]interface{}{{int64(1)}},
				},
				{
					name:    "exists local star",
					query:   "WITH 1 AS a RETURN EXISTS { UNWIND [1] AS z RETURN * } AS c",
					columns: []string{"c"},
					rows:    [][]interface{}{{true}},
				},
			})
		})
		t.Run("errors", func(t *testing.T) {
			runMembershipErrorCases(t, []membershipErrorCase{
				{
					// Only enclosing constants, no body-local name.
					name:  "collect star with only captured name",
					query: "WITH 1 AS a RETURN COLLECT { RETURN * } AS c",
				},
				{
					name:  "count star with only captured name",
					query: "WITH 1 AS a RETURN COUNT { RETURN * } AS c",
				},
				{
					name:  "exists star with only captured name",
					query: "WITH 1 AS a RETURN EXISTS { RETURN * } AS c",
				},
				{
					// A nested scalar star still sees no enclosing member.
					name:  "nested collect star inside call body",
					query: "WITH 1 AS a CALL (a) { RETURN COLLECT { RETURN * } AS c } RETURN *",
				},
			})
		})
	})
}

// TestScopedImportUnionColumnControls: the acceptance/mismatch shape of
// UNION inside a body, with membership held constant.
func TestScopedImportUnionColumnControls(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		t.Run("rows", func(t *testing.T) {
			runMembershipRowCases(t, []membershipRowCase{
				{
					// Bare local-only union is accepted and retained
					// (current-release behavior).
					name:    "bare local only union",
					query:   "WITH 1 AS a CALL (a) { UNWIND [a] AS z RETURN z UNION RETURN 2 AS z } RETURN z ORDER BY z",
					columns: []string{"z"},
					rows:    [][]interface{}{{int64(1)}, {int64(2)}},
				},
				{
					name:    "local union with with star",
					query:   "WITH 1 AS a CALL (a) { UNWIND [1] AS z WITH * RETURN * UNION RETURN 2 AS z } RETURN *",
					columns: []string{"a", "z"},
					rows:    [][]interface{}{{int64(1), int64(1)}, {int64(1), int64(2)}},
				},
				{
					name:    "promoted import union carries outer value",
					query:   "WITH 1 AS a CALL (a) { UNWIND [a] AS z RETURN * } RETURN * UNION RETURN 2 AS a, 3 AS z",
					columns: []string{"a", "z"},
					rows:    [][]interface{}{{int64(1), int64(1)}, {int64(2), int64(3)}},
				},
			})
		})
		t.Run("errors", func(t *testing.T) {
			runMembershipErrorCases(t, []membershipErrorCase{
				{
					name:  "outer union star column mismatch",
					query: "WITH 1 AS a CALL (a) { UNWIND [1] AS z RETURN * } RETURN * UNION RETURN 2 AS z",
				},
				{
					name:  "outer union local column mismatch",
					query: "WITH 1 AS a CALL (a) { UNWIND [a] AS z RETURN z } RETURN * UNION RETURN 2 AS z, 3 AS w",
				},
				{
					name:  "local union star column mismatch",
					query: "WITH 1 AS a CALL (a) { UNWIND [1] AS z RETURN * UNION RETURN 2 AS z, 3 AS w } RETURN *",
				},
			})
		})
	})
}

// Neo4j 5.26 rejects a COLLECT wildcard even when it expands to one local.
func TestCollectSubqueryRequiresExplicitReturnColumn(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newUnitExecutor(t)
		for _, query := range []string{
			"RETURN COLLECT { WITH 1 AS z RETURN * } AS values",
			"RETURN COLLECT { UNWIND [1] AS z RETURN * } AS values",
			"RETURN COLLECT { WITH 1 AS z RETURN z,2 AS extra } AS values",
		} {
			_, err := exec.Execute(ctx, query, nil)
			requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
		}
		result, err := exec.Execute(ctx, "RETURN COLLECT { UNWIND [1] AS z RETURN z AS value } AS values", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{[]interface{}{int64(1)}}}, result.Rows)
	})
}
