package fabric

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

// TestParseUseClause_StaticGraphReferences pins how a USE clause naming a
// graph is read: the optional GRAPH keyword, dotted and backtick-quoted name
// parts (with “ escaping a backtick), spaces and comments around the name
// and its dots, and the query that follows the clause.
func TestParseUseClause_StaticGraphReferences(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		inSubquery    bool
		wantName      string
		wantRemaining string
	}{
		{name: "plain name", query: "USE db MATCH (n) RETURN n", wantName: "db", wantRemaining: "MATCH (n) RETURN n"},
		{name: "keywords are case-insensitive", query: "  use Db return 1  ", wantName: "Db", wantRemaining: "return 1"},
		{name: "GRAPH keyword before a name", query: "USE GRAPH db MATCH (n) RETURN n", wantName: "db", wantRemaining: "MATCH (n) RETURN n"},
		{name: "GRAPH keyword before a quoted name", query: "USE GRAPH `my db` RETURN 1", wantName: "my db", wantRemaining: "RETURN 1"},
		{name: "graph followed by a clause names the graph", query: "USE graph RETURN 1", wantName: "graph", wantRemaining: "RETURN 1"},
		{name: "GRAPH keyword then a graph called graph", query: "USE GRAPH graph RETURN 1", wantName: "graph", wantRemaining: "RETURN 1"},
		{name: "dotted constituent name", query: "USE cmp.alias RETURN 1", wantName: "cmp.alias", wantRemaining: "RETURN 1"},
		{name: "spaces around the dot", query: "USE cmp . alias RETURN 1", wantName: "cmp.alias", wantRemaining: "RETURN 1"},
		{name: "quoted parts", query: "USE `cmp`.`a.b` RETURN 1", wantName: "cmp.a.b", wantRemaining: "RETURN 1"},
		{name: "doubled backtick escapes a backtick", query: "USE `a``b` RETURN 1", wantName: "a`b", wantRemaining: "RETURN 1"},
		{name: "line comment before the name", query: "USE // pick the graph\n db RETURN 1", wantName: "db", wantRemaining: "RETURN 1"},
		{name: "block comments around the name", query: "USE /* c */ db /* d */ RETURN 1", wantName: "db", wantRemaining: "/* d */ RETURN 1"},
		{name: "SHOW USER DEFINED FUNCTIONS is a query, not an administration command", query: "USE db SHOW USER DEFINED FUNCTIONS", wantName: "db", wantRemaining: "SHOW USER DEFINED FUNCTIONS"},
		{name: "subquery USE", query: "USE cmp.a MATCH (n) RETURN n", inSubquery: true, wantName: "cmp.a", wantRemaining: "MATCH (n) RETURN n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clause, remaining, hasUse, err := ParseUseClause(tt.query, tt.inSubquery)
			require.NoError(t, err)
			require.True(t, hasUse)
			require.Equal(t, UseClause{Name: tt.wantName}, clause)
			require.False(t, clause.IsDynamic())
			require.Equal(t, tt.wantName, clause.Text())
			require.Equal(t, tt.wantRemaining, remaining)
		})
	}
}

// TestParseUseClause_NoUseClause pins that a statement not starting with the
// word USE is returned unchanged with hasUse false, including a name that
// merely starts with the letters USE.
func TestParseUseClause_NoUseClause(t *testing.T) {
	for _, query := range []string{"MATCH (n) RETURN n", "  RETURN 1", "USERS", "user_count RETURN 1"} {
		clause, remaining, hasUse, err := ParseUseClause(query, false)
		require.NoError(t, err, query)
		require.False(t, hasUse, query)
		require.Equal(t, UseClause{}, clause, query)
		require.Equal(t, query, remaining, query)
	}
}

// TestParseUseClause_DynamicGraphReferences pins that a function call after
// USE is a dynamic graph reference: the function name without the spaces
// around its dots, the arguments split at top-level commas only (not inside
// calls, lists, maps or string literals), and Text() printing the call as
// Neo4j does in messages, with single-quoted strings in double quotes.
func TestParseUseClause_DynamicGraphReferences(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		wantFunction  string
		wantArgs      []string
		wantText      string
		wantRemaining string
	}{
		{
			name:          "graph.byName with a string literal",
			query:         "USE graph.byName('cmp.a') MATCH (n) RETURN n",
			wantFunction:  "graph.byName",
			wantArgs:      []string{"'cmp.a'"},
			wantText:      `graph.byName("cmp.a")`,
			wantRemaining: "MATCH (n) RETURN n",
		},
		{
			name:          "spaces around the dot and the parentheses",
			query:         "USE graph . byName ( $g ) RETURN 1",
			wantFunction:  "graph.byName",
			wantArgs:      []string{"$g"},
			wantText:      "graph.byName($g)",
			wantRemaining: "RETURN 1",
		},
		{
			name:          "graph.byElementId with a variable",
			query:         "USE graph.byElementId(x) RETURN 1",
			wantFunction:  "graph.byElementId",
			wantArgs:      []string{"x"},
			wantText:      "graph.byElementId(x)",
			wantRemaining: "RETURN 1",
		},
		{
			name:          "no arguments",
			query:         "USE graph.byName() RETURN 1",
			wantFunction:  "graph.byName",
			wantArgs:      nil,
			wantText:      "graph.byName()",
			wantRemaining: "RETURN 1",
		},
		{
			name:          "commas nested in calls, lists, maps and strings do not split",
			query:         "USE f(g(a, b), [1, 2], {k: 'v,w'}, 'x)y') RETURN 1",
			wantFunction:  "f",
			wantArgs:      []string{"g(a, b)", "[1, 2]", "{k: 'v,w'}", "'x)y'"},
			wantText:      `f(g(a, b), [1, 2], {k: "v,w"}, "x)y")`,
			wantRemaining: "RETURN 1",
		},
		{
			name:          "backtick names and double-quoted strings print as written",
			query:         "USE f(`a``b` + 'x', \"b\" + 'c\"d') RETURN 1",
			wantFunction:  "f",
			wantArgs:      []string{"`a``b` + 'x'", `"b" + 'c"d'`},
			wantText:      "f(`a``b` + \"x\", \"b\" + \"c\\\"d\")",
			wantRemaining: "RETURN 1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clause, remaining, hasUse, err := ParseUseClause(tt.query, false)
			require.NoError(t, err)
			require.True(t, hasUse)
			require.True(t, clause.IsDynamic())
			require.Empty(t, clause.Name)
			require.Equal(t, tt.wantFunction, clause.Function)
			require.Equal(t, tt.wantArgs, clause.Args)
			require.Equal(t, tt.wantText, clause.Text())
			require.Equal(t, tt.wantRemaining, remaining)
		})
	}
}

// TestParseUseClause_DynamicArgumentWithEscapedQuote pins that a backslash
// escape inside a string argument does not end the literal, so the ')'
// after it still closes the call.
func TestParseUseClause_DynamicArgumentWithEscapedQuote(t *testing.T) {
	clause, remaining, hasUse, err := ParseUseClause(`USE graph.byName('it\'s)') RETURN 1`, false)
	require.NoError(t, err)
	require.True(t, hasUse)
	require.Equal(t, "graph.byName", clause.Function)
	require.Equal(t, []string{`'it\'s)'`}, clause.Args)
	require.Equal(t, "RETURN 1", remaining)
}

// TestUseClauseText_UnterminatedLiteral pins that Text() prints an argument
// whose string literal never closes as written instead of dropping it. The
// parser never produces such an argument (the call's closing parenthesis is
// only found outside string literals), so the clause is built directly.
func TestUseClauseText_UnterminatedLiteral(t *testing.T) {
	clause := UseClause{Function: "graph.byName", Args: []string{"x + 'abc"}}
	require.Equal(t, "graph.byName(x + 'abc)", clause.Text())
}

// TestParseUseClause_SyntaxErrors pins Neo4j 5.26's SyntaxError for each
// malformed USE clause: the localized message (ID and data) and its Neo4j
// English text, with the reported input token.
func TestParseUseClause_SyntaxErrors(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		inSubquery bool
		want       localization.Message
		wantText   string
	}{
		{name: "USE with nothing after it", query: "USE",
			want:     localization.CypherCommandRoutingUseInvalidGraphReference(""),
			wantText: "Invalid input '': expected an identifier, '(' or 'GRAPH'"},
		{name: "name starting with a digit", query: "USE 1abc RETURN 1",
			want:     localization.CypherCommandRoutingUseInvalidGraphReference("1abc"),
			wantText: "Invalid input '1abc': expected an identifier, '(' or 'GRAPH'"},
		{name: "unterminated quoted name", query: "USE `abc RETURN 1",
			want:     localization.CypherCommandRoutingUseInvalidGraphReference("`"),
			wantText: "Invalid input '`': expected an identifier, '(' or 'GRAPH'"},
		{name: "name part starting with a digit", query: "USE a.1b RETURN 1",
			want:     localization.CypherCommandRoutingUseInvalidGraphNamePart("1b"),
			wantText: "Invalid input '1b': expected a database name or an identifier"},
		{name: "name part that is punctuation", query: "USE a.-b RETURN 1",
			want:     localization.CypherCommandRoutingUseInvalidGraphNamePart("-"),
			wantText: "Invalid input '-': expected a database name or an identifier"},
		{name: "name ending in a dot", query: "USE a.",
			want:     localization.CypherCommandRoutingUseInvalidGraphNamePart(""),
			wantText: "Invalid input '': expected a database name or an identifier"},
		{name: "function call never closed", query: "USE graph.byName('abc RETURN 1",
			want:     localization.CypherCommandRoutingUseInvalidGraphFunctionArgument(""),
			wantText: "Invalid input '': expected an expression, ')' or ','"},
		{name: "no clause after the graph", query: "USE db",
			want:     localization.CypherCommandRoutingUseQueryCannotConclude(),
			wantText: "Query cannot conclude with USE GRAPH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD)."},
		{name: "only a semicolon after the graph", query: "USE db;",
			want: localization.CypherCommandRoutingUseQueryCannotConclude()},
		{name: "only semicolons and spaces after the graph", query: "USE db ; ;",
			want: localization.CypherCommandRoutingUseQueryCannotConclude()},
		{name: "only a line comment after the graph", query: "USE db // nothing else",
			want: localization.CypherCommandRoutingUseQueryCannotConclude()},
		{name: "only an unterminated block comment after the graph", query: "USE db /* nothing else",
			want: localization.CypherCommandRoutingUseQueryCannotConclude()},
		{name: "subquery with no clause after the graph", query: "USE db", inSubquery: true,
			want:     localization.CypherCommandRoutingUseSubqueryMustConclude(),
			wantText: "Query must conclude with a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD."},
		{name: "EXPLAIN after the graph", query: "USE db EXPLAIN MATCH (n) RETURN n",
			want: localization.CypherCommandRoutingUseInvalidClauseAfterGraph("EXPLAIN")},
		{name: "number after the graph", query: "USE db 1 RETURN 1",
			want: localization.CypherCommandRoutingUseInvalidClauseAfterGraph("1")},
		{name: ":USE after the graph", query: "USE db :USE other RETURN 1",
			want: localization.CypherCommandRoutingUseInvalidClauseAfterGraph(":")},
		{name: "administration word in a subquery", query: "USE db SHOW USERS", inSubquery: true,
			want: localization.CypherCommandRoutingUseInvalidSubqueryClauseAfterGraph("SHOW")},
		{name: "punctuation in a subquery", query: "USE db ,", inSubquery: true,
			want: localization.CypherCommandRoutingUseInvalidSubqueryClauseAfterGraph(",")},
		{name: "second USE", query: "USE db USE other RETURN 1",
			want:     localization.CypherCommandRoutingUseNotFirstClause(),
			wantText: "USE clause must be either the first clause in a (sub-)query or preceded by an importing WITH clause in a sub-query."},
		{name: "second USE in a subquery", query: "USE db use other RETURN 1", inSubquery: true,
			want: localization.CypherCommandRoutingUseNotFirstClause()},
		{name: "SHOW USERS after USE", query: "USE system SHOW USERS",
			want:     localization.CypherCommandRoutingUseAdministrationCommand(),
			wantText: "The `USE` clause is not required for Administration Commands. Retry your query omitting the `USE` clause and it will be routed automatically."},
		{name: "CREATE DATABASE after USE", query: "USE system CREATE DATABASE foo",
			want: localization.CypherCommandRoutingUseAdministrationCommand()},
		{name: "GRANT after USE", query: "USE system GRANT ROLE r TO u",
			want: localization.CypherCommandRoutingUseAdministrationCommand()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clause, remaining, hasUse, err := ParseUseClause(tt.query, tt.inSubquery)
			require.Error(t, err)
			require.True(t, hasUse)
			require.Equal(t, UseClause{}, clause)
			require.Empty(t, remaining)
			var syntaxErr *UseSyntaxError
			require.True(t, errors.As(err, &syntaxErr))
			require.Equal(t, tt.want, syntaxErr.Message)
			require.Equal(t, tt.want.Fallback, err.Error())
			if tt.wantText != "" {
				require.Equal(t, tt.wantText, err.Error())
			}
		})
	}
}

// TestParseUseClause_InvalidFollowerMessages pins the start of Neo4j's
// "expected …" lists: the top-level list offers administration commands
// (ALTER, START DATABASE, …) and ends with <EOF>; the subquery list does
// not and ends with '}'.
func TestParseUseClause_InvalidFollowerMessages(t *testing.T) {
	_, _, _, err := ParseUseClause("USE db EXPLAIN MATCH (n) RETURN n", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Invalid input 'EXPLAIN': expected a database name, '(', 'FOREACH', '.', 'ALTER', 'ORDER BY'")
	require.Contains(t, err.Error(), "'USE', 'WITH' or <EOF>")

	_, _, _, err = ParseUseClause("USE db EXPLAIN MATCH (n) RETURN n", true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Invalid input 'EXPLAIN': expected a database name, '(', 'FOREACH', '.', 'ORDER BY'")
	require.NotContains(t, err.Error(), "'ALTER'")
	require.Contains(t, err.Error(), "'USE', 'WITH' or '}'")
}

// TestIsAdministrationCommand pins which statements count as Neo4j
// administration commands (refused after USE) and which are ordinary
// queries that share their first word (SHOW INDEXES, CREATE INDEX,
// CREATE (n), SHOW USER DEFINED FUNCTIONS, …).
func TestIsAdministrationCommand(t *testing.T) {
	tests := []struct {
		statement string
		want      bool
	}{
		{"", false},
		{"   ", false},
		{"(n)", false},
		{"MATCH (n) RETURN n", false},
		{"GRANT ROLE r TO u", true},
		{"deny read {*} on graph * to r", true},
		{"REVOKE ROLE r FROM u", true},
		{"RENAME USER a TO b", true},
		{"START DATABASE foo", true},
		{"STOP DATABASE foo", true},
		{"ENABLE SERVER 'abc'", true},
		{"DRYRUN REALLOCATE DATABASES", true},
		{"DEALLOCATE DATABASES FROM SERVER 'a'", true},
		{"REALLOCATE DATABASES", true},
		{"// leading comment\nGRANT ROLE r TO u", true},
		{"SHOW", false},
		{"SHOW DATABASE foo", true},
		{"show databases", true},
		{"SHOW DEFAULT DATABASE", true},
		{"SHOW HOME DATABASE", true},
		{"SHOW SERVER 'a'", true},
		{"SHOW SERVERS", true},
		{"SHOW SUPPORTED PRIVILEGES", true},
		{"SHOW POPULATED ROLES", true},
		{"SHOW USERS", true},
		{"SHOW CURRENT USER", true},
		{"SHOW ROLE r PRIVILEGES", true},
		{"SHOW ROLES", true},
		{"SHOW PRIVILEGE", true},
		{"SHOW PRIVILEGES", true},
		{"SHOW ALIAS a FOR DATABASE", true},
		{"SHOW ALIASES FOR DATABASES", true},
		{"SHOW USER", true},
		{"SHOW USER bob PRIVILEGES", true},
		{"SHOW USER DEFINED FUNCTIONS", false},
		{"SHOW ALL", false},
		{"SHOW ALL ROLES", true},
		{"SHOW ALL ROLE", true},
		{"SHOW ALL PRIVILEGES", true},
		{"SHOW ALL PRIVILEGE", true},
		{"SHOW ALL FUNCTIONS", false},
		{"SHOW INDEXES", false},
		{"SHOW PROCEDURES", false},
		{"SHOW TRANSACTIONS", false},
		{"CREATE", false},
		{"CREATE (n) RETURN n", false},
		{"CREATE INDEX idx FOR (n:L) ON (n.p)", false},
		{"CREATE DATABASE foo", true},
		{"create composite database c", true},
		{"CREATE ALIAS a FOR DATABASE foo", true},
		{"CREATE USER u SET PASSWORD 'p'", true},
		{"CREATE ROLE r", true},
		{"CREATE OR REPLACE DATABASE foo", true},
		{"CREATE OR REPLACE", false},
		{"CREATE OR REPLACE INDEX", false},
		{"CREATE CURRENT USER", false},
		{"DROP DATABASE foo", true},
		{"DROP SERVER 'a'", true},
		{"DROP INDEX idx", false},
		{"DROP CONSTRAINT c", false},
		{"ALTER USER u SET PASSWORD 'p'", true},
		{"ALTER CURRENT USER SET PASSWORD FROM 'a' TO 'b'", true},
		{"ALTER CURRENT", false},
		{"ALTER CURRENT ROLE", false},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, IsAdministrationCommand(tt.statement), tt.statement)
	}
}
