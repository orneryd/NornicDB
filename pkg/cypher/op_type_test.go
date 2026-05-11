// Package cypher — table-driven test for the RISK-1 corrected op_type
// classifier. Plan 04-03-02 / CONTEXT D-04 (per RESEARCH RISK-1).
//
// The original CONTEXT D-04 wording named a `plan.Root.Op` chokepoint that
// does NOT exist on the normal-path executor (*ExecutionPlan is built only
// on EXPLAIN/PROFILE). The corrected classifier reads *QueryInfo from
// QueryAnalyzer.Analyze() — the actual normal-path classifier surface.
//
// D-04c demands a table-driven test that covers every clause shape the
// QueryAnalyzer recognizes; this file is that test. Adding a new clause
// type later that does not classify will fall through to the safe `read`
// default; a new table row added here will catch it (AGENTS.md §9
// belt-and-suspenders).
package cypher

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestOpType_AllClauseShapes drives the QueryAnalyzer over every clause
// shape and asserts classifyOpType returns the closed-enum value the metric
// catalog expects. Mirrors AllowedCypherOpTypes in pkg/observability.
func TestOpType_AllClauseShapes(t *testing.T) {
	type tc struct {
		name     string
		query    string
		isFabric bool
		isAdmin  bool
		want     string
	}

	a := NewQueryAnalyzer(64)

	cases := []tc{
		// ----- read -----
		{name: "match_return", query: "MATCH (n) RETURN n", want: "read"},
		{name: "optional_match", query: "OPTIONAL MATCH (n) RETURN n", want: "read"},
		{name: "match_with_where", query: "MATCH (n:User) WHERE n.age > 21 RETURN n.name", want: "read"},
		{name: "with_unwind", query: "UNWIND [1,2,3] AS x RETURN x", want: "read"},
		{name: "call_db_introspection", query: "CALL db.labels()", want: "read"},

		// ----- write -----
		{name: "create_node", query: "CREATE (n:User {name: 'Alice'})", want: "write"},
		{name: "merge_node", query: "MERGE (n:User {id: 1})", want: "write"},
		{name: "delete_node", query: "MATCH (n) DELETE n", want: "write"},
		{name: "detach_delete", query: "MATCH (n) DETACH DELETE n", want: "write"},
		{name: "set_property", query: "MATCH (n) SET n.x = 1", want: "write"},
		{name: "remove_property", query: "MATCH (n) REMOVE n.x", want: "write"},

		// ----- schema -----
		{name: "create_index", query: "CREATE INDEX my_idx FOR (n:User) ON (n.id)", want: "schema"},
		{name: "create_range_index", query: "CREATE RANGE INDEX rng FOR (n:User) ON (n.age)", want: "schema"},
		{name: "create_fulltext_index", query: "CREATE FULLTEXT INDEX ft FOR (n:Document) ON EACH [n.text]", want: "schema"},
		{name: "create_vector_index", query: "CREATE VECTOR INDEX vec FOR (n:Document) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3}}", want: "schema"},
		{name: "drop_index", query: "DROP INDEX my_idx", want: "schema"},
		{name: "create_constraint", query: "CREATE CONSTRAINT FOR (n:User) REQUIRE n.id IS UNIQUE", want: "schema"},
		{name: "drop_constraint", query: "DROP CONSTRAINT cs1", want: "schema"},
		{name: "show_indexes", query: "SHOW INDEXES", want: "schema"},

		// ----- admin -----
		// Admin dispatch happens at Site 1 in executor.go BEFORE Analyze() is
		// called for SHOW/CREATE/DROP DATABASE etc. The classifier honors
		// the isAdmin flag explicitly; QueryInfo for SHOW DATABASES would
		// still classify as "schema" (HasShow → IsSchemaQuery), but the
		// Site-1 caller passes isAdmin=true to override.
		{name: "show_databases_admin", query: "SHOW DATABASES", isAdmin: true, want: "admin"},
		{name: "create_database_admin", query: "CREATE DATABASE neo4j", isAdmin: true, want: "admin"},
		{name: "drop_database_admin", query: "DROP DATABASE neo4j", isAdmin: true, want: "admin"},
		{name: "alter_database_admin", query: "ALTER DATABASE neo4j", isAdmin: true, want: "admin"},
		// admin overrides everything (defensive: even if isFabric is also set):
		{name: "admin_overrides_fabric", query: "SHOW DATABASES", isAdmin: true, isFabric: true, want: "admin"},

		// ----- fabric -----
		{name: "fabric_use_clause", query: "USE composite.constituent MATCH (n) RETURN n", isFabric: true, want: "fabric"},
		{name: "fabric_call_use", query: "CALL { USE composite MATCH (n) RETURN n }", isFabric: true, want: "fabric"},
		// fabric overrides classification when isAdmin is false:
		{name: "fabric_with_write", query: "USE composite CREATE (n)", isFabric: true, want: "fabric"},

		// ----- parse_error / nil-info defensive -----
		// Site 2 (validateSyntax err path) emits parse_error directly —
		// the classifier's nil-info defensive branch is the safety net.
		{name: "nil_info_parse_error", query: "", want: "parse_error"},
	}

	for _, tcase := range cases {
		t.Run(tcase.name, func(t *testing.T) {
			var info *QueryInfo
			if tcase.query != "" && tcase.want != "parse_error" {
				info = a.Analyze(tcase.query)
			}
			got := classifyOpType(info, tcase.isFabric, tcase.isAdmin)
			assert.Equal(t, tcase.want, got,
				"query=%q isFabric=%v isAdmin=%v", tcase.query, tcase.isFabric, tcase.isAdmin)
		})
	}
}

// TestClassifyOpType_NilInfo asserts the defensive nil-info branch returns
// parse_error — Site 2 (validateSyntax err) callers should emit parse_error
// directly, but the classifier provides defense-in-depth.
func TestClassifyOpType_NilInfo(t *testing.T) {
	got := classifyOpType(nil, false, false)
	assert.Equal(t, "parse_error", got,
		"nil QueryInfo → parse_error (defensive; validateSyntax err callers emit directly)")
}

// TestClassifyOpType_AllReturnsAreClosedEnum asserts the classifier
// NEVER returns a value outside the closed AllowedCypherOpTypes enum,
// regardless of QueryInfo flag combinations. Belt-and-suspenders against
// AGENTS.md §9: a misclassification falls back to "read" (the safe default
// — non-write, non-schema), which is still in the enum.
func TestClassifyOpType_AllReturnsAreClosedEnum(t *testing.T) {
	closedEnum := map[string]struct{}{
		"read":        {},
		"write":       {},
		"schema":      {},
		"admin":       {},
		"fabric":      {},
		"parse_error": {},
	}

	a := NewQueryAnalyzer(8)
	queries := []string{
		"MATCH (n) RETURN n",
		"CREATE (n)",
		"MERGE (n)",
		"DELETE n",
		"SET n.x = 1",
		"REMOVE n.x",
		"CREATE INDEX i FOR (n:U) ON (n.id)",
		"DROP INDEX i",
		"SHOW DATABASES",
		"SHOW INDEXES",
		"CALL db.labels()",
		"UNWIND [1,2,3] AS x RETURN x",
		"WITH 1 AS x RETURN x",
	}

	for _, q := range queries {
		for _, isAdmin := range []bool{false, true} {
			for _, isFabric := range []bool{false, true} {
				info := a.Analyze(q)
				got := classifyOpType(info, isFabric, isAdmin)
				_, ok := closedEnum[got]
				assert.True(t, ok,
					"classifyOpType(%q, isFabric=%v, isAdmin=%v) = %q; not in closed enum",
					q, isFabric, isAdmin, got)
			}
		}
	}
}

// TestClassifyOpType_AdminPrecedence asserts the precedence order:
// admin > fabric > schema > write > read. This is the ordering the
// classifier's if-chain encodes; making it explicit here documents the
// invariant for future maintainers.
func TestClassifyOpType_AdminPrecedence(t *testing.T) {
	a := NewQueryAnalyzer(8)
	info := a.Analyze("MATCH (n) RETURN n") // would normally classify "read"
	assert.Equal(t, "admin", classifyOpType(info, true, true),
		"admin wins over fabric")
	assert.Equal(t, "admin", classifyOpType(info, false, true),
		"admin wins over read")

	infoWrite := a.Analyze("CREATE (n)")
	assert.Equal(t, "fabric", classifyOpType(infoWrite, true, false),
		"fabric wins over write when isAdmin=false")

	infoSchema := a.Analyze("CREATE INDEX i FOR (n:U) ON (n.id)")
	assert.Equal(t, "fabric", classifyOpType(infoSchema, true, false),
		"fabric wins over schema when isAdmin=false")
	assert.Equal(t, "schema", classifyOpType(infoSchema, false, false),
		"schema wins over read")
}
