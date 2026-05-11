// Package cypher — RISK-1 corrected op_type classifier (Plan 04-03-02).
//
// Closed-enum classifier {read, write, schema, admin, fabric, parse_error}
// for the MET-09 op_type label on the Cypher metric catalog
// (pkg/observability/catalog_cypher.go AllowedCypherOpTypes).
//
// RISK-1 correction (RESEARCH RISK-1): the original CONTEXT D-04 wording
// named a `plan.Root.Op` chokepoint that does NOT exist on the normal-path
// executor — `*ExecutionPlan` is built ONLY on EXPLAIN/PROFILE; normal
// Execute uses QueryAnalyzer.Analyze() *QueryInfo. This file's classifier
// reads the actual normal-path classifier surface (*QueryInfo flags), NOT
// the non-existent plan field.
//
// Three observation sites use this classifier (see executor.go):
//
//  1. Admin dispatch (Site 1) — emits op_type="admin" BEFORE Analyze() is
//     called for SHOW DATABASES, CREATE/DROP DATABASE, ALTER DATABASE etc.
//     Caller passes isAdmin=true.
//  2. Parse error (Site 2) — emits op_type="parse_error" at validateSyntax()
//     err path. Caller bypasses this function entirely (or passes nil info)
//     and emits parse_error directly. The defensive nil-info branch below
//     surfaces parse_error as a safety net.
//  3. Normal path (Site 3) — after analyzer.Analyze() returns *QueryInfo,
//     before execute. Caller passes (info, isFabric, false) and the function
//     returns one of read|write|schema|fabric.
//
// AGENTS.md §4 functional pattern: pure function, no side effects, no
// observability imports. Pkg/cypher remains a leaf consumer of pkg/observability;
// the OBSERVATION call lives in executor.go at the chokepoints — this is the
// CLASSIFIER.
package cypher

// classifyOpType maps QueryAnalyzer output + dispatch flags to the closed
// op_type enum {read, write, schema, admin, fabric, parse_error}.
//
// Precedence (encoded in the if-chain order):
//
//	admin (isAdmin)        → site-1 dispatch override
//	fabric (isFabric)      → USE-clause routing override
//	parse_error (info nil) → defensive nil-info safety net (Site 2 callers
//	                          should emit parse_error directly)
//	schema (IsSchemaQuery) → CREATE/DROP INDEX, SHOW INDEXES, etc.
//	write (IsWriteQuery)   → CREATE/MERGE/DELETE/SET/REMOVE
//	read (IsReadOnly)      → MATCH/RETURN/UNWIND/CALL db.*
//	read (default)         → safe default; closed enum NEVER returns outside
//	                          AllowedCypherOpTypes (TestClassifyOpType_AllReturnsAreClosedEnum)
//
// The default-to-read fall-through is the AGENTS.md §9 belt-and-suspenders:
// a new clause type added later that doesn't classify still bucketed safely
// (no cardinality bomb risk; no panic). The TestOpType_AllClauseShapes
// table-driven test catches the misclassification by adding a new row.
func classifyOpType(info *QueryInfo, isFabric, isAdmin bool) string {
	if isAdmin {
		return "admin"
	}
	if isFabric {
		return "fabric"
	}
	if info == nil {
		// Defensive — Site 2 (validateSyntax err) callers SHOULD emit
		// parse_error directly. This nil-info branch is the safety net.
		return "parse_error"
	}
	if info.IsSchemaQuery {
		return "schema"
	}
	if info.IsWriteQuery {
		return "write"
	}
	if info.IsReadOnly {
		return "read"
	}
	// Safe default: a query that classifies as neither write nor schema
	// nor read (e.g., a bare WITH expression) buckets to "read" so the
	// closed enum is never violated.
	return "read"
}
