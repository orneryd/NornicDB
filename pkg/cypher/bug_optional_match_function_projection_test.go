package cypher

// Regression tests for silent projection corruption in
// executeCompoundMatchOptionalMatch's traversal branch (clauses.go).
//
// When the primary MATCH contains a relationship pattern and the query has one
// or more trailing OPTIONAL MATCH clauses (no WITH), the RETURN projection is
// evaluated by resolveReturnExprFromVarMap, which only understands "var.prop"
// and bare variable references. Every other expression falls through to
// parseValue, which returns the raw expression TEXT as the column value:
//
//	RETURN type(rel)              -> the literal string "type(rel)"
//	RETURN coalesce(t.id, t.uid)  -> the literal string "coalesce(t.id, t.uid)"
//	RETURN labels(t)              -> the literal string "labels(t)"
//	RETURN count(f)               -> the literal string "count(f)"
//	RETURN rel.weight             -> the literal string "rel.weight"
//	                                 (rel is dropped from the seed MATCH:
//	                                 extractNodeVariables skips rel vars)
//
// Plain properties of nodes bound by the primary MATCH and by the FIRST
// OPTIONAL MATCH survive; everything else is corrupted silently. Verified live
// over Bolt against image pr261 (1492458), branch HEAD 7cc8895b, and main
// f5fbdb4e — all identical.
//
// The same query WITHOUT the trailing OPTIONAL MATCH evaluates every one of
// these expressions correctly (see the control test at the bottom).

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newOptProjExecutor(t *testing.T) (*StorageExecutor, context.Context) {
	t.Helper()
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE (c:OMClass {uid:"cls:ServiceDog", name:"ServiceDog", language:"python"})
		CREATE (d:OMClass {uid:"cls:Dog", name:"Dog"})
		CREATE (f:OMFile {uid:"file:svc", relative_path:"svc.py", language:"python"})
		CREATE (r:OMRepo {id:"repo:1", name:"repo1"})
		CREATE (c)-[:INHERITS {weight: 2}]->(d)
		CREATE (f)-[:CONTAINS]->(c)
		CREATE (f)-[:CONTAINS]->(d)
		CREATE (r)-[:REPO_CONTAINS]->(f)
	`, nil)
	require.NoError(t, err)
	return exec, ctx
}

// rowMap converts one result row into a column-name -> value map.
func rowMap(t *testing.T, res *ExecuteResult, i int) map[string]interface{} {
	t.Helper()
	require.Greater(t, len(res.Rows), i)
	m := make(map[string]interface{}, len(res.Columns))
	for c, col := range res.Columns {
		m[col] = res.Rows[i][c]
	}
	return m
}

// TestBug_TrailingOptionalMatch_FunctionProjectionCorrupted captures the core
// defect: function-call expressions in RETURN come back as their literal
// source text when a trailing OPTIONAL MATCH follows a relationship-bound
// MATCH.
func TestBug_TrailingOptionalMatch_FunctionProjectionCorrupted(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)

	res, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(tf:OMFile)
		RETURN type(rel) AS relType,
		       coalesce(target.id, target.uid) AS targetId,
		       labels(target) AS targetLabels,
		       target.name AS targetName,
		       tf.relative_path AS filePath
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	row := rowMap(t, res, 0)

	// Sanity: the plain properties that DO survive today.
	require.Equal(t, "Dog", row["targetName"])
	require.Equal(t, "svc.py", row["filePath"])

	// The actual defect: these must be evaluated, not echoed as source text.
	require.Equal(t, "INHERITS", row["relType"],
		"type(rel) must be evaluated, not returned as literal expression text")
	require.Equal(t, "cls:Dog", row["targetId"],
		"coalesce(target.id, target.uid) must be evaluated, not returned as literal expression text")
	require.Equal(t, []interface{}{"OMClass"}, row["targetLabels"],
		"labels(target) must be evaluated, not returned as literal expression text")
}

// TestBug_TrailingOptionalMatch_RelVarDropped captures the second facet: the
// primary MATCH's relationship variable is not carried into the projection
// scope (extractNodeVariables drops it from the seed MATCH), so rel.prop and
// the bare rel variable corrupt too.
func TestBug_TrailingOptionalMatch_RelVarDropped(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)

	res, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(tf:OMFile)
		RETURN rel.weight AS w, target.name AS targetName
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	row := rowMap(t, res, 0)

	require.Equal(t, "Dog", row["targetName"])
	require.EqualValues(t, int64(2), row["w"],
		"rel.weight must resolve the primary MATCH relationship property, not the literal string \"rel.weight\"")
}

// TestBug_TrailingOptionalMatch_AggregateCorrupted captures the third facet:
// aggregation functions after the traversal-seeded OPTIONAL MATCH are echoed
// as literal text instead of aggregating (the traversal branch never routes to
// the aggregation path).
func TestBug_TrailingOptionalMatch_AggregateCorrupted(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)

	res, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(tf:OMFile)
		RETURN target.name AS targetName, count(tf) AS fileCount
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	row := rowMap(t, res, 0)

	require.Equal(t, "Dog", row["targetName"])
	require.EqualValues(t, int64(1), row["fileCount"],
		"count(tf) must aggregate, not return the literal string \"count(tf)\"")
}

// TestBug_TrailingOptionalMatch_ChainedOptionalBindingUnbound captures the
// fourth facet: a variable bound by a SECOND, chained OPTIONAL MATCH is never
// bound (only the first OPTIONAL pattern is parsed), so its property
// projection leaks literal text instead of the value (or null).
func TestBug_TrailingOptionalMatch_ChainedOptionalBindingUnbound(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)

	res, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(tf:OMFile)
		OPTIONAL MATCH (repo:OMRepo)-[:REPO_CONTAINS]->(tf)
		RETURN target.name AS targetName, repo.id AS repoId
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	row := rowMap(t, res, 0)

	require.Equal(t, "Dog", row["targetName"])
	require.Equal(t, "repo:1", row["repoId"],
		"repo.id (bound by the chained second OPTIONAL MATCH) must resolve to the repo id, not the literal string \"repo.id\"")
}

// TestControl_NoOptionalMatch_FunctionProjectionCorrect proves the identical
// RETURN evaluates correctly WITHOUT the trailing OPTIONAL MATCH — isolating
// the corruption to the traversal-seeded OPTIONAL MATCH projection path.
func TestControl_NoOptionalMatch_FunctionProjectionCorrect(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)

	res, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		RETURN type(rel) AS relType,
		       coalesce(target.id, target.uid) AS targetId,
		       labels(target) AS targetLabels,
		       target.name AS targetName
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	row := rowMap(t, res, 0)

	require.Equal(t, "INHERITS", row["relType"])
	require.Equal(t, "cls:Dog", row["targetId"])
	require.Equal(t, []interface{}{"OMClass"}, row["targetLabels"])
	require.Equal(t, "Dog", row["targetName"])
}

// TestFix_TrailingOptionalMatch_WhereOnOptionalClause proves the per-clause
// WHERE predicate is applied (the pre-fix traversal branch silently ignored
// it).
func TestFix_TrailingOptionalMatch_WhereOnOptionalClause(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)

	res, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(tf:OMFile) WHERE tf.language = "go"
		RETURN target.name AS targetName, tf.relative_path AS filePath
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	row := rowMap(t, res, 0)
	require.Equal(t, "Dog", row["targetName"])
	require.Nil(t, row["filePath"],
		"WHERE tf.language = go must reject the python file and leave the optional binding null")
}

// TestFix_TrailingOptionalMatch_UnmatchedOptionalYieldsNullRow proves left
// outer join semantics: a target with no CONTAINS file still produces a row
// with null optional bindings, and functions on main-MATCH bindings evaluate.
func TestFix_TrailingOptionalMatch_UnmatchedOptionalYieldsNullRow(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)
	_, err := exec.Execute(ctx, `
		CREATE (o:OMClass {uid:"cls:Orphan", name:"Orphan"})
		WITH o MATCH (c:OMClass {uid:"cls:ServiceDog"}) CREATE (c)-[:INHERITS]->(o)
	`, nil)
	if err != nil {
		// Fallback seeding without WITH if the CREATE...WITH shape is unsupported.
		_, err = exec.Execute(ctx, `CREATE (o:OMClass {uid:"cls:Orphan", name:"Orphan"})`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `
			MATCH (c:OMClass {uid:"cls:ServiceDog"})
			MATCH (o:OMClass {uid:"cls:Orphan"})
			CREATE (c)-[:INHERITS]->(o)
		`, nil)
		require.NoError(t, err)
	}

	res, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(tf:OMFile)
		RETURN type(rel) AS relType, target.name AS targetName, tf.relative_path AS filePath
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 2, "one row per INHERITS target (Dog matched, Orphan unmatched)")
	byName := map[string]map[string]interface{}{}
	for i := range res.Rows {
		row := rowMap(t, res, i)
		byName[row["targetName"].(string)] = row
	}
	require.Equal(t, "INHERITS", byName["Dog"]["relType"])
	require.Equal(t, "svc.py", byName["Dog"]["filePath"])
	require.Equal(t, "INHERITS", byName["Orphan"]["relType"],
		"type(rel) must evaluate even on the row whose OPTIONAL MATCH found nothing")
	require.Nil(t, byName["Orphan"]["filePath"])
}

// TestFix_TrailingOptionalMatch_OrderByLimit proves ORDER BY and LIMIT apply
// to the traversal-seeded OPTIONAL MATCH projection.
func TestFix_TrailingOptionalMatch_OrderByLimit(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)
	_, err := exec.Execute(ctx, `CREATE (m:OMClass {uid:"cls:Mixin", name:"AMixin"})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
		MATCH (c:OMClass {uid:"cls:ServiceDog"})
		MATCH (m:OMClass {uid:"cls:Mixin"})
		CREATE (c)-[:INHERITS]->(m)
	`, nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(tf:OMFile)
		RETURN target.name AS targetName ORDER BY targetName LIMIT 1
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "AMixin", res.Rows[0][0],
		"ORDER BY targetName ascending must sort AMixin before Dog, LIMIT 1 keeps it")
}

// TestFix_TrailingOptionalMatch_GroupedAggregate proves implicit grouping:
// count(tf) grouped by target name with correct per-group counts.
func TestFix_TrailingOptionalMatch_GroupedAggregate(t *testing.T) {
	exec, ctx := newOptProjExecutor(t)
	_, err := exec.Execute(ctx, `CREATE (f2:OMFile {uid:"file:extra", relative_path:"extra.py"})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
		MATCH (d:OMClass {uid:"cls:Dog"})
		MATCH (f2:OMFile {uid:"file:extra"})
		CREATE (f2)-[:CONTAINS]->(d)
	`, nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `
		MATCH (e:OMClass {uid:"cls:ServiceDog"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(tf:OMFile)
		RETURN target.name AS targetName, count(tf) AS fileCount
	`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	row := rowMap(t, res, 0)
	require.Equal(t, "Dog", row["targetName"])
	require.EqualValues(t, int64(2), row["fileCount"],
		"Dog is contained in two files; count(tf) must aggregate both joined rows")
}
