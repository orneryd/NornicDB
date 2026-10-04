package cypher

import (
	"context"
	"strings"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMonster531IndexAdmission(t *testing.T) {
	for _, testCase := range []struct {
		name, baseline, duplicate, code string
	}{
		{"composite equivalent", "CREATE INDEX idx FOR (n:T) ON (n.a, n.b)", "CREATE INDEX other FOR (n:T) ON (n.a, n.b)", "IndexAlreadyExists"},
		{"relationship equivalent", "CREATE INDEX idx FOR ()-[r:T]-() ON (r.a)", "CREATE INDEX other FOR ()-[r:T]-() ON (r.a)", "IndexAlreadyExists"},
		{"range vector name collision", "CREATE INDEX idx FOR (n:T) ON (n.a)", "CREATE VECTOR INDEX idx FOR (n:T) ON (n.a)", "IndexWithNameAlreadyExists"},
		{"vector range name collision", "CREATE VECTOR INDEX idx FOR (n:T) ON (n.a)", "CREATE INDEX idx FOR (n:T) ON (n.a)", "IndexWithNameAlreadyExists"},
		{"fulltext equivalent", "CREATE FULLTEXT INDEX idx FOR (n:T) ON EACH [n.a]", "CREATE FULLTEXT INDEX other FOR (n:T) ON EACH [n.a]", "IndexAlreadyExists"},
		{"fulltext relationship equivalent", "CREATE FULLTEXT INDEX idx FOR ()-[r:T|U]-() ON EACH [r.a, r.b]", "CREATE FULLTEXT INDEX other FOR ()-[r:T|U]-() ON EACH [r.a, r.b]", "IndexAlreadyExists"},
		{"fulltext text name collision", "CREATE FULLTEXT INDEX idx FOR (n:T) ON EACH [n.a]", "CREATE TEXT INDEX idx FOR (n:T) ON (n.a)", "IndexWithNameAlreadyExists"},
		{"text equivalent", "CREATE TEXT INDEX idx FOR (n:T) ON (n.a)", "CREATE TEXT INDEX other FOR (n:T) ON (n.a)", "IndexAlreadyExists"},
		{"point equivalent", "CREATE POINT INDEX idx FOR (n:T) ON (n.a)", "CREATE POINT INDEX other FOR (n:T) ON (n.a)", "IndexAlreadyExists"},
		{"point conflicting name", "CREATE POINT INDEX idx FOR (n:T) ON (n.a)", "CREATE POINT INDEX idx FOR (n:U) ON (n.b)", "IndexWithNameAlreadyExists"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			executor, store := newTestExecutor(t)
			ctx := context.Background()
			_, err := executor.Execute(ctx, testCase.baseline, nil)
			require.NoError(t, err)
			before := store.GetSchema().GetIndexes()
			_, err = executor.Execute(ctx, testCase.duplicate, nil)
			require.Error(t, err)
			require.Contains(t, statusText(err), "Neo.ClientError.Schema."+testCase.code)
			require.ElementsMatch(t, before, store.GetSchema().GetIndexes())
			guarded := strings.Replace(testCase.duplicate, " FOR ", " IF NOT EXISTS FOR ", 1)
			_, err = executor.Execute(ctx, guarded, nil)
			require.NoError(t, err)
			require.ElementsMatch(t, before, store.GetSchema().GetIndexes())
		})
	}
	executor, store := newTestExecutor(t)
	ctx := context.Background()
	_, err := executor.Execute(ctx, "CREATE INDEX node_index FOR (n:T) ON (n.a)", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "CREATE INDEX relationship_index FOR ()-[r:T]-() ON (r.a)", nil)
	require.NoError(t, err)
	require.Len(t, userIndexes(store.GetSchema().GetIndexes()), 2)
}

func TestMonster531ConstraintBackingIndexAdmission(t *testing.T) {
	executor, store := newTestExecutor(t)
	ctx := context.Background()
	_, err := executor.Execute(ctx, "CREATE CONSTRAINT backing FOR (n:Doc) REQUIRE n.id IS UNIQUE", nil)
	require.NoError(t, err)
	before := store.GetSchema().GetIndexes()
	_, err = executor.Execute(ctx, "CREATE INDEX other FOR (n:Doc) ON (n.id)", nil)
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Schema.ConstraintAlreadyExists")
	require.ElementsMatch(t, before, store.GetSchema().GetIndexes())
	_, err = executor.Execute(ctx, "CREATE INDEX other IF NOT EXISTS FOR (n:Doc) ON (n.id)", nil)
	require.NoError(t, err)
	require.ElementsMatch(t, before, store.GetSchema().GetIndexes())
}

func TestMonster531NonBackingConstraintIndexName(t *testing.T) {
	executor, store := newTestExecutor(t)
	ctx := context.Background()
	_, err := executor.Execute(ctx, "CREATE CONSTRAINT taken FOR (n:Doc) REQUIRE n.id IS NOT NULL", nil)
	require.NoError(t, err)
	beforeIndexes := store.GetSchema().GetIndexes()
	beforeConstraints := store.GetSchema().GetAllConstraints()
	_, err = executor.Execute(ctx, "CREATE INDEX taken FOR (n:Other) ON (n.value)", nil)
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Schema.ConstraintWithNameAlreadyExists")
	require.ElementsMatch(t, beforeIndexes, store.GetSchema().GetIndexes())
	require.Equal(t, beforeConstraints, store.GetSchema().GetAllConstraints())
	_, err = executor.Execute(ctx, "CREATE INDEX taken IF NOT EXISTS FOR (n:Other) ON (n.value)", nil)
	require.NoError(t, err)
	require.ElementsMatch(t, beforeIndexes, store.GetSchema().GetIndexes())
}

func TestMonster531CompositeNodeUnique(t *testing.T) {
	for _, query := range []string{
		"CREATE CONSTRAINT cu1 FOR (n:CU) REQUIRE (n.a, n.b) IS UNIQUE",
		"CREATE CONSTRAINT FOR (n:CU) REQUIRE (n.a, n.b) IS UNIQUE",
		"CREATE CONSTRAINT cu1 IF NOT EXISTS FOR (n:CU) REQUIRE (n.a, n.b) IS UNIQUE",
		"CREATE CONSTRAINT cu1 FOR (`n`:CU) REQUIRE (`n`.`a`, `n`.`b`) IS UNIQUE",
	} {
		t.Run(query, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "monster531")
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			_, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			constraints := store.GetSchema().GetAllConstraints()
			require.Len(t, constraints, 1)
			require.Equal(t, []string{"a", "b"}, constraints[0].Properties)
			_, err = exec.Execute(ctx, "CREATE (:CU {a: 1, b: 2}), (:CU {a: 1, b: 3}), (:CU {a: 1}), (:CU {a: 1})", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "CREATE (:CU {a: 1, b: 2})", nil)
			require.Error(t, err)
			result, err := exec.Execute(ctx, "MATCH (n:CU) RETURN count(n)", nil)
			require.NoError(t, err)
			require.Equal(t, int64(4), result.Rows[0][0])
		})
	}
}

func TestMonster531DuplicateConstraintStatus(t *testing.T) {
	for _, testCase := range []struct {
		query string
		code  string
	}{
		{"CREATE CONSTRAINT dupc FOR (n:T) REQUIRE n.u IS UNIQUE", "Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists"},
		{"CREATE CONSTRAINT other FOR (n:T) REQUIRE n.u IS UNIQUE", "Neo.ClientError.Schema.ConstraintAlreadyExists"},
		{"CREATE CONSTRAINT dupc FOR (n:U) REQUIRE n.v IS UNIQUE", "Neo.ClientError.Schema.ConstraintWithNameAlreadyExists"},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			exec, store := newTestExecutor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE CONSTRAINT dupc FOR (n:T) REQUIRE n.u IS UNIQUE", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, testCase.query, nil)
			require.Error(t, err)
			require.Contains(t, statusText(err), testCase.code)
			require.Len(t, store.GetSchema().GetAllConstraints(), 1)
		})
	}
}

func TestMonster531RejectLegacyConstraintSyntax(t *testing.T) {
	for _, query := range []string{
		"CREATE CONSTRAINT mykey ON (n:L) ASSERT (n.a) IS NODE KEY",
		"CREATE CONSTRAINT myuniq ON (n:L) ASSERT n.b IS UNIQUE",
	} {
		t.Run(query, func(t *testing.T) {
			exec, store := newTestExecutor(t)
			_, err := exec.Execute(context.Background(), query, nil)
			require.Error(t, err)
			require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError")
			require.Empty(t, store.GetSchema().GetAllConstraints())
		})
	}
}

func TestMonster531RejectMalformedCompositeUnique(t *testing.T) {
	for _, testCase := range []struct {
		pattern   string
		predicate string
	}{
		{"(n:CU)", "(n.a, garbage)"},
		{"(n:CU)", "(n.a, m.b)"},
		{"(n:CU)", "(m.a, m.b)"},
		{"(n:CU)", "(n.a, n.b + 1)"},
		{"(n:CU)", "(n.a, n.b,)"},
		{"()-[r:CU]-()", "(r.a, s.b)"},
		{"()-[r:CU]-()", "(r.a, r.b + 1)"},
	} {
		t.Run(testCase.pattern+testCase.predicate, func(t *testing.T) {
			exec, store := newTestExecutor(t)
			_, err := exec.Execute(context.Background(), "CREATE CONSTRAINT cu1 FOR "+testCase.pattern+" REQUIRE "+testCase.predicate+" IS UNIQUE", nil)
			require.Error(t, err)
			require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError")
			require.Empty(t, store.GetSchema().GetAllConstraints())
		})
	}
}

func TestMonster531TextAndPointIndexAdmission(t *testing.T) {
	for _, testCase := range []struct {
		kind     string
		name     string
		label    string
		property string
	}{
		{"TEXT", "s530_text", "S530", "t"},
		{"POINT", "s530_point", "S530", "p"},
		{"TEXT", "ti", "P", "name"},
		{"POINT", "pi", "P", "loc"},
	} {
		t.Run(testCase.kind+"/"+testCase.label, func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE "+testCase.kind+" INDEX "+testCase.name+" FOR (n:"+testCase.label+") ON (n."+testCase.property+")", nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, "SHOW INDEXES YIELD name, type, properties WHERE name = '"+testCase.name+"' RETURN type, properties", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{testCase.kind, []string{testCase.property}}}, result.Rows)
		})
	}
}

func TestMonster531CompositeUniqueCreationAtomicity(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "monster531_atomic")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:CU {a: 1, b: 2}), (:CU {a: 1, b: 2})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE CONSTRAINT cu1 FOR (n:CU) REQUIRE (n.a, n.b) IS UNIQUE", nil)
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.DatabaseError.Schema.ConstraintCreationFailed")
	require.Empty(t, store.GetSchema().GetAllConstraints())
	indexes, err := exec.Execute(ctx, "SHOW INDEXES YIELD name, type", nil)
	require.NoError(t, err)
	require.Len(t, indexes.Rows, 2)
	for _, row := range indexes.Rows {
		require.Equal(t, "LOOKUP", row[1])
	}
}

func TestMonster531TypedIndexDDLMatrix(t *testing.T) {
	for _, query := range []string{
		"CREATE TEXT INDEX FOR (n:T) ON (n.value)",
		"CREATE POINT INDEX FOR ()-[r:T]-() ON (r.value)",
		"CREATE TEXT INDEX rel_text FOR ()-[r:T]-() ON (r.value)",
	} {
		t.Run(query, func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, "SHOW INDEXES YIELD name, type, createStatement WHERE type <> 'LOOKUP' RETURN name, type, createStatement", nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			_, err = exec.Execute(ctx, "DROP INDEX "+quoteSchemaName(result.Rows[0][0].(string)), nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, result.Rows[0][2].(string), nil)
			require.NoError(t, err)
		})
	}
	for _, query := range []string{
		"CREATE TEXT INDEX bad FOR (n:T) ON (n.a, n.b)",
		"CREATE POINT INDEX bad FOR (n:T) ON (garbage)",
	} {
		t.Run(query, func(t *testing.T) {
			exec, store := newTestExecutor(t)
			_, err := exec.Execute(context.Background(), query, nil)
			require.Error(t, err)
			require.Len(t, store.GetSchema().GetIndexes(), 2)
		})
	}
}

func TestResidualRepeatedUniqueConstraintProperties(t *testing.T) {
	for _, query := range []string{
		"CREATE CONSTRAINT cu FOR (n:P) REQUIRE (n.a, n.a) IS UNIQUE",
		"CREATE CONSTRAINT cu FOR (n:P) REQUIRE (n.a, n.`a`) IS UNIQUE",
		"CREATE CONSTRAINT cu FOR ()-[r:CU]-() REQUIRE (r.a, r.a) IS UNIQUE",
	} {
		t.Run(query, func(t *testing.T) {
			exec, ctx := newUnitExecutor(t)
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Schema.RepeatedPropertyInCompositeSchema", code)
			result, err := exec.Execute(ctx, "SHOW CONSTRAINTS", nil)
			require.NoError(t, err)
			require.Empty(t, result.Rows)
		})
	}
}

func TestCreateConstraint_BranchMatrix_NodeAndRelationshipVariants(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "schema_constraint_matrix_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	queries := []string{
		// Node domain + temporal
		"CREATE CONSTRAINT c_node_domain_named IF NOT EXISTS FOR (n:NodeDomA) REQUIRE n.status IN ['A','B']",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:NodeDomB) REQUIRE n.status IN ['C','D']",
		"CREATE CONSTRAINT c_node_temp_named IF NOT EXISTS FOR (n:NodeTempA) REQUIRE (n.k, n.valid_from, n.valid_to) IS TEMPORAL NO OVERLAP",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:NodeTempB) REQUIRE (n.k, n.valid_from, n.valid_to) IS TEMPORAL",

		// Node exists/not-null + type variants
		"CREATE CONSTRAINT c_node_exists_named IF NOT EXISTS FOR (n:NodeExistsA) REQUIRE n.email IS NOT NULL",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:NodeExistsB) REQUIRE n.email IS NOT NULL",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:NodeExistsC) ASSERT exists(n.email)",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:NodeExistsD) ASSERT n.email IS NOT NULL",
		"CREATE CONSTRAINT c_node_type_named IF NOT EXISTS FOR (n:NodeTypeA) REQUIRE n.age IS :: INTEGER",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:NodeTypeB) REQUIRE n.age IS TYPED INTEGER",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:NodeTypeC) ASSERT n.age IS :: INTEGER",

		// Relationship domain + temporal
		"CREATE CONSTRAINT c_rel_domain_named IF NOT EXISTS FOR ()-[r:RELDOMA]-() REQUIRE r.state IN ['hot','cold']",
		"CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:RELDOMB]-() REQUIRE r.state IN ['warm','cool']",
		"CREATE CONSTRAINT c_rel_temp_named IF NOT EXISTS FOR ()-[r:RELTEMPA]-() REQUIRE (r.k, r.valid_from, r.valid_to) IS TEMPORAL NO OVERLAP",
		"CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:RELTEMPB]-() REQUIRE (r.k, r.valid_from, r.valid_to) IS TEMPORAL",

		// Relationship key + unique + exists + type
		"CREATE CONSTRAINT c_rel_key_named IF NOT EXISTS FOR ()-[r:RELKEYA]-() REQUIRE (r.a, r.b) IS RELATIONSHIP KEY",
		"CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:RELKEYB]-() REQUIRE (r.a, r.b) IS RELATIONSHIP KEY",
		"CREATE CONSTRAINT c_rel_key_single_named IF NOT EXISTS FOR ()-[r:RELKEYC]-() REQUIRE r.k IS RELATIONSHIP KEY",
		"CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:RELKEYD]-() REQUIRE r.k IS RELATIONSHIP KEY",
		"CREATE CONSTRAINT c_rel_unique_composite_named IF NOT EXISTS FOR ()-[r:RELUNQA]-() REQUIRE (r.a, r.b) IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:RELUNQB]-() REQUIRE (r.a, r.b) IS UNIQUE",
		"CREATE CONSTRAINT c_rel_unique_single_named IF NOT EXISTS FOR ()-[r:RELUNQC]-() REQUIRE r.k IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:RELUNQD]-() REQUIRE r.k IS UNIQUE",
		"CREATE CONSTRAINT c_rel_exists_named IF NOT EXISTS FOR ()-[r:RELEXA]-() REQUIRE r.k IS NOT NULL",
		"CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:RELEXB]-() REQUIRE r.k IS NOT NULL",
		"CREATE CONSTRAINT c_rel_type_named IF NOT EXISTS FOR ()-[r:RELTYPEA]-() REQUIRE r.ts IS :: ZONED DATETIME",
		"CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:RELTYPEB]-() REQUIRE r.ts IS :: ZONED DATETIME",
	}

	for _, q := range queries {
		_, err := exec.executeCreateConstraint(ctx, q)
		if strings.Contains(q, " ASSERT ") {
			require.Error(t, err, q)
			continue
		}
		require.NoError(t, err, q)
	}

	constraints := store.GetSchema().GetAllConstraints()
	require.NotEmpty(t, constraints)
}

func TestCreateConstraint_BranchMatrix_DuplicateAndErrorPaths(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "schema_constraint_matrix_err")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Duplicate name with different shape must surface conflict.
	_, err := exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT dup_name_collision FOR ()-[r:DUPREL]-() REQUIRE r.k IS UNIQUE")
	require.NoError(t, err)
	_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT dup_name_collision FOR ()-[r:DUPREL]-() REQUIRE r.other IS UNIQUE")
	require.Error(t, err)

	// Temporal arity failures.
	_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT bad_temporal_node FOR (n:BadTemporal) REQUIRE (n.k, n.valid_from) IS TEMPORAL")
	require.Error(t, err)
	require.Contains(t, err.Error(), "TEMPORAL constraint")

	_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT bad_temporal_rel FOR ()-[r:BadTemporalRel]-() REQUIRE (r.k, r.valid_from) IS TEMPORAL")
	require.Error(t, err)
	require.Contains(t, err.Error(), "TEMPORAL constraint")

	// Relationship KEY with empty property tuple should fail syntax matching and error deterministically.
	_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT bad_rel_key FOR ()-[r:BadRelKey]-() REQUIRE () IS RELATIONSHIP KEY")
	require.Error(t, err)
}
