package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Regression tests for constraint gaps found while mapping a relational schema
// onto NornicDB constraints (reproduced on 1.4.0 and 1.4.1).

func newConstraintGapExecutor(t *testing.T) (*StorageExecutor, storage.Engine) {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	return NewStorageExecutor(store), store
}

// BUG: `IS :: LIST<...>` property type constraints failed with
// "unsupported property type" although lists are valid property values.
func TestBug_ListPropertyTypeConstraint(t *testing.T) {
	ctx := context.Background()

	t.Run("primitive LIST<T NOT NULL> and LIST<T> enforce element types", func(t *testing.T) {
		exec, store := newConstraintGapExecutor(t)
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT doc_tags_type FOR (n:Doc) REQUIRE n.tags IS :: LIST<STRING NOT NULL>`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `CREATE CONSTRAINT doc_ids_type FOR (n:Doc) REQUIRE n.ids IS :: list < integer >`, nil)
		require.NoError(t, err)

		types := map[string]storage.PropertyType{}
		for _, c := range store.GetSchema().GetAllPropertyTypeConstraints() {
			types[c.Name] = c.ExpectedType
		}
		require.Equal(t, storage.PropertyType("LIST<STRING NOT NULL>"), types["doc_tags_type"])
		require.Equal(t, storage.PropertyType("LIST<INTEGER NOT NULL>"), types["doc_ids_type"], "LIST<T> normalizes to Neo4j's canonical LIST<T NOT NULL>")

		_, err = exec.Execute(ctx, `CREATE (:Doc {tags: ['a', 'b'], ids: [1, 2]})`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `CREATE (:Doc {tags: []})`, nil)
		require.NoError(t, err, "empty list satisfies any element type")
		_, err = exec.Execute(ctx, `CREATE (:Doc {})`, nil)
		require.NoError(t, err, "absent property satisfies a type constraint")

		_, err = exec.Execute(ctx, `CREATE (:Doc {tags: ['a', 1]})`, nil)
		require.Error(t, err)
		_, err = exec.Execute(ctx, `CREATE (:Doc {tags: 'a'})`, nil)
		require.Error(t, err, "a scalar is not a list")
		_, err = exec.Execute(ctx, `CREATE (:Doc {ids: [1.5]})`, nil)
		require.Error(t, err)
	})

	t.Run("creation validates existing data", func(t *testing.T) {
		exec, _ := newConstraintGapExecutor(t)
		_, err := exec.Execute(ctx, `CREATE (:Doc {tags: [1, 2]})`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `CREATE CONSTRAINT doc_tags_type FOR (n:Doc) REQUIRE n.tags IS :: LIST<STRING NOT NULL>`, nil)
		require.Error(t, err)
	})

	t.Run("inside a contract", func(t *testing.T) {
		exec, _ := newConstraintGapExecutor(t)
		_, err := exec.Execute(ctx, `
			CREATE CONSTRAINT doc_contract FOR (n:Doc) REQUIRE {
			  n.id IS UNIQUE
			  n.tags IS :: LIST<STRING NOT NULL>
			}`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `CREATE (:Doc {id: 'd1', tags: ['x']})`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `CREATE (:Doc {id: 'd2', tags: [true]})`, nil)
		require.Error(t, err)
	})

	t.Run("unsupported list forms are rejected", func(t *testing.T) {
		exec, _ := newConstraintGapExecutor(t)
		for _, typ := range []string{"LIST", "LIST<>", "LIST<LIST<STRING>>", "LIST<NOPE>"} {
			_, err := exec.Execute(ctx, `CREATE CONSTRAINT bad FOR (n:Doc) REQUIRE n.tags IS :: `+typ, nil)
			require.Error(t, err, typ)
		}
	})
}

// BUG: a contract entry `n.status IN [...]` rejected nodes without `status`,
// while the equivalent primitive domain constraint accepts them; comparisons
// against an absent property compared the string "<nil>".
func TestBug_ContractPropertyPredicatesAllowAbsentProperty(t *testing.T) {
	ctx := context.Background()
	exec, _ := newConstraintGapExecutor(t)

	_, err := exec.Execute(ctx, `
		CREATE CONSTRAINT task_contract FOR (n:Task) REQUIRE {
		  n.id IS UNIQUE
		  n.status IN ['open', 'done']
		  n.points > 0
		}`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `CREATE (:Task {id: 't1'})`, nil)
	require.NoError(t, err, "absent properties satisfy IN and comparison entries (CHECK semantics)")
	_, err = exec.Execute(ctx, `CREATE (:Task {id: 't2', status: 'open', points: 3})`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `CREATE (:Task {id: 't3', status: 'archived'})`, nil)
	require.ErrorContains(t, err, "n.status IN")
	_, err = exec.Execute(ctx, `CREATE (:Task {id: 't4', points: -1})`, nil)
	require.ErrorContains(t, err, "n.points > 0")

	_, err = exec.Execute(ctx, `MATCH (t:Task {id: 't1'}) SET t.status = 'bogus'`, nil)
	require.Error(t, err, "setting a value later is still checked")

	_, err = exec.Execute(ctx, `
		CREATE CONSTRAINT works_on_contract FOR ()-[r:WORKS_ON]-() REQUIRE {
		  r.role IN ['owner', 'reviewer']
		}`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (a:Task {id: 't1'}), (b:Task {id: 't2'}) CREATE (a)-[:WORKS_ON]->(b)`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (a:Task {id: 't1'}), (b:Task {id: 't2'}) CREATE (a)-[:WORKS_ON {role: 'intern'}]->(b)`, nil)
	require.Error(t, err)
}

// BUG: a contract containing a predicate the evaluator cannot run was created
// successfully on an empty label, then rejected every later write to the label.
func TestBug_ContractUnsupportedPredicateRejectedAtCreate(t *testing.T) {
	ctx := context.Background()

	t.Run("node contract", func(t *testing.T) {
		exec, store := newConstraintGapExecutor(t)
		_, err := exec.Execute(ctx, `
			CREATE CONSTRAINT bad_contract FOR (n:Thing) REQUIRE {
			  n.id IS UNIQUE
			  n.st IS NULL OR n.st IN ['a', 'b']
			}`, nil)
		require.ErrorContains(t, err, "unsupported node predicate")

		require.Empty(t, store.GetSchema().GetAllConstraintContracts())
		show, err := exec.Execute(ctx, `SHOW CONSTRAINTS`, nil)
		require.NoError(t, err)
		require.Empty(t, show.Rows, "compiled entries must not be left behind")

		_, err = exec.Execute(ctx, `CREATE (:Thing {id: 'x1'})`, nil)
		require.NoError(t, err, "the label stays writable")
	})

	t.Run("relationship contract", func(t *testing.T) {
		exec, store := newConstraintGapExecutor(t)
		_, err := exec.Execute(ctx, `
			CREATE CONSTRAINT bad_rel_contract FOR ()-[r:LINKS]-() REQUIRE {
			  r.weight IS NULL OR r.weight > 0
			}`, nil)
		require.ErrorContains(t, err, "unsupported relationship predicate")
		require.Empty(t, store.GetSchema().GetAllConstraintContracts())
	})
}

// BUG: `DROP CONSTRAINT <contract>` reported "does not exist" while
// SHOW CONSTRAINT CONTRACTS still listed it, so contracts could never be removed.
func TestBug_DropConstraintContract(t *testing.T) {
	ctx := context.Background()
	exec, store := newConstraintGapExecutor(t)

	create := `
		CREATE CONSTRAINT person_contract FOR (n:Person) REQUIRE {
		  n.id IS UNIQUE
		  n.name IS NOT NULL
		  n.age IS :: INTEGER
		  n.status IN ['active', 'inactive']
		}`
	_, err := exec.Execute(ctx, create, nil)
	require.NoError(t, err)
	show, err := exec.Execute(ctx, `SHOW CONSTRAINTS`, nil)
	require.NoError(t, err)
	require.Len(t, show.Rows, 3)

	_, err = exec.Execute(ctx, `DROP CONSTRAINT person_contract`, nil)
	require.NoError(t, err)

	require.Empty(t, store.GetSchema().GetAllConstraintContracts())
	contracts, err := exec.Execute(ctx, `SHOW CONSTRAINT CONTRACTS`, nil)
	require.NoError(t, err)
	require.Empty(t, contracts.Rows)
	show, err = exec.Execute(ctx, `SHOW CONSTRAINTS`, nil)
	require.NoError(t, err)
	require.Empty(t, show.Rows, "compiled entries are dropped with the contract")

	// Every rule is gone: duplicate ids, missing name, wrong type and status all write.
	_, err = exec.Execute(ctx, `CREATE (:Person {id: 'p1', age: 'old', status: 'x'}), (:Person {id: 'p1'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (n:Person) DETACH DELETE n`, nil)
	require.NoError(t, err)

	// The name is free again.
	_, err = exec.Execute(ctx, create, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `DROP CONSTRAINT person_contract IF EXISTS`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `DROP CONSTRAINT person_contract IF EXISTS`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `DROP CONSTRAINT person_contract`, nil)
	require.Error(t, err)
}

// BUG: the static function-arity check read a temporal type name followed by
// "(" as a call: in a REQUIRE { } block, `n.at IS :: ZONED DATETIME` followed
// by a `(n.k, n.from, n.to) IS TEMPORAL NO OVERLAP` entry failed with
// "Too many parameters for function 'DATETIME'" (DATE likewise).
func TestBug_TypeAnnotationNotReadAsFunctionCall(t *testing.T) {
	ctx := context.Background()
	exec, store := newConstraintGapExecutor(t)

	for _, typ := range []string{"ZONED DATETIME", "DATETIME", "DATE", "LOCAL DATETIME"} {
		label := "T" + strings.ReplaceAll(typ, " ", "")
		_, err := exec.Execute(ctx, `
			CREATE CONSTRAINT `+strings.ToLower(label)+`_contract FOR (n:`+label+`) REQUIRE {
			  n.at IS :: `+typ+`
			  (n.k, n.vf, n.vt) IS TEMPORAL NO OVERLAP
			}`, nil)
		require.NoError(t, err, typ)
	}
	require.Len(t, store.GetSchema().GetAllConstraintContracts(), 4)

	_, err := exec.Execute(ctx, `CREATE (:Ev {d: date('2026-01-01'), n: 2})`, nil)
	require.NoError(t, err)
	res, err := exec.Execute(ctx, "MATCH (e:Ev) WHERE e.d IS :: DATE\n AND (e.n > 1) RETURN count(e)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, res.Rows[0][0])
	res, err = exec.Execute(ctx, "MATCH (e:Ev) WHERE e.d IS TYPED DATE AND (e.n > 1) RETURN count(e)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, res.Rows[0][0])

	// Real calls are still arity-checked.
	_, err = exec.Execute(ctx, `RETURN date('2026-01-01', 1, 2)`, nil)
	require.ErrorContains(t, err, "Too many parameters for function")
}
