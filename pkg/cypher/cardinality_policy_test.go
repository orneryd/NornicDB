package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Cardinality Constraint Tests
// ============================================================================

func TestCardinalityConstraint_DDL(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("named outgoing", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT max_employers IF NOT EXISTS FOR ()-[r:WORKS_AT]->() REQUIRE MAX COUNT 3`, nil)
		require.NoError(t, err)

		all := store.GetSchema().GetAllConstraints()
		found := false
		for _, c := range all {
			if c.Name == "max_employers" {
				require.Equal(t, storage.ConstraintCardinality, c.Type)
				require.Equal(t, storage.ConstraintEntityRelationship, c.EntityType)
				require.Equal(t, "WORKS_AT", c.Label)
				require.Equal(t, 3, c.MaxCount)
				require.Equal(t, "OUTGOING", c.Direction)
				found = true
			}
		}
		require.True(t, found, "constraint max_employers not found")
	})

	t.Run("named incoming", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT max_employees IF NOT EXISTS FOR ()<-[r:WORKS_AT]-() REQUIRE MAX COUNT 50`, nil)
		require.NoError(t, err)

		all := store.GetSchema().GetAllConstraints()
		found := false
		for _, c := range all {
			if c.Name == "max_employees" {
				require.Equal(t, storage.ConstraintCardinality, c.Type)
				require.Equal(t, "INCOMING", c.Direction)
				require.Equal(t, 50, c.MaxCount)
				found = true
			}
		}
		require.True(t, found, "constraint max_employees not found")
	})

	t.Run("unnamed outgoing", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT FOR ()-[r:FOLLOWS]->() REQUIRE MAX COUNT 100`, nil)
		require.NoError(t, err)

		all := store.GetSchema().GetAllConstraints()
		found := false
		for _, c := range all {
			if c.Label == "FOLLOWS" && c.Type == storage.ConstraintCardinality {
				require.Equal(t, "OUTGOING", c.Direction)
				require.Equal(t, 100, c.MaxCount)
				found = true
			}
		}
		require.True(t, found, "unnamed FOLLOWS cardinality constraint not found")
	})

	t.Run("unnamed incoming", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT FOR ()<-[r:LIKES]-() REQUIRE MAX COUNT 10`, nil)
		require.NoError(t, err)

		all := store.GetSchema().GetAllConstraints()
		found := false
		for _, c := range all {
			if c.Label == "LIKES" && c.Type == storage.ConstraintCardinality {
				require.Equal(t, "INCOMING", c.Direction)
				require.Equal(t, 10, c.MaxCount)
				found = true
			}
		}
		require.True(t, found, "unnamed LIKES cardinality constraint not found")
	})

	t.Run("IF NOT EXISTS is no-op for existing", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT max_employers IF NOT EXISTS FOR ()-[r:WORKS_AT]->() REQUIRE MAX COUNT 3`, nil)
		require.NoError(t, err)
	})

	t.Run("invalid max count zero", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT bad_zero FOR ()-[r:BAD]->() REQUIRE MAX COUNT 0`, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "positive integer")
	})

	t.Run("invalid max count negative", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT bad_neg FOR ()-[r:BAD]->() REQUIRE MAX COUNT -1`, nil)
		require.Error(t, err)
	})
}

func TestCardinalityConstraint_CreationValidation(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes and edges.
	_, err := exec.Execute(ctx, `CREATE (p:Person {name: 'Alice'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (c:Company {name: 'Acme'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (c2:Company {name: 'Beta'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (c3:Company {name: 'Gamma'})`, nil)
	require.NoError(t, err)

	// Create 3 outgoing WORKS_AT edges from Alice.
	_, err = exec.Execute(ctx, `MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (p)-[:WORKS_AT]->(c)`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'Beta'}) CREATE (p)-[:WORKS_AT]->(c)`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'Gamma'}) CREATE (p)-[:WORKS_AT]->(c)`, nil)
	require.NoError(t, err)

	t.Run("creation succeeds when within limit", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT card_ok FOR ()-[r:WORKS_AT]->() REQUIRE MAX COUNT 3`, nil)
		require.NoError(t, err)
	})

	t.Run("creation fails when existing edges exceed limit", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT card_fail FOR ()-[r:WORKS_AT]->() REQUIRE MAX COUNT 2`, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeding max count")
	})
}

func TestCardinalityConstraint_Enforcement(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create constraint: max 2 outgoing WORKS_AT per person.
	_, err := exec.Execute(ctx, `CREATE CONSTRAINT max_jobs FOR ()-[r:WORKS_AT]->() REQUIRE MAX COUNT 2`, nil)
	require.NoError(t, err)

	// Create nodes.
	_, err = exec.Execute(ctx, `CREATE (p:Person {name: 'Alice'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (c1:Company {name: 'Acme'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (c2:Company {name: 'Beta'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (c3:Company {name: 'Gamma'})`, nil)
	require.NoError(t, err)

	t.Run("creating edges up to max count succeeds", func(t *testing.T) {
		_, err := exec.Execute(ctx, `MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (p)-[:WORKS_AT]->(c)`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'Beta'}) CREATE (p)-[:WORKS_AT]->(c)`, nil)
		require.NoError(t, err)
	})

	t.Run("creating edge that exceeds max count is rejected", func(t *testing.T) {
		_, err := exec.Execute(ctx, `MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'Gamma'}) CREATE (p)-[:WORKS_AT]->(c)`, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "max outgoing count of 2")
	})

	t.Run("direction scoping: outgoing constraint does not affect incoming", func(t *testing.T) {
		// Create incoming constraint on a different type.
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT max_in_friends FOR ()<-[r:FRIEND_OF]-() REQUIRE MAX COUNT 1`, nil)
		require.NoError(t, err)

		// Create two people.
		_, err = exec.Execute(ctx, `CREATE (b:Person {name: 'Bob'})`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `CREATE (c:Person {name: 'Charlie'})`, nil)
		require.NoError(t, err)

		// Bob -> Alice is fine (first incoming FRIEND_OF to Alice).
		_, err = exec.Execute(ctx, `MATCH (b:Person {name: 'Bob'}), (a:Person {name: 'Alice'}) CREATE (b)-[:FRIEND_OF]->(a)`, nil)
		require.NoError(t, err)

		// Charlie -> Alice should fail (second incoming FRIEND_OF to Alice).
		_, err = exec.Execute(ctx, `MATCH (c:Person {name: 'Charlie'}), (a:Person {name: 'Alice'}) CREATE (c)-[:FRIEND_OF]->(a)`, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "max incoming count of 1")
	})
}

func TestCardinalityConstraint_Transaction(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create constraint: max 2 outgoing WRITES.
	_, err := exec.Execute(ctx, `CREATE CONSTRAINT max_writes FOR ()-[r:WRITES]->() REQUIRE MAX COUNT 2`, nil)
	require.NoError(t, err)

	// Create nodes.
	_, err = exec.Execute(ctx, `CREATE (a:Author {name: 'Alice'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (b1:Book {title: 'Book1'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (b2:Book {title: 'Book2'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (b3:Book {title: 'Book3'})`, nil)
	require.NoError(t, err)

	t.Run("batch creating edges within limit succeeds in transaction", func(t *testing.T) {
		_, err := exec.Execute(ctx, `
			MATCH (a:Author {name: 'Alice'}), (b1:Book {title: 'Book1'}), (b2:Book {title: 'Book2'})
			CREATE (a)-[:WRITES]->(b1), (a)-[:WRITES]->(b2)
		`, nil)
		require.NoError(t, err)
	})

	t.Run("batch creating edges that exceed limit is rejected in transaction", func(t *testing.T) {
		_, err := exec.Execute(ctx, `
			MATCH (a:Author {name: 'Alice'}), (b3:Book {title: 'Book3'})
			CREATE (a)-[:WRITES]->(b3)
		`, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "CARDINALITY")
	})
}

func TestCardinalityConstraint_ConflictDetection(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("same schema same direction different count is conflict", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT c1 FOR ()-[r:HAS]->() REQUIRE MAX COUNT 5`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `CREATE CONSTRAINT c2 FOR ()-[r:HAS]->() REQUIRE MAX COUNT 10`, nil)
		require.Error(t, err)
	})

	t.Run("same type different direction is not conflict", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT c_out FOR ()-[r:MANAGES]->() REQUIRE MAX COUNT 5`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `CREATE CONSTRAINT c_in FOR ()<-[r:MANAGES]-() REQUIRE MAX COUNT 10`, nil)
		require.NoError(t, err)
	})
}

// ============================================================================
// Relationship Endpoint Policy Tests
// ============================================================================

func TestPolicyConstraint_DDL(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("named ALLOWED", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT person_works_at IF NOT EXISTS FOR (:Person)-[r:WORKS_AT]->(:Company) REQUIRE ALLOWED`, nil)
		require.NoError(t, err)

		all := store.GetSchema().GetAllConstraints()
		found := false
		for _, c := range all {
			if c.Name == "person_works_at" {
				require.Equal(t, storage.ConstraintPolicy, c.Type)
				require.Equal(t, storage.ConstraintEntityRelationship, c.EntityType)
				require.Equal(t, "WORKS_AT", c.Label)
				require.Equal(t, "Person", c.SourceLabel)
				require.Equal(t, "Company", c.TargetLabel)
				require.Equal(t, "ALLOWED", c.PolicyMode)
				found = true
			}
		}
		require.True(t, found, "constraint person_works_at not found")
	})

	t.Run("named DISALLOWED", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT no_intern_exec FOR (:Intern)-[r:REPORTS_TO]->(:Executive) REQUIRE DISALLOWED`, nil)
		require.NoError(t, err)

		all := store.GetSchema().GetAllConstraints()
		found := false
		for _, c := range all {
			if c.Name == "no_intern_exec" {
				require.Equal(t, storage.ConstraintPolicy, c.Type)
				require.Equal(t, "REPORTS_TO", c.Label)
				require.Equal(t, "Intern", c.SourceLabel)
				require.Equal(t, "Executive", c.TargetLabel)
				require.Equal(t, "DISALLOWED", c.PolicyMode)
				found = true
			}
		}
		require.True(t, found, "constraint no_intern_exec not found")
	})

	t.Run("unnamed ALLOWED", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT FOR (:Student)-[r:ENROLLED_IN]->(:Course) REQUIRE ALLOWED`, nil)
		require.NoError(t, err)

		all := store.GetSchema().GetAllConstraints()
		found := false
		for _, c := range all {
			if c.Label == "ENROLLED_IN" && c.Type == storage.ConstraintPolicy {
				require.Equal(t, "ALLOWED", c.PolicyMode)
				require.Equal(t, "Student", c.SourceLabel)
				require.Equal(t, "Course", c.TargetLabel)
				found = true
			}
		}
		require.True(t, found, "unnamed ENROLLED_IN ALLOWED policy not found")
	})

	t.Run("unnamed DISALLOWED", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT FOR (:Robot)-[r:TEACHES]->(:Human) REQUIRE DISALLOWED`, nil)
		require.NoError(t, err)

		all := store.GetSchema().GetAllConstraints()
		found := false
		for _, c := range all {
			if c.Label == "TEACHES" && c.Type == storage.ConstraintPolicy && c.PolicyMode == "DISALLOWED" {
				require.Equal(t, "Robot", c.SourceLabel)
				require.Equal(t, "Human", c.TargetLabel)
				found = true
			}
		}
		require.True(t, found, "unnamed TEACHES DISALLOWED policy not found")
	})

	t.Run("IF NOT EXISTS is no-op for existing", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT person_works_at IF NOT EXISTS FOR (:Person)-[r:WORKS_AT]->(:Company) REQUIRE ALLOWED`, nil)
		require.NoError(t, err)
	})
}

func TestPolicyConstraint_DISALLOWED_Enforcement(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create DISALLOWED policy: Intern cannot REPORTS_TO Executive.
	_, err := exec.Execute(ctx, `CREATE CONSTRAINT no_intern_exec FOR (:Intern)-[r:REPORTS_TO]->(:Executive) REQUIRE DISALLOWED`, nil)
	require.NoError(t, err)

	// Create nodes.
	_, err = exec.Execute(ctx, `CREATE (i:Intern {name: 'Alice'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (e:Executive {name: 'Bob'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (m:Manager {name: 'Charlie'})`, nil)
	require.NoError(t, err)

	t.Run("creating forbidden edge is rejected", func(t *testing.T) {
		_, err := exec.Execute(ctx, `MATCH (i:Intern {name: 'Alice'}), (e:Executive {name: 'Bob'}) CREATE (i)-[:REPORTS_TO]->(e)`, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DISALLOWED")
	})

	t.Run("non-matching label pair is allowed", func(t *testing.T) {
		_, err := exec.Execute(ctx, `MATCH (m:Manager {name: 'Charlie'}), (e:Executive {name: 'Bob'}) CREATE (m)-[:REPORTS_TO]->(e)`, nil)
		require.NoError(t, err)
	})
}

func TestPolicyConstraint_ALLOWED_Enforcement(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create ALLOWED policy: only Person -> Company is allowed for WORKS_AT.
	_, err := exec.Execute(ctx, `CREATE CONSTRAINT person_works FOR (:Person)-[r:WORKS_AT]->(:Company) REQUIRE ALLOWED`, nil)
	require.NoError(t, err)

	// Create nodes.
	_, err = exec.Execute(ctx, `CREATE (p:Person {name: 'Alice'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (c:Company {name: 'Acme'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (r:Robot {name: 'R2D2'})`, nil)
	require.NoError(t, err)

	t.Run("matching label pair is allowed", func(t *testing.T) {
		_, err := exec.Execute(ctx, `MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (p)-[:WORKS_AT]->(c)`, nil)
		require.NoError(t, err)
	})

	t.Run("non-matching label pair is rejected", func(t *testing.T) {
		_, err := exec.Execute(ctx, `MATCH (r:Robot {name: 'R2D2'}), (c:Company {name: 'Acme'}) CREATE (r)-[:WORKS_AT]->(c)`, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ALLOWED policy violation")
	})
}

func TestPolicyConstraint_ALLOWED_Union(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Two ALLOWED policies form a union: Person->Company OR Contractor->Company.
	_, err := exec.Execute(ctx, `CREATE CONSTRAINT person_works FOR (:Person)-[r:WORKS_AT]->(:Company) REQUIRE ALLOWED`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE CONSTRAINT contractor_works FOR (:Contractor)-[r:WORKS_AT]->(:Company) REQUIRE ALLOWED`, nil)
	require.NoError(t, err)

	// Create nodes.
	_, err = exec.Execute(ctx, `CREATE (p:Person {name: 'Alice'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (ct:Contractor {name: 'Bob'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (r:Robot {name: 'R2D2'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (c:Company {name: 'Acme'})`, nil)
	require.NoError(t, err)

	t.Run("first ALLOWED pair succeeds", func(t *testing.T) {
		_, err := exec.Execute(ctx, `MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (p)-[:WORKS_AT]->(c)`, nil)
		require.NoError(t, err)
	})

	t.Run("second ALLOWED pair succeeds", func(t *testing.T) {
		_, err := exec.Execute(ctx, `MATCH (ct:Contractor {name: 'Bob'}), (c:Company {name: 'Acme'}) CREATE (ct)-[:WORKS_AT]->(c)`, nil)
		require.NoError(t, err)
	})

	t.Run("unlisted pair is rejected", func(t *testing.T) {
		_, err := exec.Execute(ctx, `MATCH (r:Robot {name: 'R2D2'}), (c:Company {name: 'Acme'}) CREATE (r)-[:WORKS_AT]->(c)`, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ALLOWED policy violation")
	})
}

func TestPolicyConstraint_ConflictDetection(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("ALLOWED and DISALLOWED on same pair is conflict", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT p1 FOR (:A)-[r:REL]->(:B) REQUIRE ALLOWED`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `CREATE CONSTRAINT p2 FOR (:A)-[r:REL]->(:B) REQUIRE DISALLOWED`, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "conflicting policy")
	})

	t.Run("different label pairs are not conflict", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT p3 FOR (:C)-[r:REL2]->(:D) REQUIRE ALLOWED`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `CREATE CONSTRAINT p4 FOR (:E)-[r:REL2]->(:F) REQUIRE DISALLOWED`, nil)
		require.NoError(t, err)
	})
}

func TestPolicyConstraint_CreationValidation(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes and an edge.
	_, err := exec.Execute(ctx, `CREATE (i:Intern {name: 'Alice'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (e:Executive {name: 'Bob'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (i:Intern {name: 'Alice'}), (e:Executive {name: 'Bob'}) CREATE (i)-[:REPORTS_TO]->(e)`, nil)
	require.NoError(t, err)

	t.Run("DISALLOWED creation fails when existing edges match forbidden pair", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT bad_policy FOR (:Intern)-[r:REPORTS_TO]->(:Executive) REQUIRE DISALLOWED`, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DISALLOWED")
	})

	t.Run("DISALLOWED creation succeeds when no edges match", func(t *testing.T) {
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT ok_policy FOR (:Intern)-[r:MANAGES]->(:Executive) REQUIRE DISALLOWED`, nil)
		require.NoError(t, err)
	})
}

func TestPolicyConstraint_CreationValidation_ALLOWED(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes of different label pairs and edges between them.
	_, err := exec.Execute(ctx, `CREATE (p:Person {name: 'Alice'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (c:Company {name: 'Acme'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (r:Robot {name: 'R2D2'})`, nil)
	require.NoError(t, err)

	// Create a Person->Company WORKS_AT edge (will be valid).
	_, err = exec.Execute(ctx, `MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (p)-[:WORKS_AT]->(c)`, nil)
	require.NoError(t, err)

	// Create a Robot->Company WORKS_AT edge (will NOT match Person->Company).
	_, err = exec.Execute(ctx, `MATCH (r:Robot {name: 'R2D2'}), (c:Company {name: 'Acme'}) CREATE (r)-[:WORKS_AT]->(c)`, nil)
	require.NoError(t, err)

	t.Run("ALLOWED creation fails when existing edges are not covered", func(t *testing.T) {
		// Only Person->Company is allowed, but Robot->Company already exists.
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT person_works FOR (:Person)-[r:WORKS_AT]->(:Company) REQUIRE ALLOWED`, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ALLOWED")
	})

	t.Run("ALLOWED creation succeeds when all existing edges are covered", func(t *testing.T) {
		// Use a relationship type with no existing edges — always valid.
		_, err := exec.Execute(ctx, `CREATE CONSTRAINT person_manages FOR (:Person)-[r:MANAGES]->(:Company) REQUIRE ALLOWED`, nil)
		require.NoError(t, err)
	})

	t.Run("ALLOWED creation succeeds when existing edges match the allowed pair", func(t *testing.T) {
		// Create an ALLOWED for a type where all edges match.
		// First, create a fresh edge type with only matching edges.
		_, err := exec.Execute(ctx, `MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (p)-[:CONSULTS_FOR]->(c)`, nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, `CREATE CONSTRAINT consults_allowed FOR (:Person)-[r:CONSULTS_FOR]->(:Company) REQUIRE ALLOWED`, nil)
		require.NoError(t, err)
	})
}

func TestPolicyConstraint_DISALLOWED_Precedence(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create ALLOWED for a broad pair AND DISALLOWED for a specific sub-pair.
	_, err := exec.Execute(ctx, `CREATE CONSTRAINT emp_allowed FOR (:Employee)-[r:ACCESS]->(:Resource) REQUIRE ALLOWED`, nil)
	require.NoError(t, err)

	// Node with both Employee and Intern labels — DISALLOWED should still block.
	// But first we need a DISALLOWED on a different pair.
	_, err = exec.Execute(ctx, `CREATE CONSTRAINT deny_intern FOR (:Intern)-[r:ACCESS]->(:Secret) REQUIRE DISALLOWED`, nil)
	require.NoError(t, err)

	// Create nodes.
	_, err = exec.Execute(ctx, `CREATE (i:Intern {name: 'Alice'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (s:Secret {name: 'TopSecret'})`, nil)
	require.NoError(t, err)

	t.Run("DISALLOWED takes precedence even with ALLOWED on broader type", func(t *testing.T) {
		_, err := exec.Execute(ctx, `MATCH (i:Intern {name: 'Alice'}), (s:Secret {name: 'TopSecret'}) CREATE (i)-[:ACCESS]->(s)`, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DISALLOWED")
	})
}

// ============================================================================
// SHOW CONSTRAINTS (basic visibility for new types)
// ============================================================================

func TestCardinalityConstraint_ShowConstraints(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE CONSTRAINT max_jobs FOR ()-[r:WORKS_AT]->() REQUIRE MAX COUNT 3`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "SHOW CONSTRAINTS", nil)
	require.NoError(t, err)

	found := false
	for _, row := range result.Rows {
		// Find the name column.
		for i, col := range result.Columns {
			if col == "name" && i < len(row) && row[i] == "max_jobs" {
				found = true
				// Verify type column.
				for j, col2 := range result.Columns {
					if col2 == "type" && j < len(row) {
						assert.Equal(t, "CARDINALITY", row[j])
					}
					if col2 == "entityType" && j < len(row) {
						assert.Equal(t, "RELATIONSHIP", row[j])
					}
				}
			}
		}
	}
	assert.True(t, found, "SHOW CONSTRAINTS should list cardinality constraint")
}

func TestPolicyConstraint_ShowConstraints(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE CONSTRAINT p1 FOR (:Person)-[r:KNOWS]->(:Person) REQUIRE ALLOWED`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "SHOW CONSTRAINTS", nil)
	require.NoError(t, err)

	found := false
	for _, row := range result.Rows {
		for i, col := range result.Columns {
			if col == "name" && i < len(row) && row[i] == "p1" {
				found = true
				for j, col2 := range result.Columns {
					if col2 == "type" && j < len(row) {
						assert.Equal(t, "RELATIONSHIP_POLICY", row[j])
					}
					if col2 == "entityType" && j < len(row) {
						assert.Equal(t, "RELATIONSHIP", row[j])
					}
				}
			}
		}
	}
	assert.True(t, found, "SHOW CONSTRAINTS should list policy constraint")
}

// ============================================================================
// Label Mutation Enforcement Tests
// ============================================================================

func TestPolicyConstraint_LabelMutation_DISALLOWED(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create DISALLOWED policy: Intern cannot REPORTS_TO Executive.
	_, err := exec.Execute(ctx, `CREATE CONSTRAINT no_intern_exec FOR (:Intern)-[r:REPORTS_TO]->(:Executive) REQUIRE DISALLOWED`, nil)
	require.NoError(t, err)

	// Create an Employee -> Executive edge (allowed).
	_, err = exec.Execute(ctx, `CREATE (e:Employee {name: 'Alice'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (x:Executive {name: 'Bob'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (e:Employee {name: 'Alice'}), (x:Executive {name: 'Bob'}) CREATE (e)-[:REPORTS_TO]->(x)`, nil)
	require.NoError(t, err)

	t.Run("adding forbidden label to source node is rejected", func(t *testing.T) {
		_, err := exec.Execute(ctx, `MATCH (e:Employee {name: 'Alice'}) SET e:Intern`, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DISALLOWED")
	})
}

func TestPolicyConstraint_LabelMutation_ALLOWED(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create ALLOWED policy: only Person -> Company for WORKS_AT.
	_, err := exec.Execute(ctx, `CREATE CONSTRAINT person_works FOR (:Person)-[r:WORKS_AT]->(:Company) REQUIRE ALLOWED`, nil)
	require.NoError(t, err)

	// Create nodes and a valid edge.
	_, err = exec.Execute(ctx, `CREATE (p:Person {name: 'Alice'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (c:Company {name: 'Acme'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (p)-[:WORKS_AT]->(c)`, nil)
	require.NoError(t, err)

	t.Run("removing required label from source node is rejected", func(t *testing.T) {
		_, err := exec.Execute(ctx, `MATCH (p:Person {name: 'Alice'}) REMOVE p:Person`, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ALLOWED")
	})

	t.Run("removing required label from target node is rejected", func(t *testing.T) {
		_, err := exec.Execute(ctx, `MATCH (c:Company {name: 'Acme'}) REMOVE c:Company`, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ALLOWED")
	})
}
