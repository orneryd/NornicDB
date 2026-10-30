package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestConstraintContract_ShowAndCompiledPrimitives(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE CONSTRAINT person_contract
		FOR (n:Person)
		REQUIRE {
		  n.id IS UNIQUE
		  n.name IS NOT NULL
		  n.age IS :: INTEGER
		  n.status IS :: STRING
		  n.status IN ['active', 'inactive']
		  (n.tenant, n.externalId) IS NODE KEY
		  COUNT { (n)-[:PRIMARY_EMPLOYER]->(:Company) } <= 1
		  NOT EXISTS { (n)-[:FORBIDDEN_REL]->() }
		}
	`, nil)
	require.NoError(t, err)

	contractsResult, err := exec.Execute(ctx, `SHOW CONSTRAINT CONTRACTS`, nil)
	require.NoError(t, err)
	require.Len(t, contractsResult.Rows, 1)
	require.Equal(t, "person_contract", contractsResult.Rows[0][0])
	require.Equal(t, "NODE", contractsResult.Rows[0][1])
	require.Equal(t, "Person", contractsResult.Rows[0][2])
	require.EqualValues(t, 8, contractsResult.Rows[0][3])
	require.EqualValues(t, 5, contractsResult.Rows[0][4])
	require.EqualValues(t, 3, contractsResult.Rows[0][5])
	require.Contains(t, contractsResult.Rows[0][6].(string), "REQUIRE {")

	primitiveResult, err := exec.Execute(ctx, `SHOW CONSTRAINTS`, nil)
	require.NoError(t, err)
	require.Len(t, primitiveResult.Rows, 5)
	for _, row := range primitiveResult.Rows {
		name, _ := row[1].(string)
		require.NotEqual(t, "person_contract", name)
	}

	allContracts := store.GetSchema().GetAllConstraintContracts()
	require.Len(t, allContracts, 1)
	require.Equal(t, "person_contract", allContracts[0].Name)
	require.Len(t, allContracts[0].Entries, 8)
}

func TestConstraintContract_CreateTimeValidationFailsOnBooleanEntry(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:Person {id:'p1', name:'Alice', age: 30, status:'paused', tenant:'t1', externalId:'e1'})`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
		CREATE CONSTRAINT person_contract
		FOR (n:Person)
		REQUIRE {
		  n.id IS UNIQUE
		  n.name IS NOT NULL
		  n.age IS :: INTEGER
		  n.status IN ['active', 'inactive']
		  (n.tenant, n.externalId) IS NODE KEY
		}
	`, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "constraint contract person_contract violated")
	require.Contains(t, err.Error(), "n.status IN ['active', 'inactive']")
}

func TestConstraintContract_RuntimeNodePredicateEnforcement(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE CONSTRAINT person_contract
		FOR (n:Person)
		REQUIRE {
		  n.id IS UNIQUE
		  n.name IS NOT NULL
		  n.age IS :: INTEGER
		  n.status IN ['active', 'inactive']
		  (n.tenant, n.externalId) IS NODE KEY
		  COUNT { (n)-[:PRIMARY_EMPLOYER]->(:Company) } <= 1
		  NOT EXISTS { (n)-[:FORBIDDEN_REL]->() }
		}
	`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `CREATE (:Person {id:'ok', name:'Alice', age:30, status:'active', tenant:'t1', externalId:'u1'})`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `CREATE (:Person {id:'bad', name:'Bob', age:40, status:'paused', tenant:'t1', externalId:'u2'})`, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "constraint contract person_contract violated")
	require.Contains(t, err.Error(), "n.status IN ['active', 'inactive']")

	_, err = exec.Execute(ctx, `CREATE (:Company {name:'Acme'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (:Company {name:'OtherCo'})`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `MATCH (n:Person {id:'ok'}), (c:Company {name:'Acme'}) CREATE (n)-[:PRIMARY_EMPLOYER]->(c)`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `MATCH (n:Person {id:'ok'}), (c:Company {name:'OtherCo'}) CREATE (n)-[:PRIMARY_EMPLOYER]->(c)`, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "COUNT { (n)-[:PRIMARY_EMPLOYER]->(:Company) } <= 1")

	_, err = exec.Execute(ctx, `MATCH (n:Person {id:'ok'}), (c:Company {name:'Acme'}) CREATE (n)-[:FORBIDDEN_REL]->(c)`, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "NOT EXISTS { (n)-[:FORBIDDEN_REL]->() }")
}

func TestConstraintContract_RuntimeRelationshipPredicateEnforcement(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:Person {id:'p1', tenant:'t1'}), (:Person {id:'p2', tenant:'t2'}), (:Person {id:'p3', tenant:'t1'})`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
		CREATE CONSTRAINT works_at_contract
		FOR ()-[r:WORKS_AT]-()
		REQUIRE {
		  r.id IS UNIQUE
		  r.startedAt IS NOT NULL
		  r.role IS :: STRING
		  (r.tenant, r.externalId) IS RELATIONSHIP KEY
		  startNode(r) <> endNode(r)
		  startNode(r).tenant = endNode(r).tenant
		  r.status IN ['active', 'inactive']
		  r.hoursPerWeek > 0
		}
	`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
		MATCH (a:Person {id:'p1'}), (b:Person {id:'p3'})
		CREATE (a)-[:WORKS_AT {id:'w1', startedAt:'2024-01-01', role:'Engineer', tenant:'t1', externalId:'rel-1', status:'active', hoursPerWeek:40}]->(b)
	`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
		MATCH (a:Person {id:'p1'}), (b:Person {id:'p2'})
		CREATE (a)-[:WORKS_AT {id:'w2', startedAt:'2024-01-01', role:'Engineer', tenant:'t1', externalId:'rel-2', status:'active', hoursPerWeek:40}]->(b)
	`, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "startNode(r).tenant = endNode(r).tenant")

	_, err = exec.Execute(ctx, `
		MATCH (a:Person {id:'p1'})
		CREATE (a)-[:WORKS_AT {id:'w3', startedAt:'2024-01-01', role:'Engineer', tenant:'t1', externalId:'rel-3', status:'active', hoursPerWeek:40}]->(a)
	`, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "startNode(r) <> endNode(r)")

	_, err = exec.Execute(ctx, `
		MATCH (a:Person {id:'p1'}), (b:Person {id:'p3'})
		CREATE (a)-[:WORKS_AT {id:'w4', startedAt:'2024-01-01', role:'Engineer', tenant:'t1', externalId:'rel-4', status:'paused', hoursPerWeek:40}]->(b)
	`, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "r.status IN ['active', 'inactive']")

	_, err = exec.Execute(ctx, `
		MATCH (a:Person {id:'p1'}), (b:Person {id:'p3'})
		CREATE (a)-[:WORKS_AT {id:'w5', startedAt:'2024-01-01', role:'Engineer', tenant:'t1', externalId:'rel-5', status:'active', hoursPerWeek:0}]->(b)
	`, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "r.hoursPerWeek > 0")
}

func TestConstraintContract_BlockParserErrors(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE CONSTRAINT nested_contract
		FOR (n:Person)
		REQUIRE {
		  FOR ()-[r:KNOWS]-() REQUIRE { r.id IS UNIQUE }
		}
	`, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nested FOR ... REQUIRE entries are not supported")
	require.Contains(t, err.Error(), "create a separate targeted block constraint")

	_, err = exec.Execute(ctx, `
		CREATE CONSTRAINT malformed_contract
		FOR (n:Person)
		REQUIRE {
		  n.id IS UNIQUE
	`, nil)
	require.Error(t, err)
}
