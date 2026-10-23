package testutil

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestSetupExecutorsAndHelpers(t *testing.T) {
	exec := SetupTestExecutor(t)
	result := ExecuteQuery(t, exec, "RETURN 1 AS one", nil)
	AssertQueryResultColumns(t, result, []string{"one"}, 1)
	if v := GetSingleValue(t, result); v != int64(1) {
		t.Fatalf("expected single value 1, got %v", v)
	}

	store := storage.NewMemoryEngine()
	defer store.Close()
	exec2 := SetupTestExecutorWithStore(t, store)
	result2 := ExecuteQuery(t, exec2, "RETURN 2 AS two", nil)
	AssertQueryResult(t, result2, 1)
}

func TestCreateFixturesAndColumnExtraction(t *testing.T) {
	exec := SetupTestExecutor(t)
	CreateTestNodes(t, exec)

	people := ExecuteQuery(t, exec, "MATCH (p:Person) RETURN p.name ORDER BY p.name", nil)
	AssertQueryResultColumns(t, people, []string{"p.name"}, 3)
	names := GetColumnValues(t, people, 0)
	if len(names) != 3 || names[0] != "Alice" || names[2] != "Charlie" {
		t.Fatalf("unexpected ordered names: %#v", names)
	}

	MustExecute(t, exec, "CREATE (x:Extra {v: 42})")
	extra := ExecuteQuery(t, exec, "MATCH (x:Extra) RETURN count(x) AS c", nil)
	if GetSingleValue(t, extra) != int64(1) {
		t.Fatalf("expected one Extra node")
	}
}

func TestCreateTestGraph(t *testing.T) {
	exec := SetupTestExecutor(t)
	CreateTestGraph(t, exec)

	nodes := ExecuteQuery(t, exec, "MATCH (n) RETURN count(n) AS c", nil)
	if got := GetSingleValue(t, nodes); got != int64(5) {
		t.Fatalf("expected 5 nodes in fixture graph, got %v", got)
	}

	rels := ExecuteQuery(t, exec, "MATCH ()-[r]->() RETURN count(r) AS c", nil)
	if got := GetSingleValue(t, rels); got != int64(4) {
		t.Fatalf("expected 4 relationships in fixture graph, got %v", got)
	}

	worksAt := ExecuteQuery(t, exec, "MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN count(r) AS c", nil)
	if got := GetSingleValue(t, worksAt); got != int64(2) {
		t.Fatalf("expected 2 WORKS_AT relationships, got %v", got)
	}
}
