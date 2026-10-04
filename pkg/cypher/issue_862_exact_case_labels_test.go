package cypher

// NornicDB #862: label and relationship-type names are case-sensitive, as in
// Neo4j: :Person, :person and :PERSON are three labels. Storage lower-cased
// them in its index keys and counters, so counts added every spelling.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestIssue862LabelAndTypeNamesAreCaseSensitive(t *testing.T) {
	stacks := map[string]func(t *testing.T) *StorageExecutor{
		"memory": func(t *testing.T) *StorageExecutor {
			exec, _ := newTestExecutor(t)
			return exec
		},
		"async stack": newAsyncStackTestExecutor,
	}
	for stack, build := range stacks {
		for _, mode := range []string{"auto-commit", "explicit transaction"} {
			t.Run(stack+"/"+mode, func(t *testing.T) {
				exec := build(t)
				ctx := context.Background()
				run := func(q string) [][]interface{} {
					t.Helper()
					if mode == "explicit transaction" {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					res, err := exec.Execute(ctx, q, nil)
					if mode == "explicit transaction" {
						if err != nil {
							_, _ = exec.Execute(ctx, "ROLLBACK", nil)
						} else {
							_, commitErr := exec.Execute(ctx, "COMMIT", nil)
							require.NoError(t, commitErr, q)
						}
					}
					require.NoError(t, err, q)
					return res.Rows
				}
				run("CREATE (:Person {k:'upper'}), (:person {k:'lower'})")
				run("CREATE (:A {k:1})-[:KNOWS]->(:B {k:2})")
				run("MATCH (a:A), (b:B) CREATE (a)-[:knows]->(b)")

				// Neo4j 5.26.30's answers.
				for _, tc := range []struct {
					query string
					want  [][]interface{}
				}{
					{"MATCH (n:Person) RETURN count(n)", [][]interface{}{{int64(1)}}},
					{"MATCH (n:person) RETURN count(n)", [][]interface{}{{int64(1)}}},
					{"MATCH (n:PERSON) RETURN count(n)", [][]interface{}{{int64(0)}}},
					{"MATCH (n:Person) RETURN n.k", [][]interface{}{{"upper"}}},
					{"MATCH (n:PERSON) RETURN n.k", [][]interface{}{}},
					{"MATCH (n) WHERE n:PERSON RETURN count(n)", [][]interface{}{{int64(0)}}},
					{"MATCH ()-[r:KNOWS]->() RETURN count(r)", [][]interface{}{{int64(1)}}},
					{"MATCH ()-[r:knows]->() RETURN count(r)", [][]interface{}{{int64(1)}}},
					{"MATCH ()-[r:Knows]->() RETURN count(r)", [][]interface{}{{int64(0)}}},
					{"MATCH (:A)-[r:knows]->() RETURN count(r)", [][]interface{}{{int64(1)}}},
					{"MATCH ()-[r:knows]->(:B) RETURN count(r)", [][]interface{}{{int64(1)}}},
					{"MATCH (:A)-[r:KNOWS|knows]->(:B) RETURN count(r)", [][]interface{}{{int64(2)}}},
					{"MATCH ()-[r:knows]->() RETURN type(r)", [][]interface{}{{"knows"}}},
					{"MATCH (a:A)-[r:knows]->(b:B) RETURN count(*)", [][]interface{}{{int64(1)}}},
				} {
					require.Equal(t, tc.want, run(tc.query), tc.query)
				}

				// Relabelling to another spelling moves the node between labels.
				run("MATCH (n:person) REMOVE n:person SET n:PERSON")
				require.Equal(t, [][]interface{}{{int64(0)}}, run("MATCH (n:person) RETURN count(n)"))
				require.Equal(t, [][]interface{}{{int64(1)}}, run("MATCH (n:PERSON) RETURN count(n)"))
				require.Equal(t, [][]interface{}{{int64(1)}}, run("MATCH (n:Person) RETURN count(n)"))
				run("MATCH (n:Person) SET n:person REMOVE n:Person")
				require.Equal(t, [][]interface{}{{int64(0)}}, run("MATCH (n:Person) RETURN count(n)"))
				require.Equal(t, [][]interface{}{{"upper"}}, run("MATCH (n:person) RETURN n.k"))
				run("MATCH ()-[r:knows]->() DELETE r")
				require.Equal(t, [][]interface{}{{int64(1)}}, run("MATCH ()-[r:KNOWS]->() RETURN count(r)"))
				require.Equal(t, [][]interface{}{{int64(0)}}, run("MATCH ()-[r:knows]->() RETURN count(r)"))
			})
		}
	}
}

// Unnamed constraints and indexes on labels that differ only in case are two
// schema objects, as in Neo4j: their generated names keep the label's case.
func TestIssue862SchemaOnLabelsThatDifferInCase(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	for _, q := range []string{
		"CREATE CONSTRAINT FOR (n:Person) REQUIRE n.id IS UNIQUE",
		"CREATE CONSTRAINT FOR (n:person) REQUIRE n.id IS UNIQUE",
		"CREATE INDEX FOR (n:Person) ON (n.name)",
		"CREATE INDEX FOR (n:person) ON (n.name)",
		"CREATE (:Person {id: 1}), (:person {id: 1})",
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, q)
	}
	res, err := exec.Execute(ctx, "SHOW CONSTRAINTS YIELD labelsOrTypes RETURN labelsOrTypes ORDER BY labelsOrTypes[0]", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]string{"Person"}}, {[]string{"person"}}}, res.Rows)
	_, err = exec.Execute(ctx, "CREATE (:person {id: 1})", nil)
	require.Error(t, err, "the :person constraint holds")
}

// Relationship-type filters outside MATCH compare exactly too: CREATE's
// existing-relationship check and the FastRP projection.
func TestIssue862RelationshipTypeFiltersAreCaseSensitive(t *testing.T) {
	exec, store := newTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:N {id: 'a'})-[:R]->(b:N {id: 'b'}), (a)-[:r]->(b), (b)-[:R]->(c:N {id: 'c'})", nil)
	require.NoError(t, err)
	nodes, err := store.GetNodesByLabel("N")
	require.NoError(t, err)
	byID := map[string]storage.NodeID{}
	for _, node := range nodes {
		byID[node.Properties["id"].(string)] = node.ID
	}
	for edgeType, want := range map[string]bool{"R": true, "r": true, "Rr": false} {
		exists, err := hasRelationshipOfType(store, byID["a"], byID["b"], edgeType)
		require.NoError(t, err)
		require.Equal(t, want, exists, edgeType)
	}
	exists, err := hasRelationshipOfType(store, byID["b"], byID["c"], "r")
	require.NoError(t, err)
	require.False(t, exists, "only :R joins b and c")

	upper, err := exec.buildGraphProjection("upper", []string{"N"}, []string{"R"})
	require.NoError(t, err)
	lower, err := exec.buildGraphProjection("lower", []string{"N"}, []string{"r"})
	require.NoError(t, err)
	require.Equal(t, 2, upper.RelationshipCount)
	require.Equal(t, 1, lower.RelationshipCount)
}

func TestIssue862ContractNameKeepsLabelCase(t *testing.T) {
	exec, _ := newTestExecutor(t)
	for _, label := range []string{"Person", "person"} {
		parsed, err := exec.parseCreateConstraintContractDDL("CREATE CONSTRAINT FOR (n:" + label + ") REQUIRE { n.id IS UNIQUE }")
		require.NoError(t, err)
		require.Equal(t, "constraint_"+label+"_contract", parsed.name)
	}
}
