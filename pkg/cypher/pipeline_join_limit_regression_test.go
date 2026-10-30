package cypher

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPipelineLimitAfterJoinedMatch(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	for index, repoID := range []string{"decoyA0", "decoyA1", "decoyA2", "target", "decoyB0", "decoyB1", "decoyB2"} {
		path := fmt.Sprintf("file%d", index)
		for _, statement := range []string{
			fmt.Sprintf("CREATE (:Repository {id: '%s'})", repoID),
			fmt.Sprintf("CREATE (:File {path: '%s'})", path),
			fmt.Sprintf("MATCH (r:Repository {id: '%s'}), (f:File {path: '%s'}) CREATE (r)-[:REPO_CONTAINS]->(f)", repoID, path),
		} {
			_, err := executor.Execute(ctx, statement, nil)
			require.NoError(t, err, statement)
		}
		if repoID == "target" {
			_, err := executor.Execute(ctx, "CREATE (:Function {uid: 'fn'})", nil)
			require.NoError(t, err)
			_, err = executor.Execute(ctx, fmt.Sprintf("MATCH (f:File {path: '%s'}), (e:Function {uid: 'fn'}) CREATE (f)-[:CONTAINS]->(e)", path), nil)
			require.NoError(t, err)
		}
	}

	for _, testCase := range []struct {
		name, query string
	}{
		{"split MATCH", "MATCH (e:Function {uid: 'fn'})<-[:CONTAINS]-(f:File) MATCH (repo:Repository)-[:REPO_CONTAINS]->(f) RETURN repo.id AS repo"},
		{"WITH then MATCH", "MATCH (e:Function {uid: 'fn'})<-[:CONTAINS]-(f:File) WITH f MATCH (repo:Repository)-[:REPO_CONTAINS]->(f) RETURN repo.id AS repo"},
		{"single chain", "MATCH (e:Function {uid: 'fn'})<-[:CONTAINS]-(f:File)<-[:REPO_CONTAINS]-(repo:Repository) RETURN repo.id AS repo"},
		{"single comma MATCH", "MATCH (e:Function {uid: 'fn'})<-[:CONTAINS]-(f:File), (repo:Repository)-[:REPO_CONTAINS]->(f) RETURN repo.id AS repo"},
		{"node-only WHERE", "MATCH (e:Function {uid: 'fn'}) WITH e MATCH (repo:Repository) WHERE repo.id = 'target' AND e.uid = 'fn' RETURN repo.id AS repo"},
		{"split MATCH ordered", "MATCH (e:Function {uid: 'fn'})<-[:CONTAINS]-(f:File) MATCH (repo:Repository)-[:REPO_CONTAINS]->(f) RETURN repo.id AS repo ORDER BY repo.id"},
		{"node-only WHERE ordered", "MATCH (e:Function {uid: 'fn'}) WITH e MATCH (repo:Repository) WHERE repo.id = 'target' AND e.uid = 'fn' RETURN repo.id AS repo ORDER BY repo.id"},
	} {
		for _, limit := range []int{0, 1, 2, 3, 7} {
			t.Run(fmt.Sprintf("%s/limit=%d", testCase.name, limit), func(t *testing.T) {
				query := testCase.query
				if limit > 0 {
					query += fmt.Sprintf(" LIMIT %d", limit)
				}
				result, err := executor.Execute(ctx, query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"target"}}, result.Rows)
			})
		}
	}
}
