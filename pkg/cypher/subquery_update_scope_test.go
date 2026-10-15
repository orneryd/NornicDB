package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An undefined variable in an update clause is rejected before anything is
// written, inside a CALL { } subquery body and at the top level (#625): the
// subquery body sees only what it imports. Neo4j 5.26 rejects every statement
// with "Variable `m` not defined"; the valid subqueries run as before.
func TestUndefinedVariableInUpdateClauseIsRejected(t *testing.T) {
	cases := []struct {
		stmt    string
		wantErr string // "" = succeeds with want
		want    [][]interface{}
	}{
		{stmt: "MATCH (n:T {id: 1}) CALL { WITH n SET m:B } RETURN n.id AS id", wantErr: "Variable `m` not defined"},
		{stmt: "MATCH (n:T {id: 1}) CALL { WITH n SET m.x = 1 } RETURN n.id AS id", wantErr: "Variable `m` not defined"},
		{stmt: "MATCH (n:T {id: 1}) CALL { WITH n REMOVE m.x } RETURN n.id AS id", wantErr: "Variable `m` not defined"},
		{stmt: "MATCH (n:T {id: 1}) CALL { WITH n DETACH DELETE m } RETURN count(*) AS c", wantErr: "refers to an undefined variable"},
		{stmt: "MATCH (n:T {id: 1}) CALL { WITH n CALL { WITH n SET m:B } } RETURN n.id AS id", wantErr: "Variable `m` not defined"},
		{stmt: "MATCH (n:T {id: 1}) CALL (n) { SET m:B } RETURN n.id AS id", wantErr: "SyntaxError"},
		{stmt: "MATCH (n:T {id: 1}) REMOVE m.x RETURN n.id AS id", wantErr: "Variable `m` not defined"},
		{stmt: "MATCH (n:T {id: 1}) SET m:B RETURN n.id AS id", wantErr: "Variable `m` not defined"},
		{stmt: "MATCH (n:T {id: 1}) DETACH DELETE m RETURN count(*) AS c", wantErr: "refers to an undefined variable"},
		{stmt: "MATCH (n:T {id: 1}) CALL { WITH n SET n:B } RETURN n.id AS id", want: [][]interface{}{{int64(1)}}},
		{stmt: "MATCH (n:T {id: 1}) CALL { WITH n MATCH (m:U) SET m.x = n.id } RETURN n.id AS id", want: [][]interface{}{{int64(1)}}},
		{stmt: "MATCH (n:T {id: 1}) REMOVE n.x RETURN n.id AS id", want: [][]interface{}{{int64(1)}}},
	}
	stacks := map[string]func(t *testing.T) *StorageExecutor{
		"memory": func(t *testing.T) *StorageExecutor {
			exec, _ := newTestExecutor(t)
			return exec
		},
		"server stack": newSetRouteServerStackExecutor,
	}
	for stack, build := range stacks {
		for _, mode := range []string{"auto-commit", "explicit transaction"} {
			for _, tc := range cases {
				t.Run(stack+"/"+mode+"/"+tc.stmt, func(t *testing.T) {
					exec := build(t)
					ctx := context.Background()
					_, err := exec.Execute(ctx, "CREATE (:T {id: 1, x: 5}), (:T {id: 2}), (:U {id: 9})", nil)
					require.NoError(t, err)
					if mode == "explicit transaction" {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					res, err := exec.Execute(ctx, tc.stmt, nil)
					if mode == "explicit transaction" {
						if err != nil {
							_, _ = exec.Execute(ctx, "ROLLBACK", nil)
						} else {
							_, cerr := exec.Execute(ctx, "COMMIT", nil)
							require.NoError(t, cerr)
						}
					}
					if tc.wantErr != "" {
						require.Error(t, err)
						assert.Contains(t, err.Error(), tc.wantErr)
						stored, serr := exec.Execute(ctx, "MATCH (n) RETURN labels(n) AS l, properties(n) AS p", nil)
						require.NoError(t, serr)
						require.Len(t, stored.Rows, 3, "nothing deleted")
						for _, row := range stored.Rows {
							assert.NotContains(t, fmt.Sprint(row[0]), "B", "no label added")
						}
						x, xerr := exec.Execute(ctx, "MATCH (n:T {id: 1}) RETURN n.x AS x", nil)
						require.NoError(t, xerr)
						assert.Equal(t, [][]interface{}{{int64(5)}}, x.Rows, "n.x unchanged")
						return
					}
					require.NoError(t, err)
					assert.Equal(t, tc.want, res.Rows)
				})
			}
		}
	}
}
