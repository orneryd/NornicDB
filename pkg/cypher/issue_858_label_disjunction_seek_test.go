package cypher

// NornicDB #858: an equality on an indexed property combined with a
// disjunction of labels, MATCH (n) WHERE n.id = $id AND (n:A OR n:B), scanned
// every node; Neo4j answers it with one index seek per label. Graphify's
// incremental sync deletes stale nodes with this shape, once per id.

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWhereLabelDisjunction(t *testing.T) {
	for _, tc := range []struct {
		term   string
		labels []string
	}{
		{"n:Code", []string{"Code"}},
		{"n:Code OR n:Document", []string{"Code", "Document"}},
		{"(n:Code OR n:Document OR n:Entity)", []string{"Code", "Document", "Entity"}},
		{"n:Code or n:Document", []string{"Code", "Document"}},
		{"n:Code|Document", []string{"Code", "Document"}},
		{"n : Code | `Odd Label`", []string{"Code", "Odd Label"}},
		{"n:Code OR n:Document|Entity", []string{"Code", "Document", "Entity"}},
		{"n:Code:Document", nil},          // both labels, not one of them
		{"m:Code OR n:Document", nil},     // another variable
		{"n:Code OR n.id = 1", nil},       // not only labels
		{"n.id = 1", nil},                 // no label
		{"NOT n:Code", nil},               // negation
		{"n:Code|", nil},                  // malformed
		{"n:Code AND n:Document", nil},    // conjunction
		{"(n:Code) OR (n:Document)", nil}, // not recognised: no seek, still correct
	} {
		labels, ok := whereLabelDisjunction("n", tc.term)
		require.Equal(t, tc.labels != nil, ok, tc.term)
		require.Equal(t, tc.labels, labels, tc.term)
	}
	_, ok := whereLabelDisjunction("", ":Code")
	require.False(t, ok, "an anonymous pattern has no variable to test")
	labels, ok := whereLabelDisjunction("`my n`", "`my n`:Code OR `my n`:Document")
	require.True(t, ok)
	require.Equal(t, []string{"Code", "Document"}, labels)
}

func TestIssue858LabelDisjunctionSeeksIndexes(t *testing.T) {
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
				run := func(q string, params map[string]interface{}) [][]interface{} {
					t.Helper()
					if mode == "explicit transaction" {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					res, err := exec.Execute(ctx, q, params)
					if mode == "explicit transaction" {
						if err != nil {
							_, _ = exec.Execute(ctx, "ROLLBACK", nil)
						} else {
							_, commitErr := exec.Execute(ctx, "COMMIT", nil)
							require.NoError(t, commitErr)
						}
					}
					require.NoError(t, err, q)
					return res.Rows
				}
				for _, q := range []string{
					"CREATE INDEX FOR (n:Code) ON (n.id)",
					"CREATE INDEX FOR (n:Document) ON (n.id)",
					"CREATE INDEX FOR (n:Entity) ON (n.id)",
				} {
					_, err := exec.Execute(ctx, q, nil)
					require.NoError(t, err)
				}
				run(`CREATE (:Code {id: 'c1'}), (:Code {id: 'c2'}), (:Document {id: 'd1'}), (:Entity {id: 'e1'}),
					(:Code:Document {id: 'both'}), (:Other {id: 'c1'}), (:Other {id: 'o1'}), ({id: 'c2'})`, nil)
				ids := func(rows [][]interface{}) []string {
					out := make([]string, 0, len(rows))
					for _, row := range rows {
						out = append(out, fmt.Sprint(row[0], "/", row[1]))
					}
					sort.Strings(out)
					return out
				}
				q := "MATCH (n) WHERE n.id = $id AND (n:Code OR n:Document OR n:Entity) RETURN n.id, labels(n)[0]"
				require.Equal(t, []string{"c1/Code"}, ids(run(q, map[string]interface{}{"id": "c1"})), "the :Other with the same id is not a match")
				require.Equal(t, []string{"c2/Code"}, ids(run(q, map[string]interface{}{"id": "c2"})), "nor an unlabeled node")
				require.Equal(t, []string{"both/Code"}, ids(run(q, map[string]interface{}{"id": "both"})), "a node with two of the labels is one row")
				require.Empty(t, run(q, map[string]interface{}{"id": "o1"}))
				require.Equal(t, []string{"c1/Code", "e1/Entity"}, ids(run("MATCH (n) WHERE n.id IN $ids AND (n:Code OR n:Entity) RETURN n.id, labels(n)[0]", map[string]interface{}{"ids": []interface{}{"c1", "e1", "d1", "o1"}})))
				require.Equal(t, []string{"c1/Code", "c1/Other"}, ids(run("MATCH (n) WHERE n.id = $id AND (n:Code OR n:Other) RETURN n.id, labels(n)[0]", map[string]interface{}{"id": "c1"})), ":Other has no index: no seek, same result")
				require.Equal(t, []string{"c1/Code"}, ids(run("UNWIND $ids AS id MATCH (n) WHERE n.id = id AND (n:Code OR n:Document) RETURN n.id, labels(n)[0]", map[string]interface{}{"ids": []interface{}{"c1", "o1"}})))

				// Graphify's stale-node delete.
				run("UNWIND $ids AS id MATCH (n) WHERE n.id = id AND (n:Code OR n:Document OR n:Entity) DETACH DELETE n", map[string]interface{}{"ids": []interface{}{"c1", "both", "o1"}})
				require.Equal(t, []string{"c1/Other", "c2/<nil>", "c2/Code", "d1/Document", "e1/Entity", "o1/Other"},
					ids(run("MATCH (n) RETURN n.id, labels(n)[0]", nil)), "c1 and both deleted; the :Other nodes kept")
			})
		}
	}
}

// A transaction sees the nodes it created through the seek.
func TestIssue858LabelDisjunctionSeesPendingNodes(t *testing.T) {
	exec := newAsyncStackTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE INDEX FOR (n:Code) ON (n.id)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE INDEX FOR (n:Document) ON (n.id)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
	_, err = exec.Execute(ctx, "CREATE (:Document {id: 'new'})", nil)
	require.NoError(t, err)
	res, err := exec.Execute(ctx, "MATCH (n) WHERE n.id = 'new' AND (n:Code OR n:Document) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, res.Rows)
}

// The stale-node delete costs about what the labelled delete does: the
// disjunction seeks the label indexes instead of scanning every node per id.
func TestIssue858StaleDeleteCost(t *testing.T) {
	exec := newAsyncStackTestExecutor(t)
	ctx := context.Background()
	for _, q := range []string{"CREATE INDEX FOR (n:Code) ON (n.id)", "CREATE INDEX FOR (n:Document) ON (n.id)", "CREATE INDEX FOR (n:Entity) ON (n.id)"} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}
	for start := 0; start < 20000; start += 5000 {
		_, err := exec.Execute(ctx, "UNWIND range($a, $b) AS i CREATE (:Code {id: 'Code' + toString(i), body: 'some body text'})", map[string]interface{}{"a": int64(start), "b": int64(start + 4999)})
		require.NoError(t, err)
	}
	timed := func(query string, from int) time.Duration {
		ids := make([]interface{}, 0, 50)
		for i := 0; i < 50; i++ {
			ids = append(ids, fmt.Sprintf("Code%d", from+i))
		}
		start := time.Now()
		_, err := exec.Execute(ctx, query, map[string]interface{}{"ids": ids})
		require.NoError(t, err)
		return time.Since(start)
	}
	labelled := timed("UNWIND $ids AS id MATCH (n:Code) WHERE n.id = id DETACH DELETE n", 0)
	disjunction := timed("UNWIND $ids AS id MATCH (n) WHERE n.id = id AND (n:Code OR n:Document OR n:Entity) DETACH DELETE n", 100)
	require.Less(t, disjunction, 10*labelled+200*time.Millisecond, "labelled %s, disjunction %s", labelled, disjunction)
	res, err := exec.Execute(ctx, "MATCH (n:Code) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(19900)}}, res.Rows)
}
