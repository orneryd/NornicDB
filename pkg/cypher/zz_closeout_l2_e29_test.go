//go:build closeout_l2_e29

package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// E29: (A) an end-labelled self-loop pattern with ORDER BY ... LIMIT through
// the property-index end-seed top-k path returns a row whose two ends differ;
// (B) a UNION inside CALL whose first branch follows an inner CALL loses the
// second branch's rows. Database API (the entry's server routes gave the same
// results); the parser is the process's (NORNICDB_PARSER).

func l2E29Exec(t *testing.T) *StorageExecutor {
	t.Helper()
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
}

func l2E29Must(t *testing.T, exec *StorageExecutor, query string) *ExecuteResult {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, nil)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return result
}

func l2E29SetupA(t *testing.T, exec *StorageExecutor, index bool) {
	t.Helper()
	l2E29Must(t, exec, "CREATE (a:P {id:1})-[:SELF]->(b:P {id:2}), (c:P {id:3})-[:SELF]->(c)")
	l2E29Must(t, exec, "UNWIND range(4, 250) AS id CREATE (:P {id:id})")
	if index {
		l2E29Must(t, exec, "CREATE INDEX p_id FOR (p:P) ON (p.id)")
	}
}

var l2E29AStatements = []string{
	"MATCH (p)-[:SELF]->(p:P) RETURN p.id ORDER BY p.id LIMIT 1",
	"MATCH (p:P)-[:SELF]->(p) RETURN p.id ORDER BY p.id LIMIT 1",
}

const l2E29BTemplate = "MATCH (n:ZZN {v:1}) CALL { %s MATCH (n:ZZN) RETURN n.v AS v UNION WITH n RETURN n.v AS v } RETURN v ORDER BY v"

var l2E29BInners = []string{
	"CALL () { MATCH (n:ZZN) RETURN count(n) AS k }",
	"CALL () { MATCH (x:ZZN) RETURN count(x) AS k }",
	"CALL () { RETURN 1 AS k }",
}

func l2E29Rows(r *ExecuteResult) string { return fmt.Sprint(r.Rows) }

func TestL2_e29_Repro(t *testing.T) {
	var defects, seen []string

	// Part A, with the index (the defect's path).
	for _, q := range l2E29AStatements {
		exec := l2E29Exec(t)
		l2E29SetupA(t, exec, true)
		got := l2E29Rows(l2E29Must(t, exec, q))
		seen = append(seen, "A:"+got)
		if got != "[[3]]" {
			defects = append(defects, fmt.Sprintf("A %q -> %s want [[3]]", q, got))
		}
	}

	// Part B.
	for _, inner := range l2E29BInners {
		exec := l2E29Exec(t)
		l2E29Must(t, exec, "CREATE (:ZZN {v:1}), (:ZZN {v:2})")
		q := fmt.Sprintf(l2E29BTemplate, inner)
		res, err := exec.Execute(context.Background(), q, nil)
		got := ""
		if err != nil {
			got = "error " + err.Error()
		} else {
			got = l2E29Rows(res)
		}
		seen = append(seen, "B:"+got)
		if got != "[[1] [2]]" {
			defects = append(defects, fmt.Sprintf("B %q -> %s want [[1] [2]]", inner, got))
		}
	}

	verdict := "OK"
	if len(defects) > 0 {
		verdict = "DEFECT"
	}
	t.Logf("L2RESULT E29 %s parser=%s defects=%d results=%v", verdict, config.GetParserType(), len(defects), seen)
	for _, d := range defects {
		t.Error(d)
	}
}

// Control: the entry's controls - part A without the index, and with the
// index but without LIMIT, return [[3]]; part B without an inner CALL
// returns [[1],[2]].
func TestL2_e29_Control(t *testing.T) {
	for _, q := range l2E29AStatements {
		exec := l2E29Exec(t)
		l2E29SetupA(t, exec, false)
		if got := l2E29Rows(l2E29Must(t, exec, q)); got != "[[3]]" {
			t.Errorf("control A no index %q -> %s, want [[3]]", q, got)
		}
		exec = l2E29Exec(t)
		l2E29SetupA(t, exec, true)
		noLimit := q[:len(q)-len(" LIMIT 1")]
		if got := l2E29Rows(l2E29Must(t, exec, noLimit)); got != "[[3]]" {
			t.Errorf("control A index no LIMIT %q -> %s, want [[3]]", noLimit, got)
		}
	}
	exec := l2E29Exec(t)
	l2E29Must(t, exec, "CREATE (:ZZN {v:1}), (:ZZN {v:2})")
	q := fmt.Sprintf(l2E29BTemplate, "")
	if got := l2E29Rows(l2E29Must(t, exec, q)); got != "[[1] [2]]" {
		t.Errorf("control B no inner -> %s, want [[1] [2]]", got)
	}
}

// Cause A: tryExecuteTraversalSeedOrderLimit (traversal.go:688) end-seed
// branch (:763-809) takes end-node candidates from the index in order and
// returns the first `limit` reversed paths with no check that the path's first
// and last node are the same node when both ends carry one variable. The hot
// path trace shows which seed branch answered each statement, next to its rows
// with the index, without the index and inside BEGIN.
//
// Cause B is not located in the entry; the log shows the B result with each
// inner part, and with the inner CALL moved before the outer CALL, to narrow
// it to the inner CALL's position within the UNION branch.
func TestL2_e29_Cause(t *testing.T) {
	for _, q := range l2E29AStatements {
		for _, index := range []bool{true, false} {
			for _, explicit := range []bool{false, true} {
				exec := l2E29Exec(t)
				l2E29SetupA(t, exec, index)
				if explicit {
					l2E29Must(t, exec, "BEGIN")
				}
				res := l2E29Must(t, exec, q)
				trace := exec.LastHotPathTrace()
				if explicit {
					l2E29Must(t, exec, "ROLLBACK")
				}
				t.Logf("cause A parser=%s index=%t begin=%t %q -> %s endSeedTopK=%t startSeedTopK=%t",
					config.GetParserType(), index, explicit, q, l2E29Rows(res), trace.TraversalEndSeedTopK, trace.TraversalStartSeedTopK)
			}
		}
	}
	for _, inner := range append([]string{""}, l2E29BInners...) {
		for _, variant := range []string{
			fmt.Sprintf(l2E29BTemplate, inner),
			"MATCH (n:ZZN {v:1}) CALL { " + inner + " MATCH (n:ZZN) RETURN n.v AS v } RETURN v ORDER BY v",
			"MATCH (n:ZZN {v:1}) CALL { WITH n RETURN n.v AS v UNION " + inner + " MATCH (n:ZZN) RETURN n.v AS v } RETURN v ORDER BY v",
		} {
			exec := l2E29Exec(t)
			l2E29Must(t, exec, "CREATE (:ZZN {v:1}), (:ZZN {v:2})")
			res, err := exec.Execute(context.Background(), variant, nil)
			if err != nil {
				t.Logf("cause B %q -> error %v", variant, err)
				continue
			}
			t.Logf("cause B %q -> %s", variant, l2E29Rows(res))
		}
	}
}
