//go:build closeout_l2_e27

package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// E27: after DELETE n, a read of n wrapped in parentheses, a list, an index or
// a list comprehension returns []/null instead of EntityNotFound. Database API
// rows of the entry: each statement on its own new in-memory store, query
// normalization off and on; the parser is the process's (NORNICDB_PARSER).

const l2E27EntityNotFound = "Neo.ClientError.Statement.EntityNotFound"

type l2E27Case struct {
	expr    string
	collect bool // the entry's last row: WITH collect(n) AS ns
}

func (c l2E27Case) query() string {
	if c.collect {
		return "CREATE (n:EIDel {p:1}) DELETE n WITH collect(n) AS ns RETURN " + c.expr + " AS v"
	}
	return "CREATE (n:EIDel {p:1}) DELETE n RETURN " + c.expr + " AS v"
}

// The entry's defect rows (Expected EntityNotFound, got []/null).
var l2E27DefectCases = []l2E27Case{
	{expr: "labels((n))"},
	{expr: "labels(head([n]))"},
	{expr: "(n).p"},
	{expr: "n['p']"},
	{expr: "[x IN [n] | x.p]"},
}

// The entry's control rows (already EntityNotFound on 2dbc67e96).
var l2E27ControlCases = []l2E27Case{
	{expr: "labels(n)"},
	{expr: "n.p"},
	{expr: "[`x` IN ns | x.p]", collect: true},
}

// l2E27Run runs one statement on its own new in-memory store and returns the
// Neo4j status code ("" when the statement succeeded) and a printable outcome.
func l2E27Run(t *testing.T, c l2E27Case) (string, string) {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
	result, err := exec.Execute(context.Background(), c.query(), nil)
	if err != nil {
		code, _ := nornicerrors.Neo4jStatus(err)
		return code, fmt.Sprintf("error %s (%v)", code, err)
	}
	return "", fmt.Sprintf("rows %v", result.Rows)
}

func l2E27WithNormalization(t *testing.T, enabled bool) {
	t.Helper()
	prior := config.IsCypherQueryNormalizationEnabled()
	config.SetCypherQueryNormalizationEnabled(enabled)
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
}

func TestL2_e27_Repro(t *testing.T) {
	var defects []string
	total := 0
	for _, normalize := range []bool{false, true} {
		l2E27WithNormalization(t, normalize)
		for _, c := range l2E27DefectCases {
			total++
			code, outcome := l2E27Run(t, c)
			if code != l2E27EntityNotFound {
				defects = append(defects, fmt.Sprintf("normalize=%t %s -> %s", normalize, c.expr, outcome))
			}
		}
	}
	verdict := "OK"
	if len(defects) > 0 {
		verdict = "DEFECT"
	}
	t.Logf("L2RESULT E27 %s parser=%s not_entity_not_found=%d/%d %v", verdict, config.GetParserType(), len(defects), total, defects)
	for _, d := range defects {
		t.Errorf("want %s: %s", l2E27EntityNotFound, d)
	}
}

func TestL2_e27_Control(t *testing.T) {
	for _, normalize := range []bool{false, true} {
		l2E27WithNormalization(t, normalize)
		for _, c := range l2E27ControlCases {
			code, outcome := l2E27Run(t, c)
			t.Logf("control parser=%s normalize=%t %s -> %s", config.GetParserType(), normalize, c.expr, outcome)
			if code != l2E27EntityNotFound {
				t.Errorf("control normalize=%t %s: %s, want %s", normalize, c.expr, outcome, l2E27EntityNotFound)
			}
		}
		// A statement without DELETE reads the node normally.
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
		result, err := exec.Execute(context.Background(), "CREATE (n:EIDel {p:1}) RETURN labels((n)) AS l, (n).p AS p, n['p'] AS q", nil)
		if err != nil || len(result.Rows) != 1 || fmt.Sprint(result.Rows[0]) != "[[EIDel] 1 1]" {
			t.Errorf("control live node normalize=%t: rows=%v err=%v, want [[EIDel] 1 1]", normalize, result, err)
		}
	}
}

// Cause: the entry's owner (deletedEntityReadsIn, a text-shape scan of read
// sites) no longer exists at the pin; deleted reads are now decided on the
// value (deletedEntityReadFailure / deletedEntities.readsDeleted,
// deleted_entity_projection.go:404/:429). This logs, for each wrapping form,
// the outcome next to the same read reached through an alias and through a
// map, so a failing form shows which value path (container, view, alias) the
// value-based check misses.
func TestL2_e27_Cause(t *testing.T) {
	forms := []l2E27Case{
		{expr: "labels((n))"}, {expr: "labels(head([n]))"}, {expr: "(n).p"}, {expr: "n['p']"}, {expr: "[x IN [n] | x.p]"},
		{expr: "head([n]).p"}, {expr: "[n][0].p"}, {expr: "labels([n][0])"}, {expr: "labels(coalesce(n))"},
		{expr: "keys((n))"}, {expr: "properties((n))"}, {expr: "n {.*}"},
	}
	for _, c := range forms {
		_, outcome := l2E27Run(t, c)
		t.Logf("cause parser=%s %s -> %s", config.GetParserType(), c.expr, outcome)
	}
}
