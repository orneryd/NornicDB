package bolt

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// NOR-412 round 2: no transaction is refused because of the size of its
// journal record. Every transaction here is committed by b6d8e762a; before
// this change each was refused at COMMIT with "wal: entry exceeds maximum WAL
// frame size of 16777216 bytes". Real on-disk database, database manager,
// Bolt server and official driver (kvLab), with a clean restart after each.

// runMany runs statements in one explicit transaction, or each as its own
// auto-commit statement when explicit is false.
func (l *kvLab) runMany(explicit bool, statements []string, params []map[string]any) error {
	ctx := context.Background()
	session := l.d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer session.Close(ctx)
	if !explicit {
		for i, q := range statements {
			r, err := session.Run(ctx, q, params[i])
			if err != nil {
				return fmt.Errorf("RUN %d: %w", i, err)
			}
			if _, err := r.Consume(ctx); err != nil {
				return fmt.Errorf("PULL %d: %w", i, err)
			}
		}
		return nil
	}
	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("BEGIN: %w", err)
	}
	defer tx.Close(ctx)
	for i, q := range statements {
		r, err := tx.Run(ctx, q, params[i])
		if err != nil {
			return fmt.Errorf("RUN %d: %w", i, err)
		}
		if _, err := r.Consume(ctx); err != nil {
			return fmt.Errorf("PULL %d: %w", i, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("COMMIT: %w", err)
	}
	return nil
}

func (l *kvLab) count(query string, params map[string]any) int64 {
	l.t.Helper()
	rows := l.mustRun(query, params)
	require.Len(l.t, rows, 1)
	return rows[0][0].(int64)
}

// seedDocs creates n nodes of label with textBytes of text, grp = i % 16 and
// scalars, in batches small enough for any build, then gives each one chunk
// vector of 2,048 dimensions through the embedding sidecar.
func (l *kvLab) seedDocs(label string, n, textBytes int) {
	l.t.Helper()
	text := strings.Repeat("w", textBytes)
	l.mustRun("CREATE INDEX "+strings.ToLower(label)+"_id IF NOT EXISTS FOR (n:"+label+") ON (n.id)", nil)
	for from := 0; from < n; from += 500 {
		l.mustRun("UNWIND range($from, $to - 1) AS i CREATE (:"+label+" {id: i, grp: i % 16, text: $text, flag: false, a: 0, b: 0})", map[string]any{"from": from, "to": min(from+500, n), "text": text})
	}
	l.embed(label, 1)
}

// Updates of 8,000, 16,000 and 32,000 nodes of 2 KB with one kept vector
// each, as one statement and as 16 statements in one explicit transaction.
func TestLargeTransactionUpdatesCommit(t *testing.T) {
	lab := kvOpen(t, t.TempDir(), kvConfig())
	lab.seedDocs("Doc", 32000, 2000)
	value := 0
	for _, n := range []int{8000, 16000, 32000} {
		for _, form := range []string{"one-statement", "sixteen-statements"} {
			value++
			var statements []string
			var params []map[string]any
			if form == "one-statement" {
				statements = []string{"MATCH (n:Doc) WHERE n.id < $n SET n.flag = $f, n.a = $v, n.b = $v"}
				params = []map[string]any{{"n": n, "v": value, "f": value%2 == 0}}
			} else {
				for g := 0; g < 16; g++ {
					statements = append(statements, "MATCH (n:Doc) WHERE n.id < $n AND n.grp = $g SET n.flag = $f, n.a = $v, n.b = $v")
					params = append(params, map[string]any{"n": n, "g": g, "v": value, "f": value%2 == 0})
				}
			}
			seq, _ := lab.journalSince(0)
			err := lab.runMany(true, statements, params)
			_, largest := lab.journalSince(seq)
			t.Logf("SPANNED update N=%d form=%s error=%v largestEntityRecord=%d", n, form, err, largest)
			require.NoError(t, err, "%d nodes, %s", n, form)
			require.Equal(t, int64(n), lab.count("MATCH (n:Doc) WHERE n.a = $v RETURN count(n)", map[string]any{"v": value}))
		}
	}
	lab = lab.reopen(kvConfig())
	require.Equal(t, int64(32000), lab.count("MATCH (n:Doc) WHERE n.a = $v RETURN count(n)", map[string]any{"v": value}))
	lab.requireVectors("Doc", 32000, 1)
}

// Creates of 10,000 and 20,000 nodes of 2 KB in one explicit statement.
func TestLargeTransactionCreatesCommit(t *testing.T) {
	lab := kvOpen(t, t.TempDir(), kvConfig())
	text := strings.Repeat("w", 2000)
	for _, n := range []int{10000, 20000} {
		label := fmt.Sprintf("New%d", n)
		seq, _ := lab.journalSince(0)
		err := lab.runMany(true, []string{"UNWIND range(1, $n) AS i CREATE (:" + label + " {i: i, text: $text})"}, []map[string]any{{"n": n, "text": text}})
		_, largest := lab.journalSince(seq)
		t.Logf("SPANNED create N=%d error=%v largestEntityRecord=%d", n, err, largest)
		require.NoError(t, err)
		require.Equal(t, int64(n), lab.count("MATCH (n:"+label+") RETURN count(n)", nil))
	}
	lab = lab.reopen(kvConfig())
	require.Equal(t, int64(10000), lab.count("MATCH (n:New10000) RETURN count(n)", nil))
	require.Equal(t, int64(20000), lab.count("MATCH (n:New20000) RETURN count(n)", nil))
}

// One explicit transaction mixing a node with 30 MB of text, 6,000 new nodes
// and 6,000 new relationships, and updates of existing embedded nodes.
func TestLargeTransactionMixedCommits(t *testing.T) {
	lab := kvOpen(t, t.TempDir(), kvConfig())
	lab.seedDocs("Doc", 2000, 2000)
	statements := []string{
		"CREATE (:Big {id: 'big', text: $text})",
		"UNWIND range(1, 6000) AS i CREATE (a:Item {i: i, text: $small}) WITH collect(a) AS items UNWIND range(0, size(items) - 2) AS k WITH items[k] AS a, items[k + 1] AS b, k CREATE (a)-[:NEXT {w: k, note: $small}]->(b)",
		"MATCH (b:Big {id: 'big'}), (i:Item) WHERE i.i <= 1000 CREATE (b)-[:HAS]->(i)",
		"MATCH (n:Doc) SET n.flag = true, n.a = 7",
	}
	small := strings.Repeat("s", 2000)
	params := []map[string]any{{"text": strings.Repeat("b", 30<<20)}, {"small": small}, nil, nil}
	seq, _ := lab.journalSince(0)
	err := lab.runMany(true, statements, params)
	_, largest := lab.journalSince(seq)
	t.Logf("SPANNED mixed error=%v largestEntityRecord=%d", err, largest)
	require.NoError(t, err)
	check := func(stage string) {
		require.Equal(t, int64(6000), lab.count("MATCH (i:Item) RETURN count(i)", nil), stage)
		require.Equal(t, int64(5999), lab.count("MATCH ()-[r:NEXT]->() RETURN count(r)", nil), stage)
		require.Equal(t, int64(1000), lab.count("MATCH (:Big)-[r:HAS]->() RETURN count(r)", nil), stage)
		require.Equal(t, int64(30<<20), lab.count("MATCH (b:Big) RETURN size(b.text)", nil), stage)
		require.Equal(t, int64(2000), lab.count("MATCH (n:Doc) WHERE n.a = 7 RETURN count(n)", nil), stage)
	}
	check("commit")
	lab = lab.reopen(kvConfig())
	check("restart")
	lab.requireVectors("Doc", 2000, 1)
}

// The reported shape: 8,023 existing nodes of four labels, about 1.77 KB of
// stored properties each (largest about 2.9 KB, mostly one text property),
// one kept 2,048-dim vector each, about 16,000 relationships among them; one
// explicit transaction of 16 statements, each `SET n += $map` plus scalar
// fields on its share of the nodes and a property on their relationships.
func TestLargeTransactionReportedShapeCommits(t *testing.T) {
	lab := kvOpen(t, t.TempDir(), kvConfig())
	labels := []string{"Part", "Section", "Note", "Digest"}
	const total = 8023
	for li, label := range labels {
		lab.mustRun("CREATE INDEX "+strings.ToLower(label)+"_id IF NOT EXISTS FOR (n:"+label+") ON (n.id)", nil)
		from, to := li*total/4, (li+1)*total/4
		for start := from; start < to; start += 500 {
			lab.mustRun(`UNWIND range($from, $to - 1) AS i
				CREATE (:`+label+` {id: i, grp: i % 16, kind: $label, ord: i, title: 'title ' + toString(i),
					text: CASE WHEN i % 50 = 0 THEN $long ELSE $text END, flag: false, rev: 0})`,
				map[string]any{"from": start, "to": min(start+500, to), "label": label, "text": strings.Repeat("t", 1650), "long": strings.Repeat("t", 2800)})
		}
		lab.embed(label, 1)
	}
	for _, label := range labels {
		for _, offset := range []int{1, 2} {
			lab.mustRun("MATCH (a:"+label+") WITH a MATCH (b:"+label+" {id: a.id + $o}) CREATE (a)-[:LINKS {rev: 0}]->(b)", map[string]any{"o": offset})
		}
	}
	links := lab.count("MATCH ()-[r:LINKS]->() RETURN count(r)", nil)
	t.Logf("SPANNED shape nodes=%d links=%d", total, links)
	var statements []string
	var params []map[string]any
	for g := 0; g < 16; g++ {
		statements = append(statements, `MATCH (n) WHERE n.grp = $g AND n.kind IS NOT NULL
			SET n += $map, n.flag = true, n.rev = $rev
			WITH n MATCH (n)-[r:LINKS]->() SET r.rev = $rev`)
		params = append(params, map[string]any{"g": g, "rev": 1, "map": map[string]any{"status": "reviewed", "score": 0.5, "tags": []any{"x", "y"}}})
	}
	seq, _ := lab.journalSince(0)
	err := lab.runMany(true, statements, params)
	_, largest := lab.journalSince(seq)
	t.Logf("SPANNED shape error=%v largestEntityRecord=%d", err, largest)
	require.NoError(t, err)
	check := func(stage string) {
		require.Equal(t, int64(total), lab.count("MATCH (n) WHERE n.rev = 1 AND n.status = 'reviewed' RETURN count(n)", nil), stage)
		require.Equal(t, links, lab.count("MATCH ()-[r:LINKS]->() WHERE r.rev = 1 RETURN count(r)", nil), stage)
	}
	check("commit")
	lab = lab.reopen(kvConfig())
	check("restart")
	for _, label := range labels {
		lab.requireVectors(label, len(lab.nodesByLabel(label)), 1)
	}
}

// A transaction whose statements touch the same nodes several times commits;
// as on main, the record carries one image per change. Measured on 2,023
// nodes of about 1.77 KB.
func TestRepeatedTouchesAreJournaledOncePerChange(t *testing.T) {
	lab := kvOpen(t, t.TempDir(), kvConfig())
	const n = 2023
	lab.seedDocs("Doc", n, 1650)
	lab.mustRun("UNWIND range(0, $n - 1) AS i CREATE (:Peer {id: i})", map[string]any{"n": n})
	measure := func(name string, statements []string, params []map[string]any) int {
		seq, _ := lab.journalSince(0)
		require.NoError(t, lab.runMany(true, statements, params), name)
		_, largest := lab.journalSince(seq)
		t.Logf("SPANNED touches case=%s nodes=%d entityRecord=%d perNode=%d", name, n, largest, largest/n)
		return largest
	}
	touch := func(k, v int) (string, map[string]any) {
		return fmt.Sprintf("MATCH (n:Doc) SET n.t%d = $v", k), map[string]any{"v": v}
	}
	q1, p1 := touch(1, 1)
	one := measure("one", []string{q1}, []map[string]any{p1})
	q1, p1 = touch(1, 2)
	q2, p2 := touch(2, 2)
	two := measure("two", []string{q1, q2}, []map[string]any{p1, p2})
	q1, p1 = touch(1, 3)
	q2, p2 = touch(2, 3)
	q3, p3 := touch(3, 3)
	three := measure("three", []string{q1, q2, q3}, []map[string]any{p1, p2, p3})
	same := measure("same-statement-twice", []string{"MATCH (n:Doc) SET n.s = $v, n.t1 = $w", "MATCH (n:Doc) SET n.s = $v, n.t1 = $w"}, []map[string]any{{"v": 1, "w": 9}, {"v": 1, "w": 9}})
	merge := measure("merge-on-match-then-set", []string{"UNWIND range(0, $n - 1) AS i MERGE (d:Doc {id: i}) ON MATCH SET d.m = $v SET d.t2 = $v"}, []map[string]any{{"n": n, "v": 4}})
	rel := measure("update-and-relationship", []string{"MATCH (d:Doc), (p:Peer {id: d.id}) SET d.t3 = $v CREATE (d)-[:SEES]->(p)"}, []map[string]any{{"v": 5}})
	// As on main: every statement that changes a node journals one image
	// of it; a write that changes nothing is skipped.
	require.GreaterOrEqual(t, two, one*19/10, "two touches journal two images per node")
	require.GreaterOrEqual(t, three, one*28/10, "three touches journal three images per node")
	require.LessOrEqual(t, same, one+one/10, "an unchanged second write is skipped")
	require.GreaterOrEqual(t, merge, one*19/10, "MERGE ON MATCH then SET journals two images per node")
	require.LessOrEqual(t, rel, one+one/10+n*400, "update plus relationship journals one node image and the relationship")
	lab = lab.reopen(kvConfig())
	require.Equal(t, int64(n), lab.count("MATCH (d:Doc) WHERE d.t1 = 9 AND d.t2 = 4 AND d.t3 = 5 AND d.m = 4 RETURN count(d)", nil))
	require.Equal(t, int64(n), lab.count("MATCH (:Doc)-[r:SEES]->(:Peer) RETURN count(r)", nil))
	lab.requireVectors("Doc", n, 1)
}
