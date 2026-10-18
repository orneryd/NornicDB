package cypher

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func countGrammarParses(t *testing.T) *atomic.Int64 {
	t.Helper()
	previousParser := config.GetParserType()
	config.SetParserType(config.ParserTypeANTLR)
	var parses atomic.Int64
	validate := grammarValidate
	grammarValidate = func(query string) error {
		parses.Add(1)
		return validate(query)
	}
	t.Cleanup(func() {
		grammarValidate = validate
		config.SetParserType(previousParser)
	})
	return &parses
}

// Under NORNICDB_PARSER=antlr only the user's statement gets the grammar
// check: the texts NornicDB builds from it while it runs (a CALL body per
// row, UNION branches, subquery seeds) get the Nornic validator. They used to
// cost a cold grammar parse each, several per large statement, and most of
// its prediction state.
func TestANTLRGrammarChecksOnlyTheUserStatement(t *testing.T) {
	parses := countGrammarParses(t)
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	if _, err := exec.Execute(ctx, "UNWIND range(1, 3) AS i CREATE (:G {i: i})-[:R]->(:H {i: i})", nil); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		"MATCH (g:G) CALL (g) { MATCH (g)-[:R]->(h) RETURN h.i AS hi } RETURN g.i AS i, hi ORDER BY i",
		"MATCH (g:G) RETURN g.i AS i UNION MATCH (h:H) RETURN h.i AS i",
		"MATCH (g:G) RETURN g.i AS i, COUNT { MATCH (g)-[:R]->() } AS c ORDER BY i",
	} {
		before := parses.Load()
		result, err := exec.Execute(ctx, query, nil)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if len(result.Rows) == 0 {
			t.Fatalf("%s: no rows", query)
		}
		if got := parses.Load() - before; got != 1 {
			t.Fatalf("%s: %d grammar parses, want 1 (the user's statement)", query, got)
		}
	}
	// A statement a procedure runs is a user's statement: the grammar still
	// checks it, and still refuses what it refuses.
	before := parses.Load()
	if _, err := exec.executeUserStatement(ctx, "MATCH (g:G) RETURN count(g) AS c", nil); err != nil {
		t.Fatal(err)
	}
	if got := parses.Load() - before; got != 1 {
		t.Fatalf("executeUserStatement: %d grammar parses, want 1", got)
	}
	if _, err := exec.executeUserStatement(ctx, "MATCH (g:G) RETRUN g", nil); err == nil {
		t.Fatalf("executeUserStatement accepted a statement the grammar rejects")
	}
}

var internalTextStringLiteral = regexp.MustCompile(`'[^']*'|"[^"]*"`)

// An internal text skips the grammar check only because it is built from a
// statement the grammar already checked: from that statement's text, never
// from values. Every internal text a statement builds while it runs (CALL
// bodies per row, UNION branches, subquery bodies) must hold no parameter or
// stored value, and no string literal its statement doesn't hold. Values
// travel as parameters ($name) or row bindings.
func TestANTLRSkippedInternalTextsHoldOnlyStatementText(t *testing.T) {
	countGrammarParses(t)
	const paramSentinel = "PARAM_SENTINEL_5172"
	const dataSentinel = "DATA_SENTINEL_9304"
	var mu sync.Mutex
	var built []string
	internalTextChecked = func(text string) {
		mu.Lock()
		built = append(built, text)
		mu.Unlock()
	}
	t.Cleanup(func() { internalTextChecked = nil })

	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	if _, err := exec.Execute(ctx, "UNWIND range(1, 3) AS i CREATE (:G {i: i, tag: $tag})-[:R]->(:H {i: i, tag: $tag})", map[string]interface{}{"tag": dataSentinel}); err != nil {
		t.Fatal(err)
	}
	params := map[string]interface{}{"p": paramSentinel, "n": int64(2), "names": []interface{}{paramSentinel, "x"}}
	statements := []string{
		"MATCH (g:G) CALL (g) { MATCH (g)-[:R]->(h) WHERE h.tag <> $p RETURN h.tag AS ht } RETURN g.i AS i, ht ORDER BY i",
		"MATCH (g:G) CALL { WITH g MATCH (g)-[:R]->(h) RETURN h.i AS hi } RETURN g.i AS i, hi ORDER BY i",
		"MATCH (g:G) WITH g, g.tag AS tag CALL { MATCH (h:H) WHERE h.tag = tag AND h.i >= $n RETURN count(h) AS c } RETURN g.i AS i, c ORDER BY i",
		"MATCH (g:G) WHERE g.tag <> $p RETURN g.tag AS v UNION MATCH (h:H) WHERE h.i = $n RETURN h.tag AS v",
		"MATCH (g:G) RETURN g.i AS i, COUNT { MATCH (g)-[:R]->(h) WHERE h.tag IN $names OR h.tag = g.tag } AS c ORDER BY i",
		"MATCH (g:G) RETURN g.i AS i, COLLECT { MATCH (g)-[:R]->(h) WHERE h.i >= $n RETURN h.tag } AS tags ORDER BY i",
		"MATCH (g:G) WITH g WHERE EXISTS { MATCH (g)-[:R]->(h:H {tag: g.tag}) WHERE h.tag <> $p } RETURN count(g) AS c",
		"UNWIND $names AS name CALL (name) { WITH name WHERE name = $p RETURN name AS hit } RETURN hit",
		// An optional import resolved per seed (the seed travels as a
		// parameter).
		"MATCH (g:G) OPTIONAL MATCH (g)-[:R]->(h:H) WHERE h.tag <> $p WITH g, h CALL { WITH g, h RETURN h.i AS hi } RETURN g.i AS i, hi ORDER BY i",
	}
	total := 0
	for _, statement := range statements {
		mu.Lock()
		built = built[:0]
		mu.Unlock()
		if _, err := exec.Execute(ctx, statement, params); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
		mu.Lock()
		texts := append([]string(nil), built...)
		mu.Unlock()
		for _, text := range texts {
			if strings.Contains(text, paramSentinel) || strings.Contains(text, dataSentinel) {
				t.Fatalf("statement %q built an internal text holding a value:\n%s", statement, text)
			}
			for _, literal := range internalTextStringLiteral.FindAllString(text, -1) {
				if !strings.Contains(statement, literal) {
					t.Fatalf("statement %q built an internal text with the literal %s it doesn't hold:\n%s", statement, literal, text)
				}
			}
		}
		total += len(texts)
		t.Logf("%d internal texts for %s", len(texts), statement)
	}
	if total == 0 {
		t.Fatalf("no statement built an internal text; the test checks nothing")
	}
}

// The Fabric entry checks the syntax of an EXPLAIN or PROFILE statement
// only; any other statement's fragments get the configured parser's check,
// as a user's statement does, so the grammar still refuses what it refuses.
// A fragment text repeated per record is checked once.
func TestANTLRGrammarChecksFabricFragments(t *testing.T) {
	parses := countGrammarParses(t)
	base := newTestMemoryEngine(t)
	mgr, err := multidb.NewDatabaseManager(base, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.CreateDatabase("leaf_a"); err != nil {
		t.Fatal(err)
	}
	if err := mgr.CreateCompositeDatabase("cmp", []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: "leaf_a", Type: "local", AccessMode: "read_write"},
	}); err != nil {
		t.Fatal(err)
	}
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	ctx := context.Background()

	for _, query := range []string{
		"USE cmp.a RETURN 1 AS one",
		"USE cmp CALL { USE cmp.a RETURN 1 AS v } RETURN v",
	} {
		before := parses.Load()
		if _, err := exec.Execute(ctx, query, nil); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if got := parses.Load() - before; got == 0 {
			t.Fatalf("%s: no grammar check", query)
		}
	}

	before := parses.Load()
	result, err := exec.Execute(ctx, "UNWIND range(1, 10) AS i CALL { USE cmp.a WITH i RETURN i * 2 AS v } RETURN v", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 10 {
		t.Fatalf("per-record fragment: %d rows, want 10", len(result.Rows))
	}
	if got := parses.Load() - before; got == 0 || got >= 10 {
		t.Fatalf("per-record fragment: %d grammar parses for 10 records, want at least one and one per distinct text", got)
	}

	// The Nornic validator accepts LIMIT before OFFSET; the grammar doesn't.
	if _, err := exec.Execute(ctx, "USE cmp.a RETURN 1 AS one LIMIT 1 OFFSET 1", nil); err == nil {
		t.Fatalf("a fragment the grammar rejects ran without its check")
	}
}
