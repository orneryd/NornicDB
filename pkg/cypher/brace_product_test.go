package cypher

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// Author: #1663 implementation lane. Product scope confirmed by Nornic2.
func TestBraceProductRetainsOriginalFacts(t *testing.T) {
	query := "WITH {k: 1} AS count CALL () { MATCH (n {x: {y: 2}}) RETURN n } RETURN count{.k}"
	ctx, source := cyphertext.WithSource(t.Context(), query)
	ctx, syntax, err := admitBraceSyntax(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		text string
		kind mapBraceKind
	}{
		{"{k: 1}", literalBrace}, {"{ MATCH", subqueryBrace}, {"{x:", patternPropertyBrace}, {"{y:", literalBrace}, {"{.k}", projectionBrace},
	} {
		open := strings.Index(query, item.text)
		fact, found := syntax.at(source, open)
		if !found || fact.kind != item.kind {
			t.Errorf("%s: kind=%v found=%v", item.text, fact.kind, found)
		}
		view := source.Window(open, len(query))
		projected, found := syntax.at(view, 0)
		if !found || projected.opening != fact.opening || projected.kind != fact.kind {
			t.Errorf("window changed fact: %s", item.text)
		}
	}
	_, retained, err := admitBraceSyntax(ctx, source.Window(strings.Index(query, "MATCH"), strings.Index(query, " RETURN n")))
	if err != nil || retained != syntax {
		t.Fatalf("slice reparsed its own role/admission: %v", err)
	}
}

func TestBraceProductProjectsCopiesWithoutAdmission(t *testing.T) {
	query := "MATCH (a)-->(b {k: coalesce(a.k, {x: 1})}) RETURN b"
	source := cyphertext.NormalizeSource(query)
	syntax := readBraceSyntax(source)
	if syntax.err != nil {
		t.Fatal(syntax.err)
	}
	text, rewrite, err := desugarLabelExpressions(query, nil, false)
	if err != nil || rewrite == nil {
		t.Fatalf("rewrite: %v", err)
	}
	view := sourceRewriteView(source, rewrite)
	open := strings.Index(text, "{x: 1}")
	fact, found := syntax.at(view, open)
	if !found || fact.kind != literalBrace {
		t.Fatalf("copy lost original brace: %+v %v", fact, found)
	}
	entry, found := syntax.entryAt(view, cyphertext.Span{Start: open + 1, End: open + 2})
	if !found || entry.accepted.Kind != keyedBraceEntry {
		t.Fatalf("copy lost original entry: %+v %v", entry, found)
	}
	if got := query[entry.name.Start:entry.name.End]; got != "x" {
		t.Errorf("entry name origin: %s", got)
	}
}

func TestBraceProductAdmissionPrecedesEmptyExecution(t *testing.T) {
	for _, query := range []string{
		"UNWIND [] AS x MATCH (a)-->(b {k: a.k,}) RETURN b",
		"CALL () { RETURN {x: {y: 1,}} AS out } RETURN out",
		"WITH {x: 1} AS n UNWIND [] AS x RETURN n{.x,} AS out",
		"RETURN {x: {y: 1} AS out",
	} {
		if syntax := readBraceSyntax(cyphertext.NormalizeSource(query)); syntax.err == nil {
			t.Errorf("admitted malformed original source: %s", query)
		}
	}
}
