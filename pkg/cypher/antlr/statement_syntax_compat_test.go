// Author: R17 implementation lane, 1 November 2026.
package antlr

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"reflect"
	"strings"
	"testing"
)

func TestCompatibilityViewsKeepDe70EntryContracts(t *testing.T) {
	cases := []struct {
		source, window string
		entry          SyntaxEntry
	}{
		{"RETURN 0 AS before; RETURN 1 AS after", "RETURN 1 AS after", StatementEntry},
		{"MATCH (n) WITH n RETURN n", "WITH n RETURN n", StatementEntry},
		{"CALL () { UNWIND [1,2] AS x RETURN x } RETURN x", "UNWIND [1,2] AS x RETURN x", StatementEntry},
		{"MERGE (n:X) WITH n MERGE (n:X) RETURN n", "MERGE (n:X)", ClauseEntry},
		{"MATCH (n) SET n = {x: 1} RETURN n", "SET n = {x: 1}", ClauseEntry},
		{"RETURN 1 + 2 AS x", "1 + 2", ExpressionEntry},
		{"WITH 1 AS value CALL db.labels() YIELD label RETURN *", "CALL db.labels()", FragmentEntry},
		{"MATCH (n:X {id: 1}) RETURN n", "(n:X {id: 1})", PatternEntry},
		{"USE graph.byName('tenant') RETURN 1", "USE graph.byName('tenant') RETURN 1", UseEntry},
	}
	for _, c := range cases {
		t.Run(c.window, func(t *testing.T) {
			whole, err := parseSyntaxStandalone(context.Background(), c.source, StatementEntry, false, AcceptedSource)
			if err != nil {
				t.Fatal(err)
			}
			direct, err := parseSyntaxStandalone(context.Background(), c.window, c.entry, false, AcceptedSource)
			if err != nil {
				t.Fatal(err)
			}
			for from := 0; from <= len(c.source)-len(c.window); {
				at := strings.Index(c.source[from:], c.window)
				if at < 0 {
					break
				}
				at += from
				derived, ok := whole.CompatibilityView(at, at+len(c.window), c.entry)
				if !ok {
					t.Fatalf("no derived view at %d", at)
				}
				if &derived.data.nodes[0] != &whole.data.nodes[0] || &derived.data.tokens[0] != &whole.data.tokens[0] {
					t.Fatal("view copied parser arena")
				}
				if derived.Source() != c.window {
					t.Fatalf("source %q", derived.Source())
				}
				if got, want := compatibilityFacts(derived), compatibilityFacts(direct); !reflect.DeepEqual(got, want) {
					t.Fatalf("derived facts %v; standalone %v", got, want)
				}
				from = at + 1
			}
		})
	}
}

func compatibilityFacts(syntax StatementSyntax) []any {
	var facts []any
	syntax.Walk(func(node SyntaxNode) func() {
		// Container shape can be projected; every expression/name/clause remains
		// the exact grammar fact the de70 consumer observes.
		switch node.Rule() {
		case CypherParserRULE_expression, CypherParserRULE_symbol, CypherParserRULE_mergeSt, CypherParserRULE_setSt, CypherParserRULE_returnSt, CypherParserRULE_useClause:
			a, b := node.Span()
			name, hasName := node.Name()
			facts = append(facts, []any{node.Rule(), a, b, node.Text(), node.Code(), name, hasName})
		}
		return nil
	})
	return facts
}

func TestCompatibilityProjectionPreservesNamesAndExpressionText(t *testing.T) {
	ctx := context.Background()
	original := "RETURN /* gap */ n.old AS value"
	syntax, err := parseSyntaxStandalone(ctx, original, StatementEntry, false, AcceptedSource)
	if err != nil {
		t.Fatal(err)
	}
	source := cyphertext.NewSource(ctx, original, AcceptedSource)
	oldStart := strings.Index(original, "old")
	gapStart := strings.Index(original, "/*")
	gapEnd := strings.Index(original, "*/") + 2
	withoutGap := original[:gapStart] + original[gapEnd:]
	current := source.Rewrite(withoutGap, []cyphertext.SourceEdit{{Original: cyphertext.Span{Start: gapStart, End: gapEnd}, Accepted: cyphertext.Span{Start: gapStart, End: gapStart}}})
	newStart := oldStart - (gapEnd - gapStart)
	current = current.Rewrite(withoutGap[:newStart]+"newLong"+withoutGap[newStart+3:], []cyphertext.SourceEdit{{Original: cyphertext.Span{Start: newStart, End: newStart + 3}, Accepted: cyphertext.Span{Start: newStart, End: newStart + 7}}})
	projected, ok := syntax.ProjectSyntax(current)
	if !ok {
		t.Fatal("no retained projection")
	}
	direct, err := parseSyntaxStandalone(ctx, current.Text, StatementEntry, false, AcceptedSource)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(compatibilityFacts(projected), compatibilityFacts(direct)) {
		t.Fatalf("projected %v; direct %v", compatibilityFacts(projected), compatibilityFacts(direct))
	}
}

func TestSyntaxOwnerDoesNotParseAnOverlappingFallback(t *testing.T) {
	ctx := WithSyntaxOwner(context.Background(), "RETURN 1 + 2 AS x")
	whole, err := ParseSyntax(ctx, "RETURN 1 + 2 AS x", StatementEntry, false, AcceptedSource)
	if err != nil {
		t.Fatal(err)
	}
	expression, err := ParseSyntax(ctx, "1 + 2", ExpressionEntry, false, AcceptedSource)
	if err != nil {
		t.Fatal(err)
	}
	if &whole.data.nodes[0] != &expression.data.nodes[0] {
		t.Fatal("second owner")
	}
	if _, err := ParseSyntax(ctx, "1 + 3", ExpressionEntry, false, AcceptedSource); err == nil {
		t.Fatal("unrelated text acquired a fallback")
	}
}

func TestCompatibilityRawSourceKeepsAcceptedCode(t *testing.T) {
	syntax, err := parseSyntaxStandalone(context.Background(), "RETURN （1 + 2） AS x", StatementEntry, false, RawSource)
	if err != nil {
		t.Fatal(err)
	}
	var code string
	syntax.Walk(func(node SyntaxNode) func() {
		if code == "" && node.Rule() == CypherParserRULE_expression {
			code = node.Code()
		}
		return nil
	})
	if code != "(1 + 2)" {
		t.Fatalf("accepted code %q", code)
	}
}

func TestCompatibilityProjectionAdaptersKeepLocalFacts(t *testing.T) {
	source := "WITH 1 AS n RETURN n AS x, n /* hidden */ + 1 AS total"
	ctx, view := cyphertext.WithSource(context.Background(), source)
	ctx = WithSyntaxOwner(ctx, view.Text)
	for _, item := range []string{"n AS x", "n /* hidden */ + 1 AS total", "n /* hidden */ + 1", "n   + 1"} {
		t.Run(item, func(t *testing.T) {
			derived, err := ProjectionClauseSyntax(ctx, item, false)
			if err != nil {
				t.Fatal(err)
			}
			direct, err := ProjectionClauseSyntax(context.Background(), item, false)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := compatibilityFacts(StatementSyntax{derived.data}), compatibilityFacts(StatementSyntax{direct.data}); !reflect.DeepEqual(got, want) {
				t.Fatalf("derived %v; direct %v", got, want)
			}
			a, b := derived.Span()
			if a != 0 || b != len("RETURN "+item) || derived.Code() != direct.Code() {
				t.Fatalf("clause frame %d:%d code %q vs %q", a, b, derived.Code(), direct.Code())
			}
		})
	}
}

func TestCompatibilityCodeWindowsKeepOneArena(t *testing.T) {
	text := "WITH 1 AS n CALL () { RETURN n /* gap */ + 1 AS total } RETURN total"
	ctx, source := cyphertext.WithSource(context.Background(), text)
	ctx = WithSyntaxOwner(ctx, source.Text)
	whole, err := ParseSyntax(ctx, text, StatementEntry, false, AcceptedSource)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		text  string
		entry SyntaxEntry
	}{
		{"n   + 1", ExpressionEntry},
		{"RETURN n   + 1 AS total", StatementEntry},
	} {
		derived, err := ParseSyntax(ctx, c.text, c.entry, false, AcceptedSource)
		if err != nil {
			t.Fatal(err)
		}
		direct, err := parseSyntaxStandalone(context.Background(), c.text, c.entry, false, AcceptedSource)
		if err != nil {
			t.Fatal(err)
		}
		if &whole.data.nodes[0] != &derived.data.nodes[0] {
			t.Fatal("Code view acquired another arena")
		}
		if !reflect.DeepEqual(compatibilityFacts(derived), compatibilityFacts(direct)) {
			t.Fatalf("derived %v; direct %v", compatibilityFacts(derived), compatibilityFacts(direct))
		}
	}
}

func TestCompatibilityCallScopeReadsRealRoles(t *testing.T) {
	for _, scope := range []string{"", "*", "x", "x, `y z`", "true, null"} {
		source := "WITH 1 AS x CALL (" + scope + ") { RETURN 1 AS y } RETURN y"
		ctx, view := cyphertext.WithSource(context.Background(), source)
		ctx = WithSyntaxOwner(ctx, view.Text)
		call, err := CallScopeSyntax(ctx, scope, false)
		if err != nil {
			t.Fatal(scope, err)
		}
		direct, err := CallScopeSyntax(context.Background(), scope, false)
		if err != nil {
			t.Fatal(scope, err)
		}
		var got, want []string
		for i := 0; i < call.ChildCount(); i++ {
			if name, ok := call.Child(i).Name(); ok {
				got = append(got, name)
			}
		}
		for i := 0; i < direct.ChildCount(); i++ {
			if name, ok := direct.Child(i).Name(); ok {
				want = append(want, name)
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("scope %q: %v vs %v", scope, got, want)
		}
	}
}

func TestCompatibilityLateAcquisitionUsesPreparedInputOnce(t *testing.T) {
	ctx, source := cyphertext.WithSource(context.Background(), "RETURN s'hello {1}' AS value")
	ctx = WithSyntaxOwner(ctx, source.Text)
	accepted := "RETURN 'hello ' + __nornic_interpolate(1) AS value"
	rewrite := source.Rewrite(accepted, []cyphertext.SourceEdit{{Original: cyphertext.Span{Start: 7, End: strings.Index(source.Text, " AS value")}, Accepted: cyphertext.Span{Start: 7, End: strings.Index(accepted, " AS value")}}})
	ctx = WithSyntaxProgram(ctx, rewrite)
	ctx = cyphertext.WithSourceView(ctx, rewrite)
	syntax, err := ParseSyntax(ctx, accepted, StatementEntry, true, AcceptedSource)
	if err != nil {
		t.Fatal(err)
	}
	owner := ctx.Value(syntaxOwnerKey{}).(*syntaxOwner)
	if owner.syntax.data.source != accepted || &owner.syntax.data.nodes[0] != &syntax.data.nodes[0] {
		t.Fatal("prepared input did not own the retained product")
	}
}

func TestSyntaxOwnerRegistryRetainsDistinctProgramsAndWindows(t *testing.T) {
	original := "WITH 1 AS n CALL () { RETURN n + 1 AS total } RETURN total"
	ctx, source := cyphertext.WithSource(context.Background(), original)
	ctx = WithNewSyntaxExecution(ctx, source.Text)
	whole, err := ParseSyntax(ctx, original, StatementEntry, false, AcceptedSource)
	if err != nil {
		t.Fatal(err)
	}
	root := ctx.Value(syntaxOwnerKey{}).(*syntaxOwner)
	// A separately declared execution of an existing window derives its arena.
	childText := "RETURN n + 1 AS total"
	child := WithSyntaxOwner(ctx, childText)
	window, err := ParseSyntax(child, childText, StatementEntry, false, AcceptedSource)
	if err != nil {
		t.Fatal(err)
	}
	if whole.Origin().arena != window.Origin().arena {
		t.Fatal("declared window was reparsed")
	}
	// A genuinely new lowering product has one distinct retained acquisition.
	loweredText := "RETURN $n + 1 AS total"
	lowered := WithSyntaxOwner(ctx, loweredText)
	first, err := ParseSyntax(lowered, loweredText, StatementEntry, false, AcceptedSource)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		again := WithSyntaxOwner(ctx, loweredText)
		next, err := ParseSyntax(again, loweredText, StatementEntry, true, AcceptedSource)
		if err != nil {
			t.Fatal(err)
		}
		if first.Origin() != next.Origin() {
			t.Fatal("identical program acquired another parse")
		}
		if again.Value(syntaxOwnerKey{}).(*syntaxOwner).program != lowered.Value(syntaxOwnerKey{}).(*syntaxOwner).program {
			t.Fatal("duplicate registry entry")
		}
	}
	if len(root.execution.programs) != 3 {
		t.Fatalf("programs: %d", len(root.execution.programs))
	}
	// The original presentation consumer still sees the original CST.
	originalAgain := WithSyntaxOwner(lowered, original)
	retained, err := ParseSyntax(originalAgain, original, StatementEntry, false, AcceptedSource)
	if err != nil || retained.Origin() != whole.Origin() {
		t.Fatalf("lost original product: %v", err)
	}
	fresh := WithNewSyntaxExecution(ctx, original).Value(syntaxOwnerKey{}).(*syntaxOwner)
	if fresh.execution == root.execution {
		t.Fatal("public executions share a registry")
	}
}

func TestSyntaxOwnerRegistryRetainsFailedAcquisition(t *testing.T) {
	const text = "RETURN ("
	ctx := WithNewSyntaxExecution(context.Background(), text)
	first, firstErr := ParseSyntax(ctx, text, StatementEntry, false, AcceptedSource)
	if firstErr == nil {
		t.Fatal("invalid source accepted")
	}
	again := WithSyntaxOwner(ctx, text)
	second, secondErr := ParseSyntax(again, text, StatementEntry, true, AcceptedSource)
	if secondErr == nil || first.Origin() != second.Origin() {
		t.Fatal("failed acquisition was not retained")
	}
	if ctx.Value(syntaxOwnerKey{}).(*syntaxOwner).program != again.Value(syntaxOwnerKey{}).(*syntaxOwner).program {
		t.Fatal("failed program redeclared")
	}
}

func TestSyntaxOwnerLoweringKeepsInsertedAcceptedTokens(t *testing.T) {
	original := "UNWIND [3,1,2] AS x ORDER BY x LIMIT 2 RETURN x"
	ctx, source := cyphertext.WithSource(context.Background(), original)
	ctx = WithNewSyntaxExecution(ctx, source.Text)
	at := strings.Index(original, "ORDER")
	accepted := original[:at] + "WITH * " + original[at:]
	rewritten := source.Rewrite(accepted, []cyphertext.SourceEdit{{Original: cyphertext.Span{Start: at, End: at}, Accepted: cyphertext.Span{Start: at, End: at + 7}}})
	ctx = WithSyntaxProgram(ctx, rewritten)
	ctx = cyphertext.WithSourceView(ctx, rewritten)
	got, err := ParseSyntax(ctx, accepted, StatementEntry, false, AcceptedSource)
	if err != nil {
		t.Fatal(err)
	}
	want, err := parseSyntaxStandalone(context.Background(), accepted, StatementEntry, false, AcceptedSource)
	if err != nil {
		t.Fatal(err)
	}
	var clauses []string
	got.Walk(func(node SyntaxNode) func() {
		if node.Rule() == CypherParserRULE_withSt {
			clauses = append(clauses, node.Text())
			if node.Code() == "" {
				t.Fatal("inserted clause collapsed")
			}
		}
		return nil
	})
	if len(clauses) != 1 || !strings.HasPrefix(clauses[0], "WITH * ") {
		t.Fatalf("inserted clauses %q", clauses)
	}
	if !reflect.DeepEqual(compatibilityFacts(got), compatibilityFacts(want)) {
		t.Fatalf("lowering changed accepted facts: %v vs %v", compatibilityFacts(got), compatibilityFacts(want))
	}
}

func TestCompatibilityProjectionStarKeepsTerminalRole(t *testing.T) {
	text := "WITH 1 AS value RETURN *, 2 AS extra"
	ctx, source := cyphertext.WithSource(context.Background(), text)
	ctx = WithNewSyntaxExecution(ctx, source.Text)
	derived, err := ProjectionClauseSyntax(ctx, "*", false)
	if err != nil {
		t.Fatal(err)
	}
	direct, err := ProjectionClauseSyntax(context.Background(), "*", false)
	if err != nil {
		t.Fatal(err)
	}
	got := derived.ChildRule(CypherParserRULE_projectionBody).ChildRule(CypherParserRULE_projectionItems)
	want := direct.ChildRule(CypherParserRULE_projectionBody).ChildRule(CypherParserRULE_projectionItems)
	_, gotStar := got.DirectToken(CypherLexerMULT)
	_, wantStar := want.DirectToken(CypherLexerMULT)
	if !gotStar || !wantStar || derived.Code() != direct.Code() || got.Text() != want.Text() {
		t.Fatalf("star changed: %q vs %q", derived.Code(), direct.Code())
	}
}

func TestSyntaxOwnerDeclaredTextOnlyConsumerUsesRegisteredProgram(t *testing.T) {
	const original = "RETURN 1 AS x"
	const generated = "WITH 1 AS x RETURN x"
	ctx, source := cyphertext.WithSource(context.Background(), original)
	ctx = WithNewSyntaxExecution(ctx, source.Text)
	first, err := ParseSyntax(ctx, original, StatementEntry, false, AcceptedSource)
	if err != nil {
		t.Fatal(err)
	}
	// A legacy producer returns only text, while declaring its distinct program.
	_ = WithSyntaxOwner(ctx, generated)
	lowered, err := ParseSyntax(ctx, generated, StatementEntry, false, AcceptedSource)
	if err != nil {
		t.Fatal(err)
	}
	if lowered.Source() != generated {
		t.Fatalf("program source %q", lowered.Source())
	}
	generatedCtx := WithSyntaxOwner(ctx, generated)
	retained, err := ParseSyntax(generatedCtx, original, StatementEntry, false, AcceptedSource)
	if err != nil || retained.Origin() != first.Origin() {
		t.Fatalf("original presentation acquisition lost: %v", err)
	}
	if _, err := ParseSyntax(ctx, "RETURN 2 AS x", StatementEntry, false, AcceptedSource); err == nil {
		t.Fatal("undeclared text became a fallback parse")
	}
}
