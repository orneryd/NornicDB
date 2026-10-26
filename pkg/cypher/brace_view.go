package cypher

import (
	"context"
	"sort"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// braceView is an exact statement window/rewrite plus its immutable original
// product. It carries no normalization, coordinate or entry grammar.
type braceView struct {
	syntax *braceSyntax
	source *cyphertext.Source
}

func retainedBraceView(ctx context.Context, query string) braceView {
	syntax, _ := ctx.Value(braceSyntaxKey{}).(*braceSyntax)
	source, carried := cyphertext.SourceView(ctx)
	if !carried {
		panic("cypher: retained brace consumer lost its source view")
	}
	if source.Text != query && !(source == source.OriginalView() && source.Original == query) {
		panic("cypher: retained brace consumer missing source projection")
	}
	if syntax == nil {
		return sourceStatementView(source)
	}
	if source.OriginalView() != syntax.source {
		panic("cypher: retained brace consumer missing source projection")
	}
	return braceView{syntax: syntax, source: source}
}

func withBraceView(ctx context.Context, view braceView) context.Context {
	ctx = cyphertext.WithSourceView(ctx, view.source)
	return context.WithValue(ctx, braceSyntaxKey{}, view.syntax)
}

// canonicalBraceView consumes the existing canonicalizer's rewrite product.
// Classification/cache consumers retain the same original facts as execution.
func canonicalBraceView(ctx context.Context, query string) (context.Context, braceView) {
	view := statementBraceView(ctx, query)
	ctx = withBraceView(ctx, view)
	canonical, rewrite := canonicalizeQueryText(view)
	ctx = withSourceRewrite(ctx, rewrite)
	return ctx, retainedBraceView(ctx, canonical)
}

// withBraceInputSource is the genuine query-text input boundary: a procedure
// supplies another statement as a decoded value, not a slice of this statement.
// Its own original source is retained, and grammar facts are built on demand.
func withBraceInputSource(ctx context.Context, text string) context.Context {
	ctx = cyphertext.WithSourceView(ctx, cyphertext.NormalizeSource(text))
	return context.WithValue(ctx, braceSyntaxKey{}, (*braceSyntax)(nil))
}

// inputBraceView admits text as a new original statement at an exported
// string boundary, or for an internal statement the engine itself issues.
// Consumers of a statement's own text take its producer's retained view.
func inputBraceView(ctx context.Context, text string, stage cyphertext.SourceStage) braceView {
	return sourceStatementView(cyphertext.NewSource(ctx, text, stage))
}

// statementBraceView is cyphertext.SourceFor's statement product with its one
// brace product: the carried statement when ctx carries this text, otherwise
// the text as a new statement (a stored body, a statement the engine issues).
func statementBraceView(ctx context.Context, text string) braceView {
	if source, ok := cyphertext.RetainedSource(ctx, text, cyphertext.RawSource); ok {
		if syntax, _ := ctx.Value(braceSyntaxKey{}).(*braceSyntax); syntax != nil && syntax.source == source.OriginalView() {
			return braceView{syntax: syntax, source: source}
		}
		return sourceStatementView(source)
	}
	return sourceStatementView(cyphertext.SourceFor(ctx, text))
}

// expressionInputView admits text as a new standalone expression: a decoded
// argument a procedure evaluates, or a grammar node read on its own.
func expressionInputView(text string, stage cyphertext.SourceStage) braceView {
	source := cyphertext.NewSource(context.Background(), text, stage)
	return braceView{source: source, syntax: readOriginalBraceSyntax(source, true)}
}

// sourceStatementView pairs an original statement Source with its one product.
func sourceStatementView(source *cyphertext.Source) braceView {
	return braceView{source: source, syntax: readBraceSyntax(source)}
}

func (view braceView) text() string {
	if view.source == nil {
		return ""
	} // absent optional syntax, never a new input
	return view.source.Text
}
func (view braceView) window(start, end int) braceView {
	return braceView{syntax: view.syntax, source: view.source.Window(start, end)}
}
func (view braceView) at(opening int) (braceFact, bool) {
	if record, found, err := view.source.QueryBodyAt(opening); found {
		if err != nil {
			panic(err)
		}
		return braceFact{record: record, rootSource: view.source, source: view.source, span: cyphertext.Span{Start: record.Open, End: record.Close + 1}, rootSpan: cyphertext.Span{Start: record.Open, End: record.Close + 1}, kind: subqueryBrace, generated: true}, true
	}
	return view.syntax.at(view.source, opening)
}

// entryAt returns the retained entry grammar in this occurrence's accepted
// coordinates. Expression consumers supply a key/selector token span from
// their syntax product. They do not parse a new synthetic map around a body.
func (view braceView) entryAt(name cyphertext.Span) (cyphertext.BraceEntry, bool) {
	entry, found, err := view.source.EntryAt(name)
	if err != nil {
		panic(err)
	}
	return entry, found
}
func (view braceView) close(opening int) (int, bool) {
	if opening < 0 || opening >= len(view.text()) || view.text()[opening] != '{' {
		return 0, false
	}
	span, found, err := view.source.DelimiterSpan(opening)
	if err != nil {
		panic(err)
	}
	return span.End - 1, found
}

// entries projects retained boundaries into this occurrence of a brace. A
// copied map can occur more than once, so each projected entry is constrained
// to the requested opening/closing interval before the consumer sees it.
func (view braceView) entries(opening, closing int, visit func(cyphertext.BraceEntry)) bool {
	fact, found := view.at(opening)
	if !found || view.syntax.err != nil {
		return false
	}
	var entries []cyphertext.BraceEntry
	for _, entry := range fact.entries {
		projected, err := view.source.ProjectEntry(entry.accepted)
		if err != nil {
			panic(err)
		}
		for _, candidate := range projected {
			if candidate.Span.Start >= opening+1 && candidate.Span.End <= closing {
				entries = append(entries, candidate)
			}
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Span.Start < entries[j].Span.Start })
	for _, entry := range entries {
		visit(entry)
	}
	return true
}

// Embedded-expression and string-token extents come from the same original
// lexical record as braces. A rewrite producer takes the exact occurrence.
func (view braceView) interpolationEnd(opening int) (int, bool) {
	lexical := view.syntax.source.BraceFacts()
	for _, scope := range lexical.Scopes() {
		if scope.Span.Start == 0 {
			continue
		}
		for _, span := range view.source.ProjectFrom(lexical.Source(), cyphertext.Span{Start: scope.Span.Start - 1, End: scope.Span.End + 1}) {
			if span.Start == opening {
				return span.End - 1, true
			}
		}
	}
	return 0, false
}

func (view braceView) stringEnd(opening int) (int, bool) {
	lexical := view.syntax.source.BraceFacts()
	for _, scope := range lexical.Scopes() {
		for _, token := range scope.Tokens() {
			if token.Kind != cyphertext.String || !lexical.StringClosed(token.Start) {
				continue
			}
			for _, span := range view.source.ProjectFrom(lexical.Source(), cyphertext.Span{Start: token.Start, End: token.End}) {
				if span.Start == opening {
					return span.End, true
				}
			}
		}
	}
	return 0, false
}
