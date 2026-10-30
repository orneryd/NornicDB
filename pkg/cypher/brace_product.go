package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// braceSyntax is the immutable original-statement brace product. Construction
// runs before structural rewriting. Consumers retain the product and a Source
// view; a projected view never runs brace classification or entry grammar.
type braceSyntax struct {
	source  *cyphertext.Source
	product *cypherantlr.BraceAnalysis
	err     error
}
type braceFact struct {
	record         cyphertext.BraceRecord
	source         *cyphertext.Source
	rootSource     *cyphertext.Source
	generated      bool
	span, original cyphertext.Span
	rootSpan       cyphertext.Span
	opening        cyphertext.Span
	kind           mapBraceKind
	introducer     cyphertext.Span
	bodyKeyword    string
	entries        []braceEntryFact
}

type braceEntryFact struct {
	source                        *cyphertext.Source
	rootSource                    *cyphertext.Source
	generated                     bool
	accepted                      cyphertext.BraceEntry
	original, name, value         cyphertext.Span
	rootSpan, rootName, rootValue cyphertext.Span
}

type braceSyntaxKey struct{}

// sourceBraceView adapts a producer's actual Source window to the retained
// original record. It never constructs an input Source from fragment text.
func sourceBraceView(source *cyphertext.Source) braceView {
	return braceView{source: source, syntax: readOriginalBraceSyntax(source, true)}
}

// validateAdmittedBraceSyntax consumes the statement's admission verdict.
// Standalone validation is a new original statement; execution with a retained
// product never re-admits a transformed fragment.
func validateAdmittedBraceSyntax(ctx context.Context, query string) error {
	if syntax, ok := ctx.Value(braceSyntaxKey{}).(*braceSyntax); ok && syntax != nil {
		return syntax.err
	}
	return statementBraceView(ctx, query).admission()
}

func admitBraceSyntax(ctx context.Context, source *cyphertext.Source) (context.Context, *braceSyntax, error) {
	if syntax, ok := ctx.Value(braceSyntaxKey{}).(*braceSyntax); ok && syntax != nil {
		if syntax.source != source.OriginalView() {
			panic("cypher: internal admission lost the original source view")
		}
		return ctx, syntax, syntax.err
	}
	syntax := readBraceSyntax(source)
	return context.WithValue(ctx, braceSyntaxKey{}, syntax), syntax, syntax.err
}

func readBraceSyntax(source *cyphertext.Source) *braceSyntax {
	return readOriginalBraceSyntax(source, false)
}

// Independent expression inputs (decoded procedure fields) have an explicit
// grammar frame. They use the same original lexical records and role owner.
func readOriginalBraceSyntax(source *cyphertext.Source, expression bool) *braceSyntax {
	product := cypherantlr.OriginalBraceAnalysis(source, expression, readPolicyKeywordBoundary)
	err := product.Admission()
	if admission, ok := err.(*cypherantlr.BraceAdmissionError); ok {
		err = newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", admission.Message)
	}
	return &braceSyntax{source: product.Source(), product: product, err: err}
}

// Author: #1663 implementation lane. Occurrence adapters copy the canonical
// record's role; they do not retain or decide another admission product.
func (syntax *braceSyntax) fact(record cypherantlr.BraceRecord) braceFact {
	source := syntax.source
	lexical := record.Lexical
	span := cyphertext.Span{Start: lexical.Open, End: lexical.Close + 1}
	fact := braceFact{record: lexical, source: source, rootSource: source, span: span, rootSpan: span, original: source.Origin(span).Original, opening: source.Origin(cyphertext.Span{Start: lexical.Open, End: lexical.Open + 1}).Original, kind: record.Role, introducer: record.Introducer, bodyKeyword: record.BodyKeyword}
	for _, entry := range lexical.Entries() {
		fact.entries = append(fact.entries, braceEntryFact{source: source, rootSource: source, accepted: entry, original: source.Origin(entry.Span).Original, name: source.Origin(entry.Name).Original, value: source.Origin(entry.Value).Original, rootSpan: entry.Span, rootName: entry.Name, rootValue: entry.Value})
	}
	return fact
}
func (syntax *braceSyntax) braceFacts() []braceFact {
	records := syntax.product.Records()
	facts := make([]braceFact, len(records))
	for index, record := range records {
		facts[index] = syntax.fact(record)
	}
	return facts
}
func (syntax *braceSyntax) at(view *cyphertext.Source, opening int) (braceFact, bool) {
	record, found := syntax.product.At(view, opening)
	if !found {
		return braceFact{}, false
	}
	return syntax.fact(record), true
}
func (syntax *braceSyntax) entryAt(view *cyphertext.Source, name cyphertext.Span) (braceEntryFact, bool) {
	entry, found, err := view.EntryAt(name)
	if err != nil {
		panic(err)
	}
	if !found {
		return braceEntryFact{}, false
	}
	return braceEntryFact{source: view, rootSource: syntax.source, accepted: entry, original: view.Origin(entry.Span).Original, name: view.Origin(entry.Name).Original, value: view.Origin(entry.Value).Original, rootSpan: entry.Span, rootName: entry.Name, rootValue: entry.Value}, true
}
