package fabric

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// Author: #1663 implementation lane. Configured policy planning and all Cypher
// planning carry the final original analysis. The standalone native-policy API
// preserves its release framing contract, as allocated by Nornic2 on 29 Oct.
type planningSource struct {
	view, anchor     *cyphertext.Source
	analysis         *cypherantlr.BraceAnalysis
	ctx              context.Context
	keywordIsName    ClauseKeywordSourceNameReader
	standalonePolicy bool
}

func (p *FabricPlanner) planningInput(ctx context.Context, source *cyphertext.Source) (planningSource, error) {
	view := planningSource{view: source, anchor: source, ctx: ctx, keywordIsName: p.keywordIsName}
	view.standalonePolicy = p.policyBoundary == nil && cypherantlr.IsKnowledgePolicySource(source)
	if !view.standalonePolicy {
		view.analysis = cypherantlr.OriginalBraceAnalysis(source, false, p.policyBoundary)
		if err := view.analysis.Admission(); err != nil {
			return planningSource{}, err
		}
	}
	return view.trim(), nil
}

// fragmentPlanningSource reads a planned fragment through the Source it
// retains. A fragment built without one is its query text as a new input.
func fragmentPlanningSource(fragment *FragmentExec) planningSource {
	view, anchor := fragment.Source, fragment.sourceAnchor
	if view == nil || view.Text != fragment.Query {
		view, anchor = cyphertext.NormalizeSource(fragment.Query), nil
	}
	if anchor == nil {
		anchor = view
	}
	source := planningSource{view: view, anchor: anchor, ctx: context.Background()}
	source.standalonePolicy = cypherantlr.IsKnowledgePolicySource(view)
	if !source.standalonePolicy {
		source.analysis = cypherantlr.OriginalBraceAnalysis(view, false, nil)
	}
	return source
}

func (source planningSource) text() string { return source.view.Text }
func (source planningSource) window(start, end int) planningSource {
	source.view = source.view.Window(start, end)
	return source
}
func (source planningSource) trim() planningSource {
	span := trimPlanningSpan(source.text(), cyphertext.Span{Start: 0, End: len(source.text())})
	return source.window(span.Start, span.End)
}
func trimPlanningSpan(query string, span cyphertext.Span) cyphertext.Span {
	text := query[span.Start:span.End]
	left := strings.TrimLeftFunc(text, cyphertext.IsWhitespace)
	span.Start += len(text) - len(left)
	span.End = span.Start + len(strings.TrimRightFunc(left, cyphertext.IsWhitespace))
	return span
}

func (source planningSource) body(opening int) (bool, int, bool) {
	if record, found, err := source.view.QueryBodyAt(opening); found {
		if err != nil {
			panic(err)
		}
		return true, record.Close, true
	}
	if source.standalonePolicy {
		// Public compatibility boundary: release reads a CALL's following
		// delimiter without native-policy admission. Its existing framing
		// consumes the original lexical pair, without a policy recognizer.
		record, found := source.view.BraceFacts().AtView(source.view, opening)
		if !found {
			return false, 0, false
		}
		spans, err := source.view.ProjectBrace(record)
		if err != nil {
			return true, -1, true
		}
		for _, span := range spans {
			if span.Start == opening {
				return true, span.End - 1, true
			}
		}
		return true, -1, true
	}
	record, found := source.analysis.At(source.view, opening)
	if !found {
		return false, 0, false
	}
	body := record.Role == cypherantlr.SubqueryBrace
	spans, err := source.view.ProjectBrace(record.Lexical)
	if err != nil {
		return body, -1, true
	}
	for _, span := range spans {
		if span.Start == opening {
			return body, span.End - 1, true
		}
	}
	return body, -1, true
}

type planningPiece struct {
	span      cyphertext.Span
	generated string
	source    *cyphertext.Source
}

func copiedPlanningPiece(start, end int) planningPiece {
	return planningPiece{span: cyphertext.Span{Start: start, End: end}}
}
func generatedPlanningPiece(text string) planningPiece { return planningPiece{generated: text} }
func copiedPlanningSource(source planningSource) planningPiece {
	return planningPiece{source: source.view}
}

// compose records the actual source slices emitted by a planning producer.
// Source owns all coordinate interpretation and generated-origin decisions.
func (source planningSource) compose(pieces ...planningPiece) planningSource {
	inputs := make([]cyphertext.SourcePiece, 0, len(pieces))
	for _, piece := range pieces {
		if piece.source != nil {
			inputs = append(inputs, cyphertext.SourcePiece{View: piece.source})
		} else if piece.span.End > piece.span.Start {
			inputs = append(inputs, cyphertext.SourcePiece{View: source.view.Window(piece.span.Start, piece.span.End)})
		} else {
			inputs = append(inputs, cyphertext.SourcePiece{Generated: piece.generated})
		}
	}
	view, err := source.view.Assemble(inputs)
	if err != nil {
		panic(err)
	}
	source.view = view
	return source
}

func (source planningSource) exec(target planTarget, scope string) *FragmentExec {
	fragment := newExec(source.text(), target, scope)
	fragment.Source, fragment.sourceAnchor = source.view, source.anchor
	return fragment
}

func (source planningSource) rowProducing() planningSource {
	text := ensureRowProducingSourcePrefix(source)
	if text == source.text() {
		return source
	}
	return source.compose(copiedPlanningPiece(0, len(source.text())), generatedPlanningPiece(text[len(source.text()):]))
}

type sourceUse struct {
	UseClause
	source    *cyphertext.Source
	arguments []*cyphertext.Source
}

func parseLeadingUseSource(source planningSource, inSubquery bool) (sourceUse, planningSource, bool, error) {
	reference, remaining, ok, err := ReadUseClauseSource(source.ctx, source.view, inSubquery)
	use := sourceUse{UseClause: reference.UseClause, source: source.view, arguments: reference.ArgumentViews}
	if !ok || err != nil {
		return use, source, ok, err
	}
	source.view = remaining
	return use, source.trim(), true, nil
}

func parseLeadingWithUseSource(source planningSource) (sourceUse, planningSource, bool, error) {
	source = source.trim()
	query := source.text()
	if !startsWithFold(query, "WITH") {
		return sourceUse{}, source, false, nil
	}
	withEnd, found := findLeadingWithSourceClauseEnd(source)
	if !found || withEnd <= 0 || withEnd >= len(query) {
		return sourceUse{}, source, false, nil
	}
	withSpan := trimPlanningSpan(query, cyphertext.Span{Start: 0, End: withEnd})
	restSpan := trimPlanningSpan(query, cyphertext.Span{Start: withEnd, End: len(query)})
	selector := source.window(restSpan.Start, restSpan.End)
	reference, remaining, ok, err := ReadUseClauseSource(selector.ctx, selector.view, true)
	use := sourceUse{UseClause: reference.UseClause, source: selector.view, arguments: reference.ArgumentViews}
	if !ok || err != nil {
		return use, source, ok, err
	}
	return use, source.compose(copiedPlanningPiece(withSpan.Start, withSpan.End), generatedPlanningPiece(" "), planningPiece{source: remaining}).trim(), true, nil
}

// BindPlanSource gives a cached structural plan this statement's source
// association. Relations are replayed by Source; cached nodes remain immutable.
func BindPlanSource(fragment Fragment, source *cyphertext.Source) Fragment {
	switch node := fragment.(type) {
	case *FragmentExec:
		copy := *node
		copy.Input = BindPlanSource(node.Input, source)
		if node.Source != nil {
			copy.Source = node.Source.Rebase(node.sourceAnchor, source)
			copy.GraphArguments = make([]*cyphertext.Source, len(node.GraphArguments))
			for index, argument := range node.GraphArguments {
				copy.GraphArguments[index] = argument.Rebase(node.sourceAnchor, source)
			}
			copy.sourceAnchor = source
		}
		return &copy
	case *FragmentApply:
		copy := *node
		copy.Input = BindPlanSource(node.Input, source)
		copy.Inner = BindPlanSource(node.Inner, source)
		return &copy
	case *FragmentUnion:
		copy := *node
		copy.LHS = BindPlanSource(node.LHS, source)
		copy.RHS = BindPlanSource(node.RHS, source)
		return &copy
	default:
		return fragment
	}
}

// Author: #1663 lane. One traversal for Fabric's top-level framing readers.
// Brace extent comes only from the retained record, never emitted-text pairing.
type planningCursor struct {
	source                   planningSource
	offset, parens, brackets int
}

func (source planningSource) topLevelCursor() planningCursor { return planningCursor{source: source} }

func (cursor *planningCursor) next(from int) (int, bool) {
	text := cursor.source.text()
	for cursor.offset < len(text) {
		at := cursor.offset
		if end, opaque := cyphertext.SkipToken(text, at); opaque {
			cursor.offset = end
			continue
		}
		cursor.offset++
		switch text[at] {
		case '{':
			span, complete, err := cursor.source.view.DelimiterSpan(at)
			if err != nil {
				panic(err)
			}
			if !complete {
				cursor.offset = len(text)
				return 0, false
			}
			cursor.offset = span.End
			continue
		case '}':
			continue
		case '(':
			cursor.parens++
			continue
		case ')':
			if cursor.parens > 0 {
				cursor.parens--
			}
			continue
		case '[':
			cursor.brackets++
			continue
		case ']':
			if cursor.brackets > 0 {
				cursor.brackets--
			}
			continue
		}
		if at >= from && cursor.parens == 0 && cursor.brackets == 0 {
			return at, true
		}
	}
	return 0, false
}

func (cursor *planningCursor) keyword(from int, keyword string) int {
	for at, found := cursor.next(from); found; at, found = cursor.next(from) {
		if clauseKeywordAt(cursor.source, at, keyword) {
			return at
		}
	}
	return -1
}

func (source planningSource) nextTopLevel(from int, accept func(int) bool) int {
	cursor := source.topLevelCursor()
	for at, found := cursor.next(from); found; at, found = cursor.next(from) {
		if accept(at) {
			return at
		}
	}
	return len(source.text())
}

func (source planningSource) topLevelKeyword(from int, keyword string) int {
	cursor := source.topLevelCursor()
	return cursor.keyword(from, keyword)
}
