package antlr

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
)

// Author: #1663 lane. The shared framing grammar requests contextual keyword
// boundaries; the Cypher layer supplies its existing keywordIndexFrom owner.
// This seam defines no keyword, name, operator or whitespace recognition.
type PolicyBoundaryKind uint8

const (
	PolicyWordBoundary PolicyBoundaryKind = iota
	PolicyWhitespaceBoundary
)

type PolicyBoundaryReader func(source *cyphertext.Source, keyword string, from int, boundary PolicyBoundaryKind) int

func policyAcceptedPresentation(policy knowledgepolicy.PromotionPolicyDef) knowledgepolicy.PromotionPolicyDef {
	policy.WhenClauses = append([]knowledgepolicy.PromotionPolicyWhenClause(nil), policy.WhenClauses...)
	for index := range policy.WhenClauses {
		policy.WhenClauses[index].Predicate = policyStoredText(policy.WhenClauses[index].Predicate, false)
	}
	if policy.OnAccess != nil {
		access := *policy.OnAccess
		access.Mutations = append([]knowledgepolicy.OnAccessMutation(nil), access.Mutations...)
		for index := range access.Mutations {
			access.Mutations[index].Expression = policyStoredText(access.Mutations[index].Expression, false)
		}
		policy.OnAccess = &access
	}
	return policy
}

// Author: #1663 implementation lane. The existing declaration grammar emits
// original expression windows. These are framing facts, not a second lexer or
// brace classifier. The statement's retained analysis owns their admission.
type knowledgePolicySyntax struct {
	command     any
	handled     bool
	err         error
	expressions []*cyphertext.Source
}

type policyInput struct {
	source   *cyphertext.Source
	product  *knowledgePolicySyntax
	boundary PolicyBoundaryReader
}

func (input policyInput) text() string { return input.source.Text }
func (input policyInput) storedText(start, end int) (string, error) {
	return (policyDDLSource{Source: input.source, original: true}).storedText(start, end)
}
func (input policyInput) window(start, end int) policyInput {
	input.source = input.source.Window(start, end)
	return input
}
func (input policyInput) expression(start, end int) {
	if end <= start {
		return
	}
	input.product.expressions = append(input.product.expressions, input.source.Window(start, end))
}

func (input policyInput) opaqueEnds() map[int]int {
	facts := input.source.BraceFacts()
	ends := make(map[int]int)
	for _, original := range facts.OpaqueSpans() {
		for _, span := range input.source.ProjectFrom(facts.Source(), original) {
			ends[span.Start] = span.End
		}
	}
	for _, scope := range facts.Scopes() {
		for _, token := range scope.Tokens() {
			if !token.Is(facts.Source().Text, "(") && !token.Is(facts.Source().Text, "[") && !token.Is(facts.Source().Text, "{") {
				continue
			}
			close, complete := facts.Close(token.Start)
			if !complete {
				continue
			}
			for _, span := range input.source.ProjectFrom(facts.Source(), cyphertext.Span{Start: token.Start, End: close + 1}) {
				ends[span.Start] = span.End
			}
		}
	}
	return ends
}

func (input policyInput) block(index int) (policyInput, int, bool) {
	index = kpSkipSpaces(input.text(), index)
	record, found := input.source.BraceFacts().AtView(input.source, index)
	if !found {
		return policyInput{}, index, false
	}
	spans, err := input.source.ProjectBrace(record)
	if err != nil {
		return policyInput{}, index, false
	}
	for _, span := range spans {
		if span.Start == index {
			return input.window(index+1, span.End-1), span.End, true
		}
	}
	return policyInput{}, index, false
}

func parseKnowledgePolicyProduct(source *cyphertext.Source, boundary PolicyBoundaryReader) *knowledgePolicySyntax {
	product := &knowledgePolicySyntax{}
	span := cyphertext.TrimWhitespaceSpan(source.Text, cyphertext.Span{End: len(source.Text)})
	input := policyInput{source: source.Window(span.Start, span.End), product: product, boundary: boundary}
	product.command, product.handled, product.err = parseKnowledgePolicyInput(input)
	return product
}

type originalBraceFrame struct {
	span       cyphertext.Span
	tokens     []cyphertext.BraceToken
	expression bool
}

func originalBraceFrames(source *cyphertext.Source, lexical *cyphertext.BraceFacts, policy *knowledgePolicySyntax, expression bool) []originalBraceFrame {
	var inputs []originalBraceFrame
	shell, hasShell := shellOriginalBraceFrames(source, lexical)
	if hasShell {
		inputs = shell
	} else if policy != nil {
		for _, window := range policy.expressions {
			copies, err := window.CopyRelationsFrom(source)
			if err != nil || len(copies) != 1 {
				panic("cypher: declaration expression is not one original window")
			}
			inputs = append(inputs, originalBraceFrame{span: copies[0].Original, expression: true})
		}
	} else {
		inputs = []originalBraceFrame{{span: cyphertext.Span{End: len(source.Text)}, expression: expression}}
	}
	var frames []originalBraceFrame
	for _, scope := range lexical.Scopes() {
		for _, input := range inputs {
			span := cyphertext.Span{Start: max(input.span.Start, scope.Span.Start), End: min(input.span.End, scope.Span.End)}
			if span.End <= span.Start {
				continue
			}
			var tokens []cyphertext.BraceToken
			for _, token := range scope.Tokens() {
				if token.Start >= span.Start && token.End <= span.End {
					tokens = append(tokens, token)
				}
			}
			frames = append(frames, originalBraceFrame{span: span, tokens: tokens, expression: input.expression || scope.Span.Start != 0})
		}
	}
	return frames
}
