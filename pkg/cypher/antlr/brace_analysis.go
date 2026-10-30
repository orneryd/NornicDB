package antlr

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// Author: #1663 implementation lane. BraceAnalysis is the immutable final
// original-statement role, entry and admission product. Every public Cypher and
// Fabric entrance consumes this product, including standalone planning.
type BraceAnalysis struct {
	source    *cyphertext.Source
	records   []BraceRecord
	byOpening map[cyphertext.Span]int
	err       error
	policy    *knowledgePolicySyntax
	grammar   StatementSyntax
}

type BraceRecord struct {
	Lexical     cyphertext.BraceRecord
	Role        BraceRole
	Introducer  cyphertext.Span
	BodyKeyword string
}

const (
	MapLiteralEntryMessage      = "map literal keys must be valid symbolic names"
	PatternPropertyEntryMessage = "pattern property maps require key-value entries"
)

// BraceAdmissionError retains the shared admission verdict. The Cypher public
// error adapter supplies its existing status/detail representation.
type BraceAdmissionError struct{ Message string }

func (err *BraceAdmissionError) Error() string { return err.Message }

func OriginalBraceAnalysis(source *cyphertext.Source, expression bool, boundary PolicyBoundaryReader) *BraceAnalysis {
	return source.OriginalBraceAnalysis(func(original *cyphertext.Source) any {
		return buildOriginalBraceAnalysis(original, expression, boundary)
	}).(*BraceAnalysis)
}
func (syntax *BraceAnalysis) Source() *cyphertext.Source { return syntax.source }
func (syntax *BraceAnalysis) Admission() error           { return syntax.err }
func (syntax *BraceAnalysis) Records() []BraceRecord {
	return append([]BraceRecord(nil), syntax.records...)
}

// MapKeyAt consumes final contextual role plus the original entry/name record.
// A name-only window need not contain the enclosing separators; this method
// supplies no entry/value extent and never rescans the view. Author: #1663 lane.
func (syntax *BraceAnalysis) MapKeyAt(view *cyphertext.Source, name cyphertext.Span) bool {
	entry, found := view.OriginalEntryAt(name)
	if !found || entry.Kind != cyphertext.KeyedBraceEntry {
		return false
	}
	return syntax.entryHasMapRole(view, entry)
}

// MapColonAt is the same original-record query for a copied/declared colon.
// The operand window need not contain unrelated entry endpoints. Author: #1663 lane.
func (syntax *BraceAnalysis) MapColonAt(view *cyphertext.Source, colon cyphertext.Span) bool {
	entry, found := view.OriginalEntryAtColon(colon)
	return found && syntax.entryHasMapRole(view, entry)
}

func (syntax *BraceAnalysis) entryHasMapRole(view *cyphertext.Source, entry cyphertext.BraceEntry) bool {
	if view.OriginalView() != syntax.source {
		return false
	}
	brace, found := entry.BraceRecord()
	if !found {
		return false
	}
	opening, valid := syntax.source.OriginalSpan(cyphertext.Span{Start: brace.Open, End: brace.Open + 1}, cyphertext.SourceExtent)
	if !valid {
		return false
	}
	index, found := syntax.byOpening[opening]
	if !found {
		return false
	}
	switch syntax.records[index].Role {
	case LiteralBrace, ProjectionBrace, PatternPropertyBrace:
		return true
	}
	return false
}
func (syntax *BraceAnalysis) At(view *cyphertext.Source, opening int) (BraceRecord, bool) {
	if record, found, err := view.QueryBodyAt(opening); found {
		if err != nil {
			panic(err)
		}
		return BraceRecord{Lexical: record, Role: SubqueryBrace}, true
	}
	if opening < 0 || opening >= len(view.Text) {
		return BraceRecord{}, false
	}
	origin := view.Origin(cyphertext.Span{Start: opening, End: opening + 1})
	if origin.Generated {
		return BraceRecord{}, false
	}
	index, found := syntax.byOpening[origin.Original]
	if !found {
		return BraceRecord{}, false
	}
	return syntax.records[index], true
}
func (syntax *BraceAnalysis) PolicyCommand(original bool) (any, bool, error) {
	if syntax.policy == nil {
		return nil, false, nil
	}
	if syntax.policy.err != nil {
		return nil, syntax.policy.handled, syntax.policy.err
	}
	if syntax.err != nil {
		return nil, syntax.policy.handled, syntax.err
	}
	command := syntax.policy.command
	if !original {
		// The retained command is immutable. Only stored-expression spelling
		// changes for TJ's standalone AcceptedSource API presentation.
		switch cmd := command.(type) {
		case *CreatePromotionPolicyCmd:
			copy := *cmd
			copy.Policy = policyAcceptedPresentation(cmd.Policy)
			command = &copy
		case *AlterPromotionPolicyDefinitionCmd:
			copy := *cmd
			copy.Policy = policyAcceptedPresentation(cmd.Policy)
			command = &copy
		}
	}
	return command, syntax.policy.handled, nil
}
func buildOriginalBraceAnalysis(source *cyphertext.Source, expression bool, boundary PolicyBoundaryReader) *BraceAnalysis {
	lexical := source.BraceFacts()
	source = lexical.Source()
	syntax := &BraceAnalysis{source: source, byOpening: make(map[cyphertext.Span]int)}
	query := source.Text
	entry := StatementEntry
	if expression {
		entry = ExpressionEntry
	}
	syntax.grammar, _ = ParseSyntax(context.Background(), query, entry, false, AcceptedSource)
	declarationOpenings := make(map[int]bool)
	optionMaps := make(map[int]bool)
	if syntax.grammar.Valid() {
		syntax.grammar.Walk(func(node SyntaxNode) func() {
			switch node.Rule() {
			case CypherParserRULE_constraintBlock:
				if open, found := node.DirectToken(CypherLexerLBRACE); found {
					at, _ := open.Span()
					declarationOpenings[at] = true
				}
			case CypherParserRULE_mapLit:
				for parent, found := node.Parent(); found; parent, found = parent.Parent() {
					switch parent.Rule() {
					case CypherParserRULE_schemaCommand:
						if open, found := node.DirectToken(CypherLexerLBRACE); found {
							at, _ := open.Span()
							optionMaps[at] = true
						}
					}
				}
			}
			return nil
		})
	}
	statement := source
	if !expression {
		statement = originalStatementView(source)
	}
	// Declaration grammars retain their own admission boundary. Their expression
	// consumers must carry the original expression windows into this product.
	if IsKnowledgePolicyDDLStatement(statement.Text) {
		syntax.policy = parseKnowledgePolicyProduct(statement, boundary)
	}

	if len(lexical.Unmatched()) != 0 {
		syntax.err = &BraceAdmissionError{Message: localization.CypherTransactionsSyntaxUnbalancedCurlyBraces().Fallback}
	}
	for _, scope := range originalBraceFrames(source, lexical, syntax.policy, expression) {
		view := source.Window(scope.span.Start, scope.span.End)
		text := view.Text
		infos := scope.tokens
		hasBrace := false
		for _, info := range infos {
			if info.Is(query, "{") {
				hasBrace = true
				break
			}
		}
		if !hasBrace {
			continue
		}
		scanner := originalBraceRoleCursor{Query: text, source: view, expression: scope.expression}
		for index := range infos {
			infos[index].Start -= scope.span.Start
			infos[index].End -= scope.span.Start
			if infos[index].ParenOpen >= 0 {
				infos[index].ParenOpen -= scope.span.Start
			}
		}
		context := newMapPatternContextRecords(view, infos, syntax.grammar)
		for _, info := range infos {
			global := info.Start + scope.span.Start
			scanner.Earlier, scanner.Prior, scanner.Last = scanner.Prior, scanner.Last, info
			if !info.Is(text, "{") {
				continue
			}
			record, found := lexical.At(global)
			if !found || record.Close < 0 {
				continue
			}
			scanner.entryFacts = record.Entries()
			kind := scanner.kind(&context)
			if declarationOpenings[global] {
				kind = DeclarationBrace
			} else if optionMaps[global] {
				kind = LiteralBrace
			}
			fact := BraceRecord{Lexical: record, Role: kind}
			if kind == SubqueryBrace {
				for _, keyword := range subqueryExpressionKeywords {
					if scanner.Prior.Keyword(text, keyword) {
						fact.Introducer = cyphertext.Span{Start: scanner.Prior.Start + scope.span.Start, End: scanner.Prior.End + scope.span.Start}
						fact.BodyKeyword = keyword
						break
					}
				}
			}
			opening := source.Origin(cyphertext.Span{Start: global, End: global + 1}).Original
			if kind != SubqueryBrace && kind != QuantifierBrace && kind != DeclarationBrace {
				valid := true
				for _, entry := range record.Entries() {
					valid = valid && (entry.Kind == cyphertext.KeyedBraceEntry || kind == ProjectionBrace && entry.Kind != cyphertext.InvalidBraceEntry)
				}
				if !valid && syntax.err == nil {
					message := MapLiteralEntryMessage
					if kind == PatternPropertyBrace && !record.Separator {
						message = PatternPropertyEntryMessage
					}
					syntax.err = &BraceAdmissionError{Message: message}
				}
			}
			syntax.byOpening[opening] = len(syntax.records)
			syntax.records = append(syntax.records, fact)
		}
	}
	if syntax.policy != nil {
		for _, record := range lexical.Braces() {
			if record.Close < record.Open {
				continue
			}
			opening := source.Origin(cyphertext.Span{Start: record.Open, End: record.Open + 1}).Original
			if _, found := syntax.byOpening[opening]; found {
				continue
			}
			fact := BraceRecord{Lexical: record, Role: DeclarationBrace}
			syntax.byOpening[opening] = len(syntax.records)
			syntax.records = append(syntax.records, fact)
		}
	}
	return syntax
}

func originalStatementView(source *cyphertext.Source) *cyphertext.Source {
	if source != source.OriginalView() {
		return source
	}
	// Prefix identity and token extent are facts of the existing lexical owner.
	scopes := source.BraceFacts().Scopes()
	if len(scopes) > 0 {
		tokens := scopes[0].Tokens()
		if len(tokens) > 0 {
			return source.Window(tokens[0].Start, len(source.Text))
		}
	}
	return source
}

// IsKnowledgePolicySource identifies the native input boundary from the
// original lexical statement prefix; it performs no policy admission.
func IsKnowledgePolicySource(source *cyphertext.Source) bool {
	return IsKnowledgePolicyDDLStatement(originalStatementView(source).Text)
}
