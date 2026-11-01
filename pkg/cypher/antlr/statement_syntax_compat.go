// Author: R17 implementation lane, 1 November 2026.
package antlr

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/localization"
	"strings"
)

func (data *statementSyntaxData) node(id int) syntaxNodeData {
	if id < len(data.nodes) {
		return data.nodes[id]
	}
	return data.containers[id-len(data.nodes)]
}
func (data *statementSyntaxData) sourceText() string {
	return data.view.Original[data.viewStart:data.viewEnd]
}

// container describes only a grammar container over existing children. It
// never copies a subtree or creates a token, expression, name or clause.
func (data *statementSyntaxData) container(rule int, children []int) int {
	first, last := data.node(children[0]), data.node(children[len(children)-1])
	id := len(data.nodes) + len(data.containers)
	data.containers = append(data.containers, syntaxNodeData{rule: rule, parent: -1, start: first.start, end: last.end, firstToken: first.firstToken, afterToken: last.afterToken, children: children})
	for _, child := range children {
		data.parents[child] = id
	}
	return id
}

// CompatibilityView projects an existing occurrence to the same source-local
// interface as a standalone grammar entry. All nodes and tokens remain owned
// by the one immutable parser arena. Equal text at repeated occurrences is
// allowed: each standalone de70 adapter observes only its local text, not an
// enclosing occurrence's binding or execution decisions.
func (syntax StatementSyntax) CompatibilityView(start, end int, entry SyntaxEntry) (StatementSyntax, bool) {
	if !syntax.Valid() || start < 0 || end < start || end > len(syntax.Source()) {
		return StatementSyntax{}, false
	}
	first, after := -1, -1
	for i := syntax.data.firstToken; i < syntax.data.afterToken; i++ {
		t := syntax.data.tokens[i]
		a, b := (SyntaxToken{syntaxView[syntaxToken]{syntax.data, i}}).Span()
		if b <= start || a >= end || t.kind == -1 {
			continue
		}
		if t.kind == CypherLexerWS {
			continue
		}
		if a < start || b > end {
			return StatementSyntax{}, false
		}
		if t.channel == 0 {
			if first < 0 {
				first = i
			}
			after = i + 1
		}
	}
	if first < 0 {
		if start == end && (entry == FragmentEntry || entry == ClauseEntry) {
			for id, n := range syntax.data.nodes {
				if n.rule != CypherParserRULE_subqueryBody || n.start != n.end {
					continue
				}
				a, b := (SyntaxNode{syntaxView[syntaxNode]{syntax.data, id}}).Span()
				if a != start || b != end {
					continue
				}
				data := *syntax.data
				data.root = id
				data.viewStart += start
				data.viewEnd = data.viewStart
				data.firstToken = max(0, min(n.firstToken, len(data.tokens)))
				data.afterToken = data.firstToken
				data.effects = (StatementSyntax{&data}).collectEffects()
				return StatementSyntax{&data}, true
			}
		}
		return StatementSyntax{}, false
	}
	data := *syntax.data
	data.containers = append([]syntaxNodeData(nil), syntax.data.containers...)
	data.parents = make(map[int]int, len(syntax.data.parents)+5)
	for k, v := range syntax.data.parents {
		data.parents[k] = v
	}
	data.viewStart = syntax.data.viewStart + start
	data.viewEnd = syntax.data.viewStart + end
	data.firstToken, data.afterToken = first, after
	selected := -1
	for id := 0; id < len(data.nodes)+len(data.containers); id++ {
		n := data.node(id)
		completeScript := n.rule == CypherParserRULE_script && n.afterToken == after+1 && after < len(data.tokens) && data.tokens[after].kind == -1
		if n.firstToken != first || (n.afterToken != after && !completeScript && entry != UseEntry && entry != CallEntry) {
			continue
		}
		allowed := false
		switch entry {
		case ExpressionEntry:
			allowed = n.rule == CypherParserRULE_expression
		case PatternEntry:
			allowed = n.rule == CypherParserRULE_pattern || n.rule == CypherParserRULE_patternPart
		case CallEntry:
			allowed = n.rule == CypherParserRULE_callSubquery
		case UseEntry:
			allowed = n.rule == CypherParserRULE_useClause
		case ClauseEntry:
			allowed = n.rule == CypherParserRULE_subqueryBody
		default:
			switch n.rule {
			case CypherParserRULE_script, CypherParserRULE_query, CypherParserRULE_regularQuery, CypherParserRULE_singleQuery, CypherParserRULE_subqueryBody, CypherParserRULE_expression:
				allowed = true
			}
		}
		if allowed {
			selected = id
			break
		}
	}
	// Procedure consumers inspect the invocation before YIELD. It is a
	// complete grammar prefix of the retained call, not another program parse.
	if selected < 0 && (entry == StatementEntry || entry == FragmentEntry || entry == ClauseEntry) {
		for _, call := range data.nodes {
			if (call.rule != CypherParserRULE_queryCallSt && call.rule != CypherParserRULE_standaloneCall) || call.firstToken != first || call.afterToken <= after {
				continue
			}
			var children []int
			invocation, complete := false, true
			for _, child := range call.children {
				item := data.node(child)
				if item.firstToken >= after {
					break
				}
				if item.afterToken > after {
					complete = false
					break
				}
				children = append(children, child)
				invocation = invocation || item.rule == CypherParserRULE_invocationName
			}
			if !complete || !invocation || len(children) == 0 || data.node(children[len(children)-1]).afterToken != after {
				continue
			}
			clause := data.container(call.rule, children)
			// Keep the same public query-body contract as the grammar's CALL entry.
			if call.rule == CypherParserRULE_standaloneCall {
				selected = data.container(CypherParserRULE_query, []int{clause})
			} else {
				selected = data.container(CypherParserRULE_singleQuery, []int{clause})
			}
			if entry == ClauseEntry {
				selected = data.container(CypherParserRULE_subqueryBody, []int{clause})
			}
			break
		}
	}
	// A suffix/prefix containing complete sibling clauses is not necessarily
	// an existing rule extent. Derive its container from those exact siblings.
	if selected < 0 && (entry == StatementEntry || entry == FragmentEntry || entry == ClauseEntry) {
		for id := 0; id < len(syntax.data.nodes); id++ {
			parent := syntax.data.node(id)
			switch parent.rule {
			case CypherParserRULE_singleQuery, CypherParserRULE_subqueryBody, CypherParserRULE_singlePartQ, CypherParserRULE_multiPartQ:
			default:
				continue
			}
			var children []int
			valid := true
			for _, child := range parent.children {
				n := syntax.data.node(child)
				if n.afterToken <= first || n.firstToken >= after {
					continue
				}
				if n.firstToken < first || n.afterToken > after {
					valid = false
					break
				}
				children = append(children, child)
			}
			if valid && len(children) > 0 && data.node(children[0]).firstToken == first && data.node(children[len(children)-1]).afterToken == after {
				rule := CypherParserRULE_singleQuery
				if entry == ClauseEntry {
					rule = CypherParserRULE_subqueryBody
				}
				selected = data.container(rule, children)
				break
			}
		}
	}
	if selected < 0 {
		return StatementSyntax{}, false
	}
	if entry == PatternEntry && data.node(selected).rule == CypherParserRULE_patternPart {
		selected = data.container(CypherParserRULE_pattern, []int{selected})
	}
	rule := data.node(selected).rule
	// Existing consumers of StatementEntry and statement-shaped FragmentEntry
	// expect Script -> Query -> RegularQuery -> SingleQuery. Recreate only
	// missing containers, preserving all clause nodes and the consumer contract.
	if entry == StatementEntry || entry == FragmentEntry {
		switch rule {
		case CypherParserRULE_subqueryBody:
			selected = data.regularFromBody(selected)
			selected = data.container(CypherParserRULE_query, []int{selected})
			selected = data.container(CypherParserRULE_script, []int{selected})
		case CypherParserRULE_singleQuery:
			selected = data.container(CypherParserRULE_regularQuery, []int{selected})
			fallthrough
		case CypherParserRULE_regularQuery:
			selected = data.container(CypherParserRULE_query, []int{selected})
			fallthrough
		case CypherParserRULE_query:
			selected = data.container(CypherParserRULE_script, []int{selected})
		}
	}
	data.root = selected
	if data.useError != nil {
		copy := *data.useError
		copy.Offset, _ = data.view.OriginalOffset(copy.Offset, true, cyphertext.SourceExtent)
		copy.Offset -= data.viewStart
		if copy.Offset >= 0 && copy.Offset <= len(data.sourceText()) {
			data.useError = &copy
		} else {
			data.useError = nil
		}
	}
	product := StatementSyntax{&data}
	data.effects = product.collectEffects()
	return product, true
}

// compatibilityText resolves text only at this legacy adapter boundary. A
// successful match must still be a complete grammar interval, never a partial
// token. Source-local spans make repeated identical windows interchangeable.
func (syntax StatementSyntax) compatibilityText(text string, entry SyntaxEntry) (StatementSyntax, bool) {
	source := syntax.Source()
	for from := 0; from <= len(source)-len(text); {
		at := strings.Index(source[from:], text)
		if at < 0 {
			break
		}
		at += from
		if view, ok := syntax.CompatibilityView(at, at+len(text), entry); ok {
			return view, true
		}
		from = at + 1
	}
	return StatementSyntax{}, false
}

// ProjectSyntax keeps parser-input coordinates while presenting a retained
// source rewrite/window. The source owner supplies every inverse edit.
func (syntax StatementSyntax) ProjectSyntax(source *cyphertext.Source) (StatementSyntax, bool) {
	bridge, extent, ok := source.ViewFrom(syntax.data.source)
	if !ok {
		return StatementSyntax{}, false
	}
	data := *syntax.data
	data.projected = true
	data.view = bridge
	data.viewStart = 0
	data.viewEnd = len(source.Text)
	product := StatementSyntax{&data}
	if extent.Start == 0 && extent.End == len(data.source) {
		data.effects = product.collectEffects()
		return product, true
	}
	// The bridge maps the full parser arena into the selected local window;
	// CompatibilityView selects only the grammar nodes visible in that window.
	return product.CompatibilityView(0, len(source.Text), FragmentEntry)
}

// regularFromBody adapts the grammar's recursive braced UNION form to its
// top-level query container contract. Clauses and UNION tokens are retained.
func (data *statementSyntaxData) regularFromBody(body int) int {
	var branches []int
	for {
		node := data.node(body)
		var clauses, union []int
		next := -1
		for _, child := range node.children {
			n := data.node(child)
			if n.rule == CypherParserRULE_subqueryBody {
				next = child
				continue
			}
			if n.rule < 0 && n.firstToken >= 0 && (data.tokens[n.firstToken].kind == CypherLexerUNION || data.tokens[n.firstToken].kind == CypherLexerALL) {
				union = append(union, child)
				continue
			}
			clauses = append(clauses, child)
		}
		if len(clauses) == 0 {
			return body
		}
		single := data.container(CypherParserRULE_singleQuery, clauses)
		if len(branches) == 0 {
			branches = append(branches, single)
		} else {
			last := branches[len(branches)-1]
			n := data.node(last)
			n.children = append(n.children, single)
			n.end = data.node(single).end
			n.afterToken = data.node(single).afterToken
			data.containers[last-len(data.nodes)] = n
			data.parents[single] = last
		}
		if next < 0 {
			break
		}
		// A UNION descriptor is completed by the following iteration's branch.
		branches = append(branches, data.container(CypherParserRULE_unionSt, union))
		body = next
	}
	return data.container(CypherParserRULE_regularQuery, branches)
}

// compatibilityError preserves an adapter's local diagnostic frame. Errors in
// an omitted prefix/sibling do not turn an independently valid window into a
// rejection; errors inside a retained window cannot become successful facts.
func (syntax StatementSyntax) compatibilityError() error {
	var messages []localization.Message
	first, after := syntax.Root().TokenRange()
	if first < 0 || after <= first || first >= len(syntax.data.tokens) {
		return nil
	}
	after = min(after, len(syntax.data.tokens))
	inputStart, inputEnd := syntax.data.tokens[first].start, syntax.data.tokens[after-1].end
	for i, message := range syntax.data.diagnostics {
		if i >= len(syntax.data.diagnosticOffsets) {
			messages = append(messages, message)
			continue
		}
		offset := syntax.data.diagnosticOffsets[i]
		if offset >= 0 && (offset < inputStart || offset > inputEnd) {
			continue
		}
		if offset >= 0 {
			mapped, _ := syntax.data.view.OriginalOffset(offset, true, cyphertext.SourceExtent)
			mapped -= syntax.data.viewStart
			localSource := cyphertext.Source{Original: syntax.Source(), Text: syntax.Source()}
			line, column, _ := localSource.InputPosition(max(0, min(mapped, len(localSource.Text))))
			message = localization.CypherANTLRUnexpectedInput(line, column)
		}
		messages = append(messages, message)
	}
	if len(messages) == 0 {
		return nil
	}
	message := localization.CypherANTLRParseSyntaxError(strings.Join(diagnosticFallbacks(messages), "; "), messages...)
	return localization.NewLocalizedError(string(message.ID), message, nil)
}

// present projects this arena into a retained presentation association while
// preserving the current window's grammar owner and node identities.
func (syntax StatementSyntax) present(presentation *cyphertext.Source) (StatementSyntax, bool) {
	bridge, _, ok := presentation.ViewFrom(syntax.data.view.Original)
	if !ok {
		return StatementSyntax{}, false
	}
	frame, ok := syntax.data.view.Reframe(bridge)
	if !ok {
		return StatementSyntax{}, false
	}
	data := *syntax.data
	data.view = frame
	data.viewStart = 0
	data.viewEnd = len(presentation.Text)
	data.projected = true
	return StatementSyntax{&data}, true
}

// compatibilityCode serves comment-stripped inputs from the same retained
// grammar occurrence. CodeSource records the existing edit decisions, so this
// adapter neither invents a source association nor retries parsing a fragment.
func (syntax StatementSyntax) compatibilityCode(text string, entry SyntaxEntry) (StatementSyntax, bool) {
	for id, item := range syntax.data.nodes {
		eligible := false
		switch entry {
		case ExpressionEntry:
			eligible = item.rule == CypherParserRULE_expression
		case PatternEntry:
			eligible = item.rule == CypherParserRULE_pattern || item.rule == CypherParserRULE_patternPart
		case UseEntry:
			eligible = item.rule == CypherParserRULE_useClause
		case CallEntry:
			eligible = item.rule == CypherParserRULE_callSubquery
		default:
			switch item.rule {
			case CypherParserRULE_script, CypherParserRULE_query, CypherParserRULE_regularQuery, CypherParserRULE_singleQuery, CypherParserRULE_subqueryBody, CypherParserRULE_singlePartQ, CypherParserRULE_multiPartQ, CypherParserRULE_expression:
				eligible = true
			}
		}
		if !eligible {
			continue
		}
		node := SyntaxNode{syntaxView[syntaxNode]{syntax.data, id}}
		a, b := node.Span()
		if a < 0 || b < a || b > len(syntax.Source()) || node.Code() != text {
			continue
		}
		presented, ok := syntax.present(node.CodeSource())
		if !ok {
			continue
		}
		if view, ok := presented.CompatibilityView(0, len(text), entry); ok {
			return view, true
		}
	}
	return StatementSyntax{}, false
}
