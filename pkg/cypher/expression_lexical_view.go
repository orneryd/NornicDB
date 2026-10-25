package cypher

import (
	"strings"
	"unicode"
)

// expressionLexicalView borrows a bounded span of an immutable table. Child
// readers keep this pointer, so neither fragment text nor cache eviction can
// cause another token pass. text has comments blanked at their original offsets.
type expressionLexicalView struct {
	facts *expressionLexicalFacts
	span  syntaxSpan
}

func expressionView(text string) expressionLexicalView {
	facts := readExpressionLexicalFacts(text)
	return expressionLexicalView{facts, syntaxSpan{0, len(text)}}
}

func borrowedExpressionView(text string, views []expressionLexicalView) expressionLexicalView {
	if len(views) > 0 && views[0].facts != nil {
		return views[0].within(text)
	}
	return expressionView(text)
}

func (v expressionLexicalView) text() string { return v.facts.code[v.span.start:v.span.end] }

func (v expressionLexicalView) slice(start, end int) expressionLexicalView {
	return expressionLexicalView{v.facts, syntaxSpan{v.span.start + start, v.span.start + end}}
}

func (v expressionLexicalView) trim() expressionLexicalView {
	text := v.text()
	left := strings.TrimLeftFunc(text, unicode.IsSpace)
	start := len(text) - len(left)
	return v.slice(start, start+len(strings.TrimSpace(left)))
}

// Separate operator/CASE/binding grammars return slices of their input. Rebind
// those slices within the current operand, never by searching the whole query
// or by acquiring a table for their text. Positional argument readers use slice.
func (v expressionLexicalView) within(text string) expressionLexicalView {
	if text == v.text() || text == v.facts.text[v.span.start:v.span.end] {
		return v
	}
	if text == "" {
		return v.slice(0, 0)
	}
	for _, source := range []string{v.text(), v.facts.text[v.span.start:v.span.end]} {
		for from := 0; from <= len(source)-len(text); {
			at := strings.Index(source[from:], text)
			if at < 0 {
				break
			}
			at += from
			// A grammar slice cannot begin in quoted data, a comment or
			// an identifier merely containing the requested operand text.
			left := len(text) - len(strings.TrimLeftFunc(text, unicode.IsSpace))
			end := v.span.start + at + len(strings.TrimRightFunc(text, unicode.IsSpace))
			last := v.tokenIndex(end) - 1
			if token, ok := v.facts.at[v.span.start+at+left]; ok && v.facts.tokens[token].kind != lexicalComment && last >= token && v.facts.tokens[last].end == end {
				return v.slice(at, at+len(text))
			}
			from = at + 1
		}
	}
	panic("static expression is outside its retained lexical span")
}

func (v expressionLexicalView) tokenIndex(at int) int {
	low, high := 0, len(v.facts.tokens)
	for low < high {
		mid := low + (high-low)/2
		if v.facts.tokens[mid].start < at {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low
}

func (v expressionLexicalView) tokens() []expressionLexicalToken {
	return v.facts.tokens[v.tokenIndex(v.span.start):v.tokenIndex(v.span.end)]
}

func (v expressionLexicalView) group(open int) (int, bool) {
	i, ok := v.facts.at[v.span.start+open]
	if !ok {
		return 0, false
	}
	t := v.facts.tokens[i]
	return t.group, t.kind == lexicalPunctuation && strings.ContainsRune("([{", rune(v.facts.text[t.start]))
}

func (v expressionLexicalView) enclosed(open, close byte) (string, bool) {
	text := v.text()
	if len(text) < 2 || text[0] != open || text[len(text)-1] != close {
		return "", false
	}
	group, ok := v.group(0)
	if !ok || v.facts.groups[group].close != v.span.end-1 {
		return "", false
	}
	return strings.TrimSpace(text[1 : len(text)-1]), true
}

func (v expressionLexicalView) call() (functionCallSpan, string, bool) {
	i, ok := v.facts.at[v.span.start]
	if !ok || v.facts.tokens[i].call < 0 {
		return functionCallSpan{}, "", false
	}
	call := v.facts.calls[v.facts.tokens[i].call]
	if call.start != v.span.start || !call.expressionCall() || call.close("") != v.span.end-1 {
		return functionCallSpan{}, "", false
	}
	return call, v.facts.code[call.open+1 : v.span.end-1], true
}

func (v expressionLexicalView) appendArguments(parts []expressionLexicalView, group int) []expressionLexicalView {
	spans := v.facts.argumentSpans(group)
	for _, span := range spans {
		part := (expressionLexicalView{v.facts, span}).trim()
		if part.span.start == part.span.end && (len(spans) == 1 || span.end == v.facts.groups[group].close) {
			continue
		}
		parts = append(parts, part)
	}
	return parts
}

func (v expressionLexicalView) splitComma(text string) []string {
	v = v.within(text).trim()
	if v.span.start == v.span.end {
		return nil
	}
	// Argument separators at this span's own depth are already in the table.
	tokens := v.tokens()
	depth := tokens[0].depth
	start := 0
	var parts []string
	for _, token := range tokens {
		if token.depth == depth && v.facts.byteAt(v.facts.at[token.start]) == ',' {
			at := token.start - v.span.start
			parts = append(parts, strings.TrimSpace(v.text()[start:at]))
			start = at + 1
		}
	}
	if tail := strings.TrimSpace(v.text()[start:]); tail != "" {
		parts = append(parts, tail)
	}
	return parts
}

func (v expressionLexicalView) postfix() (receiver, inner string, subscript, ok bool) {
	i, found := v.facts.postfixEnd[v.span.end]
	if !found {
		return "", "", false, false
	}
	a := v.facts.postfix[i]
	if a.receiver.start != v.span.start {
		return "", "", false, false
	}
	receiver = strings.TrimSpace(v.facts.code[a.receiver.start:a.receiver.end])
	inner = strings.TrimSpace(v.facts.code[a.key.start:a.key.end])
	return receiver, inner, a.subscript, a.subscript || simpleSemanticIdentifier(inner) != ""
}

func (v expressionLexicalView) projectionGroup() (receiver, items string, ok bool) {
	last, found := v.facts.tokenBefore(v.span.end)
	if !found || last.opened <= v.span.start || v.facts.text[last.opened] != '{' || last.start != v.span.end-1 {
		return "", "", false
	}
	first := v.facts.tokens[v.tokenIndex(v.span.start)]
	if last.depth != first.depth+1 || v.subqueryBrace(last.opened) {
		return "", "", false
	}
	return strings.TrimSpace(v.facts.code[v.span.start:last.opened]), strings.TrimSpace(v.facts.code[last.opened+1 : last.start]), true
}

func (v expressionLexicalView) subqueryBrace(open int) bool {
	t := v.facts.tokens[v.facts.at[open]]
	return t.previous >= 0 && v.facts.tokens[t.previous].scalar
}

func (v expressionLexicalView) projectionReceiver() (string, bool) {
	receiver, _, ok := v.projectionGroup()
	if !ok {
		return "", false
	}
	last, _ := v.facts.tokenBefore(v.span.end)
	before, found := v.facts.tokenBefore(last.opened)
	_, subqueryName := staticSubqueryExpressionTypes[upperASCII(receiver)]
	operand := found && (before.kind == lexicalName || before.kind == lexicalQuoted || before.opened >= 0 || isDigitByte(v.facts.text[before.start]))
	if !operand || before.role == expressionKeyword && !isLiteralKeyword(before.name) && !subqueryName {
		return "", false
	}
	return receiver, true
}

func (v expressionLexicalView) operatorKeywordIndex(text, keyword string) int {
	words := strings.Fields(keyword)
	tokens := v.tokens()
	if len(tokens) == 0 {
		return -1
	}
	for _, t := range tokens {
		at := t.start - v.span.start
		if t.depth != tokens[0].depth || t.kind != lexicalName || t.role != expressionKeyword || !strings.EqualFold(t.name, words[0]) || at+len(t.name) > len(text) || !strings.EqualFold(text[at:at+len(t.name)], t.name) {
			continue
		}
		i, matched := v.facts.at[t.start], true
		for _, word := range words[1:] {
			i = v.facts.tokens[i].next
			if i < 0 || v.facts.tokens[i].start >= v.span.end || v.facts.tokens[i].role != expressionKeyword || !strings.EqualFold(v.facts.tokens[i].name, word) {
				matched = false
				break
			}
		}
		if matched {
			return at
		}
	}
	return -1
}
