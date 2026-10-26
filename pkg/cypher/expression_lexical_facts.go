package cypher

import "strings"

// expressionLexicalFacts is the immutable lexical product. Source is tokenized
// once; all subsequent passes read token indexes, never prefixes of the source.
// The execution argument inventory is a view of this same product.
type expressionLexicalFacts struct {
	functionSyntax
	code        string // comments blanked once, preserving every source offset
	tokens      []expressionLexicalToken
	at          map[int]int
	postfix     []lexicalPostfix
	postfixAt   map[int]int
	postfixEnd  map[int]int
	propertyEnd map[int]int
	trailing    int
}

type expressionTokenRole uint8

const (
	expressionIdentifier expressionTokenRole = iota
	expressionKeyword
	expressionParameter
	expressionProperty
	expressionMapKey
)

type expressionLexicalToken struct {
	kind                       lexicalTokenKind
	start, end                 int
	name                       string
	previous, next             int
	depth, group, opened, call int
	role                       expressionTokenRole
	annotation                 bool
	scalar                     bool
}

// Unlike the optional execution inventory, the shared lexical product also
// retains long expressions. Cursors hold their table across cache eviction.
var expressionLexicalCache = newBoundedCache[string, *expressionLexicalFacts](512, boundedCacheStringShardIndex)

func readExpressionLexicalFacts(text string) *expressionLexicalFacts {
	if facts, ok := expressionLexicalCache.get(text); ok {
		return facts
	}
	facts := buildExpressionLexicalFacts(text)
	expressionLexicalCache.put(text, facts)
	return facts
}

func buildExpressionLexicalFacts(text string) *expressionLexicalFacts {
	f := &expressionLexicalFacts{functionSyntax: functionSyntax{text: text, leading: len(text)}, code: text, at: make(map[int]int), postfixAt: make(map[int]int), postfixEnd: make(map[int]int), propertyEnd: make(map[int]int), trailing: -1}
	f.functionSyntax.facts = f
	f.groups = append(f.groups, syntaxGroup{open: -1, close: len(text)})
	stack := []int{0}
	previous := -1
	var separators []syntaxSeparator
	for index := 0; ; {
		kind, start, end, name := nextLexicalToken(text, index)
		index = end
		t := expressionLexicalToken{kind: kind, start: start, end: end, name: name, previous: previous, next: -1, depth: len(stack) - 1, group: stack[len(stack)-1], opened: -1, call: -1}
		ti := len(f.tokens)
		f.at[start] = ti
		if kind == lexicalComment {
			f.comments = append(f.comments, syntaxSpan{start, end})
		} else {
			if previous >= 0 {
				f.tokens[previous].next = ti
			}
			if kind != lexicalEnd {
				previous = ti
				if f.leading == len(text) {
					f.leading = start
				}
			}
		}
		if kind == lexicalQuoted {
			f.quoted = append(f.quoted, syntaxSpan{start, end})
		}
		if kind == lexicalPunctuation {
			switch text[start] {
			case '(', '[', '{':
				t.group = len(f.groups)
				f.groups = append(f.groups, syntaxGroup{open: start, close: -1})
				stack = append(stack, t.group)
			case ')', ']', '}':
				if len(stack) > 1 {
					g := &f.groups[t.group]
					if matchingExpressionDelimiter(text[g.open], text[start]) {
						g.close = start
						t.opened = g.open
						stack = stack[:len(stack)-1]
					}
				}
			case ',':
				g := &f.groups[t.group]
				separator := len(separators)
				separators = append(separators, syntaxSeparator{start, -1})
				if g.argumentStart == 0 {
					g.argumentStart = separator + 1
				} else {
					separators[g.argumentEnd].next = separator
				}
				g.argumentEnd = separator
				if t.depth == 0 {
					f.rootPunctuation = append(f.rootPunctuation, start)
				}
			default:
				if t.depth == 0 {
					f.rootPunctuation = append(f.rootPunctuation, start)
				}
			}
		}
		f.tokens = append(f.tokens, t)
		if kind == lexicalEnd {
			break
		}
	}
	for i := range f.groups {
		g := &f.groups[i]
		separator := g.argumentStart - 1
		g.argumentStart = len(f.arguments)
		if g.close >= 0 {
			start := g.open + 1
			for ; separator >= 0; separator = separators[separator].next {
				comma := separators[separator].at
				f.arguments = append(f.arguments, syntaxSpan{start, comma})
				start = comma + 1
			}
			f.arguments = append(f.arguments, syntaxSpan{start, g.close})
		}
		g.argumentEnd = len(f.arguments)
	}
	if len(f.comments) > 0 {
		code := []byte(text)
		for _, comment := range f.comments {
			for at := comment.start; at < comment.end; at++ {
				code[at] = ' '
			}
		}
		f.code = string(code)
	}
	f.classify()
	f.receivers()
	for i := range f.calls {
		f.calls[i].syntax = &f.functionSyntax
	}
	return f
}

func matchingExpressionDelimiter(open, close byte) bool {
	return open == '(' && close == ')' || open == '[' && close == ']' || open == '{' && close == '}'
}

func (f *expressionLexicalFacts) byteAt(token int) byte {
	if token < 0 || token >= len(f.tokens) || f.tokens[token].kind == lexicalEnd {
		return 0
	}
	return f.text[f.tokens[token].start]
}

// Roles describe grammar positions. A keyword spelling in a parameter, key,
// property or postfix receiver position is an identifier. Operators are admitted
// at their operand positions; clause introducers need their following shape.
func (f *expressionLexicalFacts) nameRole(i int) expressionTokenRole {
	t := f.tokens[i]
	before, after := f.byteAt(t.previous), f.byteAt(t.next)
	if before == '$' {
		return expressionParameter
	}
	if before == '.' {
		return expressionProperty
	}
	if after == ':' && t.next >= 0 && f.byteAt(f.tokens[t.next].next) != ':' {
		return expressionMapKey
	}
	if isCypherBacktickByte(f.text[t.start]) {
		return expressionIdentifier
	}
	if t.previous >= 0 && f.tokens[t.previous].role == expressionKeyword && strings.EqualFold(f.tokens[t.previous].name, "AS") {
		return expressionIdentifier
	}
	word := upperASCII(t.name)
	if word == "NOT" && f.prefixOperand(i) {
		return expressionKeyword
	}
	// Scalar-subquery keywords introduce a query body, rather than projecting
	// properties. The body's statement position supplies the distinction.
	if after == '{' && (word == "COUNT" || word == "COLLECT" || word == "EXISTS") {
		inside := f.tokens[t.next].next
		if inside >= 0 && (expressionClauseWord(f.tokens[inside].name) || f.byteAt(inside) == '(') {
			return expressionKeyword
		}
	}
	// An infix word is syntax only after a completed operand. The same word
	// after an opener, separator, clause or operator is an identifier operand.
	if t.previous >= 0 {
		p := f.tokens[t.previous]
		operand := p.role != expressionKeyword && (p.kind == lexicalName || p.kind == lexicalQuoted || before == ')' || before == ']' || before == '}' || isDigitByte(before))
		if operand {
			switch word {
			case "IN", "AND", "OR", "XOR", "IS", "AS", "CONTAINS", "STARTS", "ENDS", "THEN", "ELSE", "WHEN":
				return expressionKeyword
			}
		}
	}
	if after == '.' {
		return expressionIdentifier
	}
	if after == '{' {
		inside := f.tokens[t.next].next
		if f.byteAt(inside) == '.' || f.byteAt(inside) == '*' {
			return expressionIdentifier
		}
		if (word == "RETURN" || word == "WITH") && t.depth == 0 {
			return expressionKeyword
		}
		return expressionIdentifier
	}
	if after == '[' {
		if (word == "RETURN" || word == "WITH") && t.depth == 0 && t.next >= 0 && f.tokens[t.next].start > t.end {
			return expressionKeyword
		}
		return expressionIdentifier
	}
	if expressionClauseWord(word) {
		if after != 0 && (t.depth == 0 || f.text[f.groups[t.group].open] == '{' || before == '|') {
			return expressionKeyword
		}
	}
	if word == "FILTER" && after == '(' {
		return expressionKeyword
	}
	if t.previous >= 0 && f.tokens[t.previous].role == expressionKeyword {
		prior := upperASCII(f.tokens[t.previous].name)
		if word == "BY" && prior == "ORDER" || word == "WITH" && (prior == "STARTS" || prior == "ENDS") || word == "NOT" && prior == "IS" || word == "NULL" && (prior == "IS" || prior == "NOT") {
			return expressionKeyword
		}
	}
	if after != '[' && after != 0 {
		switch word {
		case "DISTINCT", "CASE", "WHEN", "THEN", "ELSE", "TYPED":
			return expressionKeyword
		}
	}
	return expressionIdentifier
}

// Prefix operators require an operand after them and an operand position
// before them. NOT alone, NOT[0] and NOT.property remain variable reads.
func (f *expressionLexicalFacts) prefixOperand(i int) bool {
	t := f.tokens[i]
	after := f.byteAt(t.next)
	if after == 0 || after == ',' || after == ')' || after == ']' || after == '}' || after == '.' {
		return false
	}
	if (after == '[' || after == '{') && f.tokens[t.next].start == t.end {
		return false
	}
	if next := f.tokens[t.next]; next.kind == lexicalName && !isCypherBacktickByte(f.text[next.start]) {
		switch upperASCII(next.name) {
		case "AS", "IN", "AND", "OR", "XOR", "IS", "WHEN", "THEN", "ELSE", "END":
			return false
		}
	}
	if t.previous < 0 {
		return true
	}
	p := f.tokens[t.previous]
	before := f.byteAt(t.previous)
	return p.role == expressionKeyword || before == '(' || before == '[' || before == '{' || before == ',' || before == ':' || before == '|' || strings.ContainsRune("=<>!+-*/%^", rune(before))
}

func expressionClauseWord(word string) bool {
	switch upperASCII(word) {
	case "RETURN", "WITH", "MATCH", "CREATE", "MERGE", "WHERE", "UNWIND", "SET", "DELETE", "REMOVE", "CALL", "USE", "YIELD", "ORDER", "LIMIT", "SKIP", "UNION", "OPTIONAL", "FOREACH":
		return true
	}
	return false
}

// qualifiedName reads disjoint identifier chains. Call classification invokes
// it only at the first component, so namespace length adds linear total work.
func (f *expressionLexicalFacts) qualifiedName(i int) (string, int) {
	dot := f.tokens[i].next
	if f.byteAt(dot) != '.' || f.tokens[dot].next < 0 || f.tokens[f.tokens[dot].next].kind != lexicalName {
		return f.tokens[i].name, i
	}
	end := i
	var parts []string
	for {
		parts = append(parts, f.tokens[end].name)
		dot := f.tokens[end].next
		if f.byteAt(dot) != '.' {
			break
		}
		next := f.tokens[dot].next
		if next < 0 || f.tokens[next].kind != lexicalName {
			break
		}
		end = next
	}
	if len(parts) == 1 {
		return parts[0], end
	}
	return strings.Join(parts, "."), end
}

func (f *expressionLexicalFacts) classify() {
	suppressed := -1
	var scalarBodies []syntaxSpan
	for i := range f.tokens {
		t := &f.tokens[i]
		if t.kind == lexicalEnd || t.kind == lexicalComment {
			continue
		}
		if t.previous >= 0 {
			p := f.tokens[t.previous]
			t.annotation = f.byteAt(t.previous) == ':' && p.previous >= 0 && f.byteAt(p.previous) == ':' || (p.role == expressionKeyword && strings.EqualFold(p.name, "TYPED")) || p.annotation && (strings.EqualFold(p.name, "LOCAL") || strings.EqualFold(p.name, "ZONED"))
		}
		if t.kind != lexicalName {
			if t.kind == lexicalPunctuation && (f.text[t.start] == '+' || f.text[t.start] == '-') && f.prefixOperand(i) {
				t.role = expressionKeyword
			}
			continue
		}
		t.role = f.nameRole(i)
		if t.role == expressionKeyword && f.byteAt(t.next) == '{' && (strings.EqualFold(t.name, "COUNT") || strings.EqualFold(t.name, "COLLECT") || strings.EqualFold(t.name, "EXISTS")) {
			t.scalar = true
			g := f.groups[f.tokens[t.next].group]
			if g.close >= 0 {
				scalarBodies = append(scalarBodies, syntaxSpan{t.start, g.close + 1})
			}
		}
		if i <= suppressed {
			continue
		}
		if t.role == expressionKeyword && (strings.EqualFold(t.name, "CALL") || strings.EqualFold(t.name, "USE")) && t.next >= 0 && f.tokens[t.next].kind == lexicalName {
			start := t.next
			name, last := f.qualifiedName(start)
			suppressed = last
			if strings.EqualFold(t.name, "USE") {
				continue
			}
			open := -1
			next := f.tokens[last].end
			if f.tokens[last].next >= 0 {
				next = f.tokens[f.tokens[last].next].start
				if f.byteAt(f.tokens[last].next) == '(' {
					open = next
					next++
				}
			}
			call := functionCallSpan{name: name, normalized: lowerASCII(name), start: f.tokens[start].start, nameEnd: f.tokens[last].end, open: open, next: next, procedure: true, group: -1}
			f.addCall(start, call)
			continue
		}
		if t.role == expressionParameter || t.role == expressionMapKey || t.role == expressionProperty {
			// A slice's second dot introduces a fresh expression.
			if t.role != expressionProperty || t.previous < 0 || f.tokens[t.previous].previous < 0 || f.byteAt(f.tokens[t.previous].previous) != '.' {
				continue
			}
		}
		if t.previous >= 0 && f.tokens[t.previous].end == t.start && (isIdentByte(f.text[t.start-1]) || f.text[t.start-1] == '$') {
			continue
		}
		if t.previous >= 0 && f.byteAt(t.previous) == ':' && (f.groups[t.group].open < 0 || f.text[f.groups[t.group].open] != '{' && f.text[f.groups[t.group].open] != '[') {
			continue
		}
		name, last := f.qualifiedName(i)
		openToken := f.tokens[last].next
		if f.byteAt(openToken) != '(' {
			continue
		}
		open := f.tokens[openToken].start
		call := functionCallSpan{name: name, normalized: lowerASCII(name), start: t.start, nameEnd: f.tokens[last].end, open: open, next: open + 1, annotation: t.annotation, syntaxKeyword: t.role == expressionKeyword}
		f.addCall(i, call)
	}
	// Scalar bodies are ordered at their openings. One sweep labels calls;
	// nested bodies never require restarting a reader.
	body, end := 0, -1
	for i := range f.calls {
		for body < len(scalarBodies) && scalarBodies[body].start <= f.calls[i].start {
			end = max(end, scalarBodies[body].end)
			body++
		}
		f.calls[i].inScalarSubquery = f.calls[i].start < end
	}
}

func (f *expressionLexicalFacts) addCall(token int, call functionCallSpan) {
	if call.open >= 0 {
		call.group = f.tokens[f.at[call.open]].group
	}
	i := len(f.calls)
	f.calls = append(f.calls, call)
	f.tokens[token].call = i
	if call.open >= 0 {
		f.tokens[f.at[call.open]].call = i
	}
}

func (f *expressionLexicalFacts) receivers() {
	type frame struct {
		primary, receiver     syntaxSpan
		groupStart, open, dot int
	}
	empty := func() frame {
		return frame{primary: syntaxSpan{-1, -1}, receiver: syntaxSpan{-1, -1}, open: -1, dot: -1}
	}
	stack := []frame{empty()}
	for _, t := range f.tokens {
		if t.kind == lexicalEnd {
			break
		}
		if t.kind == lexicalComment {
			continue
		}
		current := &stack[len(stack)-1]
		if t.opened >= 0 {
			closed := *current
			stack = stack[:len(stack)-1]
			current = &stack[len(stack)-1]
			current.primary = syntaxSpan{closed.groupStart, t.end}
			current.dot = -1
			if f.text[t.opened] == '[' && closed.receiver.start >= 0 {
				f.addPostfix(lexicalPostfix{receiver: closed.receiver, key: syntaxSpan{t.opened + 1, t.start}, open: t.opened, close: t.start, depth: t.depth - 1, subscript: true})
			}
			continue
		}
		if t.kind == lexicalName || t.kind == lexicalQuoted {
			if current.dot >= 0 && t.kind == lexicalName {
				f.addPostfix(lexicalPostfix{receiver: current.primary, key: syntaxSpan{t.start, t.end}, open: current.dot, close: t.end - 1, depth: t.depth})
				current.primary.end = t.end
				current.dot = -1
				continue
			}
			start := t.start
			if t.role == expressionParameter && current.primary.start >= 0 {
				start = current.primary.start
			}
			current.primary = syntaxSpan{start, t.end}
			current.dot = -1
			if t.role == expressionKeyword && !t.scalar {
				current.primary = syntaxSpan{-1, -1}
			}
			continue
		}
		switch c := f.text[t.start]; c {
		case '(', '[', '{':
			next := empty()
			next.open = t.start
			next.groupStart = t.start
			if c == '(' && t.call >= 0 && f.calls[t.call].expressionCall() {
				next.groupStart = f.calls[t.call].start
			}
			if (c == '[' || c == '{') && current.primary.start >= 0 && current.dot < 0 {
				next.groupStart = current.primary.start
				if c == '[' {
					next.receiver = current.primary
				}
			}
			stack = append(stack, next)
		case '.':
			if current.primary.start >= 0 && t.start+1 < len(f.text) && !isDigitByte(f.text[t.start+1]) && current.dot < 0 {
				current.dot = t.start
			} else {
				current.primary = syntaxSpan{-1, -1}
				current.dot = -1
			}
		case '$':
			current.primary = syntaxSpan{t.start, t.end}
			current.dot = -1
		default:
			if isDigitByte(c) {
				if current.primary.start >= 0 && current.primary.end == t.start && isDigitByte(f.text[current.primary.start]) {
					current.primary.end = t.end
				} else {
					current.primary = syntaxSpan{t.start, t.end}
				}
			} else {
				current.primary = syntaxSpan{-1, -1}
				current.dot = -1
			}
		}
	}
}

// tokenBefore is an indexed predecessor lookup, including boundaries inside
// a token. It never tokenizes or renders a growing prefix.
func (f *expressionLexicalFacts) tokenBefore(limit int) (expressionLexicalToken, bool) {
	low, high := 0, len(f.tokens)
	for low < high {
		mid := low + (high-low)/2
		if f.tokens[mid].start < limit {
			low = mid + 1
		} else {
			high = mid
		}
	}
	if low == 0 {
		return expressionLexicalToken{}, false
	}
	t := f.tokens[low-1]
	if t.kind == lexicalComment || t.kind == lexicalEnd {
		if t.previous < 0 {
			return expressionLexicalToken{}, false
		}
		t = f.tokens[t.previous]
	}
	t.end = min(t.end, limit)
	return t, true
}

// Index each emitted access once, so adapters never revisit preceding spans.
func (f *expressionLexicalFacts) addPostfix(access lexicalPostfix) {
	index := len(f.postfix)
	f.postfixAt[access.open] = index
	f.postfixEnd[access.close+1] = index
	f.postfix = append(f.postfix, access)
	if !access.subscript {
		f.propertyEnd[access.receiver.start] = access.close + 1
	}
	last := f.tokens[len(f.tokens)-1].previous
	if last >= 0 && access.depth == 0 && access.receiver.start == 0 && access.close+1 == f.tokens[last].end {
		f.trailing = index
	}
}
