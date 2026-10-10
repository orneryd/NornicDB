package cypher

import "strings"

// Label expressions (#860).
//
// A colon test or a node pattern's labels can be a label expression, as in
// Neo4j 5 and GQL:
//
//	n:A|B        n has A or B
//	n:A&B        n has A and B (as the legacy n:A:B)
//	n:!A         n doesn't have A
//	n:%          n has at least one label
//	n:(A|B)&!C   grouping
//	n IS A|B     the same, GQL's spelling
//	n:$(e)       the labels e names at run time (Neo4j 5.26): all of them,
//	             as $all(e); $any(e) for any of them (dynamic_tokens.go)
//
// ! binds tighter than &, & tighter than |. A relationship's type is tested
// as its one label: r:A|B is true when its type is A or B. The legacy colon
// conjunction (A:B) can't be mixed with the new operators.

type labelExpressionKind uint8

const (
	labelExpressionName labelExpressionKind = iota
	labelExpressionAny                      // %
	labelExpressionNot
	labelExpressionAnd
	labelExpressionOr
	// labelExpressionDynamic is $(e), $all(e) or $any(e): expression is e,
	// and any is set for $any. It is resolved to names before it is matched
	// (resolveDynamic).
	labelExpressionDynamic
)

// labelExpression is a parsed label expression.
type labelExpression struct {
	kind       labelExpressionKind
	name       string
	operands   []*labelExpression
	expression string
	any        bool
}

// labelExpressionOfNames is the conjunction of names, the legacy :A:B form.
func labelExpressionOfNames(names []string) *labelExpression {
	if len(names) == 1 {
		return &labelExpression{kind: labelExpressionName, name: names[0]}
	}
	operands := make([]*labelExpression, 0, len(names))
	for _, name := range names {
		operands = append(operands, &labelExpression{kind: labelExpressionName, name: name})
	}
	return &labelExpression{kind: labelExpressionAnd, operands: operands}
}

// matches reports whether an entity with labels satisfies the expression.
func (x *labelExpression) matches(labels []string) bool {
	switch x.kind {
	case labelExpressionName:
		for _, label := range labels {
			if label == x.name {
				return true
			}
		}
		return false
	case labelExpressionAny:
		return len(labels) > 0
	case labelExpressionNot:
		return !x.operands[0].matches(labels)
	case labelExpressionAnd:
		for _, operand := range x.operands {
			if !operand.matches(labels) {
				return false
			}
		}
		return true
	case labelExpressionDynamic:
		return false // resolved before matching (resolveDynamic)
	default: // labelExpressionOr
		for _, operand := range x.operands {
			if operand.matches(labels) {
				return true
			}
		}
		return false
	}
}

// requiredLabels returns the labels every entity satisfying the expression
// has: a label scan of any of them lists every match.
func (x *labelExpression) requiredLabels() []string {
	switch x.kind {
	case labelExpressionName:
		return []string{x.name}
	case labelExpressionAnd:
		var out []string
		for _, operand := range x.operands {
			for _, label := range operand.requiredLabels() {
				if !containsString(out, label) {
					out = append(out, label)
				}
			}
		}
		return out
	case labelExpressionOr:
		if len(x.operands) == 0 {
			return nil
		}
		out := x.operands[0].requiredLabels()
		for _, operand := range x.operands[1:] {
			other := operand.requiredLabels()
			kept := out[:0:0]
			for _, label := range out {
				if containsString(other, label) {
					kept = append(kept, label)
				}
			}
			out = kept
		}
		if len(out) == 0 {
			return nil
		}
		return out
	default: // %, !
		return nil
	}
}

// names returns the expression's labels when it is only a conjunction of
// names (A, A:B, A&B), the form the label-list paths handle as they are.
func (x *labelExpression) names() ([]string, bool) {
	switch x.kind {
	case labelExpressionName:
		return []string{x.name}, true
	case labelExpressionAnd:
		var out []string
		for _, operand := range x.operands {
			names, ok := operand.names()
			if !ok {
				return nil, false
			}
			out = append(out, names...)
		}
		return out, true
	default:
		return nil, false
	}
}

// alternatives returns the expression's names when it is only a disjunction
// of names (R, R|S, (R|S)): the relationship type form every route matches.
func (x *labelExpression) alternatives() ([]string, bool) {
	switch x.kind {
	case labelExpressionName:
		return []string{x.name}, true
	case labelExpressionOr:
		if len(x.operands) == 0 {
			return nil, false // $any([]): matches nothing
		}
		var out []string
		for _, operand := range x.operands {
			names, ok := operand.alternatives()
			if !ok {
				return nil, false
			}
			out = append(out, names...)
		}
		return out, true
	default:
		return nil, false
	}
}

// String renders the expression as Neo4j does in its messages: a nested
// conjunction or disjunction of the other kind is parenthesised
// (A|(B&C), (A&B)|C, !(A|B)), one of the same kind is flattened (A&B&C).
func (x *labelExpression) String() string {
	var b strings.Builder
	x.render(&b)
	return b.String()
}

func (x *labelExpression) render(b *strings.Builder) {
	switch x.kind {
	case labelExpressionName:
		b.WriteString(labelExpressionNameText(x.name))
	case labelExpressionAny:
		b.WriteByte('%')
	case labelExpressionDynamic:
		b.WriteByte('$')
		if x.any {
			b.WriteString("any")
		}
		b.WriteByte('(')
		b.WriteString(x.expression)
		b.WriteByte(')')
	case labelExpressionAnd, labelExpressionOr:
		if len(x.operands) == 0 {
			// The empty conjunction is true, the empty disjunction false
			// ($all([]), $any([])): % or not, and % and not.
			if x.kind == labelExpressionAnd {
				b.WriteString("(%|!%)")
			} else {
				b.WriteString("(%&!%)")
			}
			return
		}
		x.renderOperands(b)
	case labelExpressionNot:
		b.WriteByte('!')
		x.operands[0].renderOperand(b, labelExpressionNot)
	}
}

func (x *labelExpression) renderOperands(b *strings.Builder) {
	separator := byte('&')
	if x.kind == labelExpressionOr {
		separator = '|'
	}
	for i, operand := range x.operands {
		if i > 0 {
			b.WriteByte(separator)
		}
		operand.renderOperand(b, x.kind)
	}
}

func (x *labelExpression) renderOperand(b *strings.Builder, parent labelExpressionKind) {
	if (x.kind == labelExpressionAnd || x.kind == labelExpressionOr) && x.kind != parent && len(x.operands) > 0 {
		b.WriteByte('(')
		x.render(b)
		b.WriteByte(')')
		return
	}
	x.render(b)
}

// labelExpressionNameText is a label or type name as Cypher text: as is
// when it is a plain identifier, backtick-quoted otherwise.
func labelExpressionNameText(name string) string {
	if isValidIdentifier(name) {
		return name
	}
	return quoteSchemaName(name)
}

// labelChainText is the legacy pattern label chain for names (:A:B), empty
// for none.
func labelChainText(names []string) string {
	var b strings.Builder
	for _, name := range names {
		b.WriteByte(':')
		b.WriteString(labelExpressionNameText(name))
	}
	return b.String()
}

// parseLabelExpressionPrefix parses the label expression at the start of
// text (the text after a colon). It returns the expression and the length of
// text it covers; ok is false when no label expression starts there. Spaces
// are allowed around operators; an operator not followed by an operand is
// left out (x:A | x.name in a list comprehension is the test x:A).
func parseLabelExpressionPrefix(text string) (expr *labelExpression, end int, ok bool) {
	p := labelExpressionParser{text: text}
	expr, ok = p.parseOr()
	if !ok {
		return nil, 0, false
	}
	return expr, p.pos, true
}

// parseLabelExpression parses text as exactly one label expression.
func parseLabelExpression(text string) (*labelExpression, bool) {
	expr, end, ok := parseLabelExpressionPrefix(text)
	if !ok || strings.TrimSpace(text[end:]) != "" {
		return nil, false
	}
	return expr, true
}

// labelChain is a pattern's or colon test's label chain as written after
// its first colon (or IS): the expression and how it was spelled.
type labelChain struct {
	expr *labelExpression
	end  int // length of the text it covers
	// colons: a colon joined two labels (A:B; the tightest conjunction, so
	// A|B:C is A|(B&C), as Neo4j reads it to suggest a rewrite).
	colons bool
	// symbols: a label expression operator or parenthesis was used.
	symbols bool
	// barColons: the legacy relationship type alternative |: (R|:S).
	barColons bool
	// dynamic: a $(e), $all(e) or $any(e) term (dynamicAny: $any).
	dynamic, dynamicAny bool
}

// scanLabelChain parses the label chain at the start of text. With
// relationship set, |: separates alternatives as | does (the legacy form).
func scanLabelChain(text string, relationship bool) (labelChain, bool) {
	p := labelExpressionParser{text: text, colons: true, relationship: relationship}
	expr, ok := p.parseOr()
	if !ok {
		return labelChain{}, false
	}
	return labelChain{expr: expr, end: p.pos, colons: p.usedColons, symbols: p.usedSymbols, barColons: p.usedBarColons,
		dynamic: p.usedDynamic, dynamicAny: p.usedDynamicAny}, true
}

type labelExpressionParser struct {
	text string
	pos  int
	// colons: A:B is a conjunction (pattern and colon-test chains).
	colons bool
	// relationship: R|:S is R|S.
	relationship bool

	usedColons, usedSymbols, usedBarColons bool
	// usedDynamic: a $(e), $all(e) or $any(e) term; usedDynamicAny: $any.
	usedDynamic, usedDynamicAny bool
}

func (p *labelExpressionParser) skipSpaces() int {
	i := p.pos
	for i < len(p.text) && isASCIISpace(p.text[i]) {
		i++
	}
	return i
}

// binary consumes op (after optional spaces) when an operand follows it.
func (p *labelExpressionParser) binary(op byte, operand func() (*labelExpression, bool)) (*labelExpression, bool) {
	at := p.skipSpaces()
	if at >= len(p.text) || p.text[at] != op {
		return nil, false
	}
	saved := p.pos
	p.pos = at + 1
	barColon := false
	if op == '|' && p.relationship && p.pos < len(p.text) && p.text[p.pos] == ':' {
		p.pos++
		barColon = true
	}
	expr, ok := operand()
	if !ok {
		p.pos = saved
		return nil, false
	}
	if barColon {
		p.usedBarColons = true
	} else {
		p.usedSymbols = true
	}
	return expr, true
}

func (p *labelExpressionParser) parseOr() (*labelExpression, bool) {
	first, ok := p.parseAnd()
	if !ok {
		return nil, false
	}
	operands := []*labelExpression{first}
	for {
		next, ok := p.binary('|', p.parseAnd)
		if !ok {
			break
		}
		operands = append(operands, next)
	}
	if len(operands) == 1 {
		return first, true
	}
	return &labelExpression{kind: labelExpressionOr, operands: operands}, true
}

func (p *labelExpressionParser) parseAnd() (*labelExpression, bool) {
	first, ok := p.parseColons()
	if !ok {
		return nil, false
	}
	operands := []*labelExpression{first}
	for {
		next, ok := p.binary('&', p.parseColons)
		if !ok {
			break
		}
		operands = append(operands, next)
	}
	if len(operands) == 1 {
		return first, true
	}
	return &labelExpression{kind: labelExpressionAnd, operands: operands}, true
}

// parseColons reads a colon-joined run of unary terms (A:B:!C) when colons
// are allowed; a colon must touch the term before it or follow spaces only.
func (p *labelExpressionParser) parseColons() (*labelExpression, bool) {
	first, ok := p.parseUnary()
	if !ok || !p.colons {
		return first, ok
	}
	operands := []*labelExpression{first}
	for {
		at := p.skipSpaces()
		if at >= len(p.text) || p.text[at] != ':' || (at+1 < len(p.text) && p.text[at+1] == ':') {
			break
		}
		saved := p.pos
		p.pos = at + 1
		next, ok := p.parseUnary()
		if !ok {
			p.pos = saved
			break
		}
		p.usedColons = true
		operands = append(operands, next)
	}
	if len(operands) == 1 {
		return first, true
	}
	return &labelExpression{kind: labelExpressionAnd, operands: operands}, true
}

func (p *labelExpressionParser) parseUnary() (*labelExpression, bool) {
	at := p.skipSpaces()
	if at >= len(p.text) {
		return nil, false
	}
	switch p.text[at] {
	case '!':
		if at+1 < len(p.text) && p.text[at+1] == '=' {
			return nil, false // != is a comparison
		}
		saved := p.pos
		p.pos = at + 1
		operand, ok := p.parseUnary()
		if !ok {
			p.pos = saved
			return nil, false
		}
		p.usedSymbols = true
		return &labelExpression{kind: labelExpressionNot, operands: []*labelExpression{operand}}, true
	case '%':
		p.pos = at + 1
		p.usedSymbols = true
		return &labelExpression{kind: labelExpressionAny}, true
	case '$':
		// A dynamic label: $(e), $all(e), $any(e).
		open, any := at+1, false
		for _, function := range [...]string{"all", "any"} {
			if at+1+len(function) < len(p.text) && strings.EqualFold(p.text[at+1:at+1+len(function)], function) && p.text[at+1+len(function)] == '(' {
				open, any = at+1+len(function), function == "any"
			}
		}
		if open >= len(p.text) || p.text[open] != '(' {
			return nil, false
		}
		closing := findMatchingDelimiter(p.text, open, '(', ')')
		if closing < 0 {
			return nil, false
		}
		p.pos = closing + 1
		p.usedDynamic = true
		p.usedDynamicAny = p.usedDynamicAny || any
		return &labelExpression{kind: labelExpressionDynamic, expression: strings.TrimSpace(p.text[open+1 : closing]), any: any}, true
	case '(':
		saved := p.pos
		p.pos = at + 1
		inner, ok := p.parseOr()
		if !ok {
			p.pos = saved
			return nil, false
		}
		closing := p.skipSpaces()
		if closing >= len(p.text) || p.text[closing] != ')' {
			p.pos = saved
			return nil, false
		}
		p.pos = closing + 1
		p.usedSymbols = true
		return inner, true
	}
	written, end, ok := scanSymbolicName(p.text, at)
	if !ok {
		return nil, false
	}
	p.pos = end
	return &labelExpression{kind: labelExpressionName, name: symbolicNameValue(written)}, true
}

// hasLabelExpressionOperator reports whether a label chain uses the label
// expression operators rather than the plain A:B form.
func hasLabelExpressionOperator(chain string) bool {
	inBacktick := false
	for i := 0; i < len(chain); i++ {
		switch c := chain[i]; {
		case c == '`':
			inBacktick = !inBacktick
		case inBacktick:
		case c == '|' || c == '&' || c == '!' || c == '%' || c == '(':
			return true
		}
	}
	return false
}

// labelExpressionBarAt reports whether the | at s[at] is a label expression
// operator of a colon test (m:A|B), not a list or pattern comprehension's
// projection bar: it touches a label term on both sides and the text before
// it, back to a colon after a variable, is a label chain. start bounds the
// look back.
func labelExpressionBarAt(s string, start, at int) bool {
	if at <= start || at+1 >= len(s) {
		return false
	}
	before, after := s[at-1], s[at+1]
	if !(isIdentByte(before) || before == '`' || before == ')' || before == '%') ||
		!(isIdentByte(after) || after == '`' || after == '(' || after == '!' || after == '%') {
		return false
	}
	for i := at - 1; i >= start; i-- {
		switch c := s[i]; {
		case c == '`':
			// A quoted name: back to its opening backtick.
			open := strings.LastIndexByte(s[start:i], '`')
			if open < 0 {
				return false
			}
			i = start + open
		case isIdentByte(c) || c == '|' || c == '&' || c == '!' || c == '%' || c == '(' || c == ')':
		case c == ':':
			return i > start && isIdentByte(s[i-1])
		default:
			return false
		}
	}
	return false
}

// predicate renders the expression as a boolean expression over variable,
// for a MATCH pattern whose dynamic terms depend on the row: a name is the
// label test variable:Name, a dynamic term the dynamicLabelTestFunction call
// that reads its value per row.
func (x *labelExpression) predicate(variable string) string {
	switch x.kind {
	case labelExpressionName:
		return variable + ":" + labelExpressionNameText(x.name)
	case labelExpressionAny:
		return variable + ":%"
	case labelExpressionDynamic:
		any := "false"
		if x.any {
			any = "true"
		}
		return dynamicLabelTestFunction + "(" + variable + ", (" + x.expression + "), " + any + ")"
	case labelExpressionNot:
		return "NOT (" + x.operands[0].predicate(variable) + ")"
	}
	if len(x.operands) == 0 {
		if x.kind == labelExpressionAnd {
			return "true"
		}
		return "false"
	}
	separator := " AND "
	if x.kind == labelExpressionOr {
		separator = " OR "
	}
	parts := make([]string, len(x.operands))
	for i, operand := range x.operands {
		parts[i] = operand.predicate(variable)
	}
	return "(" + strings.Join(parts, separator) + ")"
}

// resolveDynamic replaces the expression's dynamic terms with the names their
// values give (resolve; dynamicLabelNames): $(e) and $all(e) by the
// conjunction of the names, $any(e) by their disjunction. constant is false,
// with no expression, when resolve can't read some term before the rows
// exist (its value depends on them).
func (x *labelExpression) resolveDynamic(resolve func(expression string) (value interface{}, constant bool, err error)) (resolved *labelExpression, constant bool, err error) {
	switch x.kind {
	case labelExpressionDynamic:
		value, constant, err := resolve(x.expression)
		if err != nil || !constant {
			return nil, false, err
		}
		names, err := dynamicLabelNames(value)
		if err != nil {
			return nil, false, err
		}
		if len(names) == 1 {
			return &labelExpression{kind: labelExpressionName, name: names[0]}, true, nil
		}
		kind := labelExpressionAnd
		if x.any {
			kind = labelExpressionOr
		}
		operands := make([]*labelExpression, 0, len(names))
		for _, name := range names {
			operands = append(operands, &labelExpression{kind: labelExpressionName, name: name})
		}
		return &labelExpression{kind: kind, operands: operands}, true, nil
	case labelExpressionNot, labelExpressionAnd, labelExpressionOr:
		operands := make([]*labelExpression, 0, len(x.operands))
		for _, operand := range x.operands {
			resolvedOperand, constant, err := operand.resolveDynamic(resolve)
			if err != nil || !constant {
				return nil, false, err
			}
			operands = append(operands, resolvedOperand)
		}
		return &labelExpression{kind: x.kind, operands: operands}, true, nil
	}
	return x, true, nil
}
