package knowledgepolicy

import (
	"fmt"
	"strconv"
	"strings"
)

func (f *AccessFlusher) applyOnAccessMutations(entityID string, entry *AccessMetaEntry, meta EntityMeta, nowNanos int64) map[string]bool {
	appliedFixed := make(map[string]bool)
	if f.scorerFunc == nil || f.entityMeta == nil {
		return appliedFixed
	}

	ns := extractNamespace(entityID)
	scorer := f.scorerFunc(ns)
	if scorer == nil || scorer.resolver == nil {
		return appliedFixed
	}

	policy := resolveOnAccessPolicy(scorer, meta)
	if policy == nil || policy.OnAccess == nil || len(policy.OnAccess.Mutations) == 0 {
		return appliedFixed
	}

	if entry.Overflow == nil {
		entry.Overflow = make(map[string]interface{})
	}

	ctx := onAccessEvalContext{
		entry:    entry,
		nowNanos: nowNanos,
		params:   map[string]interface{}{},
	}

	for _, mutation := range policy.OnAccess.Mutations {
		targetProp, expr, ok := splitOnAccessAssignment(mutation.Expression)
		if !ok {
			continue
		}

		value, err := evalOnAccessExpression(expr, ctx)
		if err != nil {
			continue
		}

		if mutation.Kalman != nil {
			measurement, ok := toFloat64(value)
			if !ok {
				continue
			}
			value = ProcessKalmanMutation(targetProp, measurement, mutation.Kalman, entry)
		}

		if setOnAccessProperty(entry, targetProp, value) {
			appliedFixed[targetProp] = true
		}
	}

	return appliedFixed
}

func resolveOnAccessPolicy(scorer *Scorer, meta EntityMeta) *PromotionPolicyDef {
	if scorer == nil || scorer.resolver == nil {
		return nil
	}
	var cb *CompiledBinding
	if meta.Scope == ScopeEdge {
		cb = scorer.resolver.ResolveEdge(meta.EdgeType)
	} else {
		cb = scorer.resolver.ResolveNode(meta.Labels)
	}
	if cb == nil {
		return nil
	}
	return cb.PromotionPolicy
}

func scoreEntityProperty(scorer *Scorer, entityID string, meta EntityMeta, propertyKey string, entry *AccessMetaEntry, nowNanos int64) ScoringResolution {
	if meta.Scope == ScopeEdge {
		return scorer.ScoreEdgeProperty(entityID, meta.EdgeType, propertyKey, entry, meta.CreatedAtNanos, meta.VersionAtNanos, nowNanos)
	}
	return scorer.ScoreProperty(entityID, meta.Labels, propertyKey, entry, meta.CreatedAtNanos, meta.VersionAtNanos, nowNanos)
}

func splitOnAccessAssignment(expression string) (string, string, bool) {
	expression = strings.TrimSpace(expression)
	parts := strings.SplitN(expression, "=", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	lhs := strings.TrimSpace(parts[0])
	rhs := strings.TrimSpace(parts[1])
	dot := strings.IndexByte(lhs, '.')
	if dot <= 0 || dot == len(lhs)-1 {
		return "", "", false
	}
	return strings.TrimSpace(lhs[dot+1:]), rhs, true
}

type onAccessEvalContext struct {
	entry    *AccessMetaEntry
	nowNanos int64
	params   map[string]interface{}
}

type onAccessTokenType int

const (
	tokenEOF onAccessTokenType = iota
	tokenIdent
	tokenNumber
	tokenString
	tokenParam
	tokenLParen
	tokenRParen
	tokenComma
	tokenPlus
	tokenMinus
	tokenStar
	tokenSlash
	tokenDot
	tokenEq
	tokenNeq
	tokenGt
	tokenGte
	tokenLt
	tokenLte
	tokenCase
	tokenWhen
	tokenThen
	tokenElse
	tokenEnd
	tokenAnd
	tokenOr
)

type onAccessToken struct {
	typ  onAccessTokenType
	text string
}

type onAccessLexer struct {
	input string
	pos   int
}

func (l *onAccessLexer) next() onAccessToken {
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			l.pos++
			continue
		}
		break
	}
	if l.pos >= len(l.input) {
		return onAccessToken{typ: tokenEOF}
	}

	ch := l.input[l.pos]
	switch ch {
	case '(':
		l.pos++
		return onAccessToken{typ: tokenLParen, text: "("}
	case ')':
		l.pos++
		return onAccessToken{typ: tokenRParen, text: ")"}
	case ',':
		l.pos++
		return onAccessToken{typ: tokenComma, text: ","}
	case '+':
		l.pos++
		return onAccessToken{typ: tokenPlus, text: "+"}
	case '-':
		l.pos++
		return onAccessToken{typ: tokenMinus, text: "-"}
	case '*':
		l.pos++
		return onAccessToken{typ: tokenStar, text: "*"}
	case '/':
		l.pos++
		return onAccessToken{typ: tokenSlash, text: "/"}
	case '.':
		l.pos++
		return onAccessToken{typ: tokenDot, text: "."}
	case '=':
		l.pos++
		return onAccessToken{typ: tokenEq, text: "="}
	case '>':
		l.pos++
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++
			return onAccessToken{typ: tokenGte, text: ">="}
		}
		return onAccessToken{typ: tokenGt, text: ">"}
	case '<':
		l.pos++
		if l.pos < len(l.input) {
			switch l.input[l.pos] {
			case '=':
				l.pos++
				return onAccessToken{typ: tokenLte, text: "<="}
			case '>':
				l.pos++
				return onAccessToken{typ: tokenNeq, text: "<>"}
			}
		}
		return onAccessToken{typ: tokenLt, text: "<"}
	case '$':
		start := l.pos
		l.pos++
		for l.pos < len(l.input) && isOnAccessIdentChar(l.input[l.pos]) {
			l.pos++
		}
		return onAccessToken{typ: tokenParam, text: l.input[start:l.pos]}
	case '\'', '"':
		quote := ch
		start := l.pos + 1
		l.pos++
		for l.pos < len(l.input) && l.input[l.pos] != quote {
			l.pos++
		}
		text := l.input[start:l.pos]
		if l.pos < len(l.input) {
			l.pos++
		}
		return onAccessToken{typ: tokenString, text: text}
	}

	if isDigit(ch) {
		start := l.pos
		l.pos++
		for l.pos < len(l.input) && (isDigit(l.input[l.pos]) || l.input[l.pos] == '.') {
			l.pos++
		}
		return onAccessToken{typ: tokenNumber, text: l.input[start:l.pos]}
	}

	if isOnAccessIdentStart(ch) {
		start := l.pos
		l.pos++
		for l.pos < len(l.input) && isOnAccessIdentChar(l.input[l.pos]) {
			l.pos++
		}
		text := l.input[start:l.pos]
		switch strings.ToUpper(text) {
		case "CASE":
			return onAccessToken{typ: tokenCase, text: text}
		case "WHEN":
			return onAccessToken{typ: tokenWhen, text: text}
		case "THEN":
			return onAccessToken{typ: tokenThen, text: text}
		case "ELSE":
			return onAccessToken{typ: tokenElse, text: text}
		case "END":
			return onAccessToken{typ: tokenEnd, text: text}
		case "AND":
			return onAccessToken{typ: tokenAnd, text: text}
		case "OR":
			return onAccessToken{typ: tokenOr, text: text}
		default:
			return onAccessToken{typ: tokenIdent, text: text}
		}
	}

	l.pos++
	return onAccessToken{typ: tokenEOF}
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isOnAccessIdentStart(ch byte) bool {
	return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_'
}

func isOnAccessIdentChar(ch byte) bool {
	return isOnAccessIdentStart(ch) || isDigit(ch)
}

type onAccessParser struct {
	lexer onAccessLexer
	cur   onAccessToken
	ctx   onAccessEvalContext
}

func evalOnAccessExpression(expression string, ctx onAccessEvalContext) (interface{}, error) {
	p := &onAccessParser{lexer: onAccessLexer{input: expression}, ctx: ctx}
	p.next()
	value, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.cur.typ != tokenEOF {
		return nil, fmt.Errorf("unexpected token %q", p.cur.text)
	}
	return value, nil
}

func (p *onAccessParser) next() {
	p.cur = p.lexer.next()
}

func (p *onAccessParser) parseOr() (interface{}, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.cur.typ == tokenOr {
		p.next()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = truthy(left) || truthy(right)
	}
	return left, nil
}

func (p *onAccessParser) parseAnd() (interface{}, error) {
	left, err := p.parseCompare()
	if err != nil {
		return nil, err
	}
	for p.cur.typ == tokenAnd {
		p.next()
		right, err := p.parseCompare()
		if err != nil {
			return nil, err
		}
		left = truthy(left) && truthy(right)
	}
	return left, nil
}

func (p *onAccessParser) parseCompare() (interface{}, error) {
	left, err := p.parseAdd()
	if err != nil {
		return nil, err
	}
	for {
		op := p.cur.typ
		switch op {
		case tokenEq, tokenNeq, tokenGt, tokenGte, tokenLt, tokenLte:
		default:
			return left, nil
		}
		p.next()
		right, err := p.parseAdd()
		if err != nil {
			return nil, err
		}
		cmp := compareValues(left, right)
		switch op {
		case tokenEq:
			left = cmp == 0
		case tokenNeq:
			left = cmp != 0
		case tokenGt:
			left = cmp > 0
		case tokenGte:
			left = cmp >= 0
		case tokenLt:
			left = cmp < 0
		case tokenLte:
			left = cmp <= 0
		}
	}
}

func (p *onAccessParser) parseAdd() (interface{}, error) {
	left, err := p.parseMul()
	if err != nil {
		return nil, err
	}
	for p.cur.typ == tokenPlus || p.cur.typ == tokenMinus {
		op := p.cur.typ
		p.next()
		right, err := p.parseMul()
		if err != nil {
			return nil, err
		}
		left, err = arithmetic(op, left, right)
		if err != nil {
			return nil, err
		}
	}
	return left, nil
}

func (p *onAccessParser) parseMul() (interface{}, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.cur.typ == tokenStar || p.cur.typ == tokenSlash {
		op := p.cur.typ
		p.next()
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left, err = arithmetic(op, left, right)
		if err != nil {
			return nil, err
		}
	}
	return left, nil
}

func (p *onAccessParser) parseUnary() (interface{}, error) {
	if p.cur.typ == tokenMinus {
		p.next()
		value, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		if i, ok := toInt64(value); ok {
			return -i, nil
		}
		if f, ok := toFloat64(value); ok {
			return -f, nil
		}
		return nil, fmt.Errorf("cannot negate %T", value)
	}
	return p.parsePrimary()
}

func (p *onAccessParser) parsePrimary() (interface{}, error) {
	switch p.cur.typ {
	case tokenNumber:
		text := p.cur.text
		p.next()
		if strings.Contains(text, ".") {
			return strconv.ParseFloat(text, 64)
		}
		return strconv.ParseInt(text, 10, 64)
	case tokenString:
		text := p.cur.text
		p.next()
		return text, nil
	case tokenParam:
		key := strings.TrimPrefix(p.cur.text, "$")
		p.next()
		return p.ctx.params[key], nil
	case tokenCase:
		return p.parseCase()
	case tokenIdent:
		ident := p.cur.text
		p.next()
		if p.cur.typ == tokenLParen {
			return p.parseFunction(ident)
		}
		if p.cur.typ == tokenDot {
			p.next()
			if p.cur.typ != tokenIdent {
				return nil, fmt.Errorf("expected property name")
			}
			prop := p.cur.text
			p.next()
			return getOnAccessProperty(p.ctx.entry, prop), nil
		}
		switch strings.ToUpper(ident) {
		case "TRUE":
			return true, nil
		case "FALSE":
			return false, nil
		case "NULL":
			return nil, nil
		default:
			return getOnAccessProperty(p.ctx.entry, ident), nil
		}
	case tokenLParen:
		p.next()
		value, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.cur.typ != tokenRParen {
			return nil, fmt.Errorf("expected closing parenthesis")
		}
		p.next()
		return value, nil
	default:
		return nil, fmt.Errorf("unexpected token %q", p.cur.text)
	}
}

func (p *onAccessParser) parseCase() (interface{}, error) {
	p.next()
	if p.cur.typ != tokenWhen {
		return nil, fmt.Errorf("expected WHEN")
	}
	p.next()
	cond, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.cur.typ != tokenThen {
		return nil, fmt.Errorf("expected THEN")
	}
	p.next()
	thenVal, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.cur.typ != tokenElse {
		return nil, fmt.Errorf("expected ELSE")
	}
	p.next()
	elseVal, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.cur.typ != tokenEnd {
		return nil, fmt.Errorf("expected END")
	}
	p.next()
	if truthy(cond) {
		return thenVal, nil
	}
	return elseVal, nil
}

func (p *onAccessParser) parseFunction(name string) (interface{}, error) {
	if p.cur.typ != tokenLParen {
		return nil, fmt.Errorf("expected (")
	}
	p.next()
	args := make([]interface{}, 0, 2)
	for p.cur.typ != tokenRParen && p.cur.typ != tokenEOF {
		value, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		args = append(args, value)
		if p.cur.typ == tokenComma {
			p.next()
		}
	}
	if p.cur.typ != tokenRParen {
		return nil, fmt.Errorf("expected )")
	}
	p.next()
	switch strings.ToUpper(name) {
	case "TIMESTAMP":
		return p.ctx.nowNanos, nil
	case "COALESCE":
		for _, arg := range args {
			if arg != nil {
				return arg, nil
			}
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported function %s", name)
	}
}

func arithmetic(op onAccessTokenType, left, right interface{}) (interface{}, error) {
	li, lok := toInt64(left)
	ri, rok := toInt64(right)
	if lok && rok && op != tokenSlash {
		switch op {
		case tokenPlus:
			return li + ri, nil
		case tokenMinus:
			return li - ri, nil
		case tokenStar:
			return li * ri, nil
		}
	}
	lf, lok := toFloat64(left)
	rf, rok := toFloat64(right)
	if !lok || !rok {
		return nil, fmt.Errorf("arithmetic requires numeric operands")
	}
	switch op {
	case tokenPlus:
		return lf + rf, nil
	case tokenMinus:
		return lf - rf, nil
	case tokenStar:
		return lf * rf, nil
	case tokenSlash:
		if rf == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return lf / rf, nil
	default:
		return nil, fmt.Errorf("unsupported arithmetic op")
	}
}

func compareValues(left, right interface{}) int {
	if lf, ok := toFloat64(left); ok {
		if rf, ok := toFloat64(right); ok {
			switch {
			case lf < rf:
				return -1
			case lf > rf:
				return 1
			default:
				return 0
			}
		}
	}
	ls := fmt.Sprint(left)
	rs := fmt.Sprint(right)
	switch {
	case ls < rs:
		return -1
	case ls > rs:
		return 1
	default:
		return 0
	}
}

func truthy(value interface{}) bool {
	switch v := value.(type) {
	case nil:
		return false
	case bool:
		return v
	case int64:
		return v != 0
	case int:
		return v != 0
	case float64:
		return v != 0
	case string:
		return v != ""
	default:
		return true
	}
}

func toInt64(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case float64:
		return int64(v), true
	case float32:
		return int64(v), true
	default:
		return 0, false
	}
}

func toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	case float64:
		return v, true
	case float32:
		return float64(v), true
	default:
		return 0, false
	}
}

func getOnAccessProperty(entry *AccessMetaEntry, prop string) interface{} {
	if entry == nil {
		return nil
	}
	switch prop {
	case "accessCount":
		return entry.Fixed.AccessCount
	case "lastAccessedAt":
		return entry.Fixed.LastAccessedAt
	case "traversalCount":
		return entry.Fixed.TraversalCount
	case "lastTraversedAt":
		return entry.Fixed.LastTraversedAt
	default:
		if entry.Overflow == nil {
			return nil
		}
		return entry.Overflow[prop]
	}
}

func setOnAccessProperty(entry *AccessMetaEntry, prop string, value interface{}) bool {
	if entry == nil {
		return false
	}
	switch prop {
	case "accessCount":
		if v, ok := toInt64(value); ok {
			entry.Fixed.AccessCount = v
			return true
		}
	case "lastAccessedAt":
		if v, ok := toInt64(value); ok {
			entry.Fixed.LastAccessedAt = v
			return true
		}
	case "traversalCount":
		if v, ok := toInt64(value); ok {
			entry.Fixed.TraversalCount = v
			return true
		}
	case "lastTraversedAt":
		if v, ok := toInt64(value); ok {
			entry.Fixed.LastTraversedAt = v
			return true
		}
	}
	if entry.Overflow == nil {
		entry.Overflow = make(map[string]interface{})
	}
	entry.Overflow[prop] = value
	return false
}
