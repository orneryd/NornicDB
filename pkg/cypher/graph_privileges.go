package cypher

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Graph read privileges, in Neo4j 5's syntax (Operations Manual, "Read
// privileges" and "Property-based access control"):
//
//	GRANT [IMMUTABLE] MATCH {*} ON GRAPH[S] {* | name[, …]} [segment] TO role[, …]
//	DENY  [IMMUTABLE] MATCH {*} ON GRAPH[S] {* | name[, …]} [segment] TO role[, …]
//	REVOKE [GRANT | DENY] MATCH {*} ON GRAPH[S] {* | name[, …]} [segment] FROM role[, …]
//
//	segment := NODE[S] {* | Label[, …]} [(*)]
//	         | FOR (n[:Label] [{p: value}]) [WHERE rule]
//	rule    := n.p {= | <> | < | <= | > | >=} value | n.p IS [NOT] NULL
//	         | n.p IN [value, …] | value IN n.p
//	         | (n)-[:REL]->(:Collection {key: value})     (NornicDB extension)
//
// The last rule form is the one NornicDB extension: a collection rule, which
// grants or denies the nodes of Label that have a REL relationship to the
// collection whose key is value, and, through the graph's access scope,
// every node derived from them. A login whose roles hold a collection rule on
// a graph is restricted there: it sees what its grants match and its denies
// don't (GraphVisibility). Values are literals or $parameters, fixed when the
// privilege is granted. Only MATCH {*} on nodes exists in this slice: TRAVERSE,
// READ, property sets and RELATIONSHIPS / ELEMENTS segments are refused as
// unsupported; a relationship is visible when both its ends are.

// graphSegmentKind is what a graph privilege's segment matches.
type graphSegmentKind int

const (
	segmentNodes      graphSegmentKind = iota // NODES * or NODES Label
	segmentProperty                           // FOR (n:Label) WHERE n.p … value
	segmentCollection                         // FOR (n:Label) WHERE (n)-[:R]->(:C {k: v})
)

// graphSegment is one parsed privilege segment.
type graphSegment struct {
	kind  graphSegmentKind
	label string // "" for every label (NODES *, FOR (n))

	property string      // property rules: the property
	operator string      // =, <>, <, <=, >, >=, IS NULL, IS NOT NULL, IN, CONTAINS-IN (value IN n.p)
	value    interface{} // property rules: the value

	relType     string // collection rules
	outgoing    bool
	target      string // the collection label
	targetKey   string
	targetValue string
}

// canonical is the segment's stored text, which parseGraphSegment reads back.
func (s graphSegment) canonical() string {
	node := "(n)"
	if s.label != "" {
		node = "(n:" + quoteIdentifier(s.label) + ")"
	}
	switch s.kind {
	case segmentProperty:
		property := "n." + quoteIdentifier(s.property)
		switch s.operator {
		case "IS NULL", "IS NOT NULL":
			return "FOR " + node + " WHERE " + property + " " + s.operator
		case "VALUE IN":
			return "FOR " + node + " WHERE " + privilegeLiteral(s.value) + " IN " + property
		}
		return "FOR " + node + " WHERE " + property + " " + s.operator + " " + privilegeLiteral(s.value)
	case segmentCollection:
		left, right := "-", "->"
		if !s.outgoing {
			left, right = "<-", "-"
		}
		return "FOR " + node + " WHERE (n)" + left + "[:" + quoteIdentifier(s.relType) + "]" + right +
			"(:" + quoteIdentifier(s.target) + " {" + quoteIdentifier(s.targetKey) + ": " + privilegeLiteral(s.targetValue) + "})"
	}
	if s.label == "" {
		return "NODES *"
	}
	return "NODES " + quoteIdentifier(s.label)
}

// display is the segment as Neo4j's SHOW PRIVILEGES lists it.
func (s graphSegment) display() string {
	if s.kind == segmentNodes {
		if s.label == "" {
			return "NODE(*)"
		}
		return "NODE(" + s.label + ")"
	}
	return s.canonical()
}

func quoteIdentifier(name string) string {
	plain := name != ""
	for i, r := range name {
		if !(unicode.IsLetter(r) || r == '_' || (i > 0 && unicode.IsDigit(r))) {
			plain = false
			break
		}
	}
	if plain {
		return name
	}
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// privilegeLiteral prints a rule value as a Cypher literal.
func privilegeLiteral(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return "null"
	case string:
		return "'" + strings.ReplaceAll(strings.ReplaceAll(v, `\`, `\\`), "'", `\'`) + "'"
	case bool:
		return strconv.FormatBool(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		if v == math.Trunc(v) && !math.IsInf(v, 0) && math.Abs(v) < 1e15 {
			return strconv.FormatFloat(v, 'f', 1, 64)
		}
		return strconv.FormatFloat(v, 'g', -1, 64)
	case []interface{}:
		parts := make([]string, len(v))
		for i, item := range v {
			parts[i] = privilegeLiteral(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
	return fmt.Sprint(value)
}

// matches reports whether the segment's node test holds for a node with
// labels and properties. A collection segment's test needs the graph, so it
// is decided by GraphVisibility, not here; it never matches here.
func (s graphSegment) matches(labels []string, properties map[string]interface{}) bool {
	if s.label != "" {
		found := false
		for _, label := range labels {
			if label == s.label {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	switch s.kind {
	case segmentNodes:
		return true
	case segmentProperty:
		return propertyRuleHolds(s.operator, properties[s.property], s.value)
	}
	return false
}

// propertyRuleHolds is a property rule's comparison, with Cypher's
// semantics: a null on either side matches nothing except IS NULL.
func propertyRuleHolds(operator string, actual, value interface{}) bool {
	switch operator {
	case "IS NULL":
		return actual == nil
	case "IS NOT NULL":
		return actual != nil
	}
	if actual == nil {
		return false
	}
	switch operator {
	case "=":
		return compareValues(actual, value)
	case "<>":
		return value != nil && !compareValues(actual, value)
	case "IN":
		list, _ := value.([]interface{})
		for _, item := range list {
			if compareValues(actual, item) {
				return true
			}
		}
		return false
	case "VALUE IN":
		list, ok := toInterfaceList(actual)
		if !ok {
			return false
		}
		for _, item := range list {
			if compareValues(item, value) {
				return true
			}
		}
		return false
	}
	order, comparable := orderValues(actual, value)
	if !comparable {
		return false
	}
	switch operator {
	case "<":
		return order < 0
	case "<=":
		return order <= 0
	case ">":
		return order > 0
	case ">=":
		return order >= 0
	}
	return false
}

func toInterfaceList(value interface{}) ([]interface{}, bool) {
	switch list := value.(type) {
	case []interface{}:
		return list, true
	case []string:
		out := make([]interface{}, len(list))
		for i, item := range list {
			out[i] = item
		}
		return out, true
	case []int64:
		out := make([]interface{}, len(list))
		for i, item := range list {
			out[i] = item
		}
		return out, true
	case []float64:
		out := make([]interface{}, len(list))
		for i, item := range list {
			out[i] = item
		}
		return out, true
	}
	return nil, false
}

// orderValues orders two numbers or two strings; other pairs don't compare.
func orderValues(a, b interface{}) (int, bool) {
	if x, ok := strictNumericValue(a); ok {
		if y, ok := strictNumericValue(b); ok {
			switch {
			case x < y:
				return -1, true
			case x > y:
				return 1, true
			}
			return 0, true
		}
		return 0, false
	}
	x, okA := a.(string)
	y, okB := b.(string)
	if !okA || !okB {
		return 0, false
	}
	return strings.Compare(x, y), true
}

// graphPrivilegeCommand is a parsed GRANT / DENY / REVOKE MATCH.
type graphPrivilegeCommand struct {
	verb      string // GRANT, DENY, REVOKE
	revokeOf  string // REVOKE GRANT / REVOKE DENY; "" revokes both
	immutable bool
	graphs    []string // "*" or names
	segments  []graphSegment
	roles     []string
}

// privilegeLexer splits a privilege statement into tokens: words,
// backtick-quoted names, string and number literals, $parameters and
// punctuation (including the two-character operators).
type privilegeToken struct {
	text  string
	kind  byte // 'w' word, 'q' quoted name, 's' string, 'n' number, 'p' parameter, 'o' punctuation
	value interface{}
}

func lexPrivilegeStatement(statement string) ([]privilegeToken, error) {
	runes := []rune(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(statement), ";")))
	var tokens []privilegeToken
	for i := 0; i < len(runes); {
		r := runes[i]
		switch {
		case unicode.IsSpace(r):
			i++
		case r == '`':
			var b strings.Builder
			i++
			for {
				if i >= len(runes) {
					return nil, errors.New("unterminated quoted name")
				}
				if runes[i] == '`' {
					if i+1 < len(runes) && runes[i+1] == '`' {
						b.WriteRune('`')
						i += 2
						continue
					}
					i++
					break
				}
				b.WriteRune(runes[i])
				i++
			}
			tokens = append(tokens, privilegeToken{text: b.String(), kind: 'q'})
		case r == '\'' || r == '"':
			quote := r
			var b strings.Builder
			i++
			for {
				if i >= len(runes) {
					return nil, errors.New("unterminated string")
				}
				if runes[i] == '\\' && i+1 < len(runes) {
					switch runes[i+1] {
					case 'n':
						b.WriteRune('\n')
					case 't':
						b.WriteRune('\t')
					default:
						b.WriteRune(runes[i+1])
					}
					i += 2
					continue
				}
				if runes[i] == quote {
					i++
					break
				}
				b.WriteRune(runes[i])
				i++
			}
			tokens = append(tokens, privilegeToken{text: b.String(), kind: 's', value: b.String()})
		case r == '$':
			start := i + 1
			i++
			for i < len(runes) && (unicode.IsLetter(runes[i]) || unicode.IsDigit(runes[i]) || runes[i] == '_') {
				i++
			}
			if i == start {
				return nil, errors.New("invalid parameter")
			}
			tokens = append(tokens, privilegeToken{text: string(runes[start:i]), kind: 'p'})
		case unicode.IsDigit(r) || (r == '-' && i+1 < len(runes) && unicode.IsDigit(runes[i+1]) && (len(tokens) == 0 || tokens[len(tokens)-1].kind == 'o' && tokens[len(tokens)-1].text != ")" && tokens[len(tokens)-1].text != "]")):
			start := i
			i++
			for i < len(runes) && (unicode.IsDigit(runes[i]) || runes[i] == '.' || runes[i] == 'e' || runes[i] == 'E') {
				i++
			}
			text := string(runes[start:i])
			if integer, err := strconv.ParseInt(text, 10, 64); err == nil {
				tokens = append(tokens, privilegeToken{text: text, kind: 'n', value: integer})
			} else if float, err := strconv.ParseFloat(text, 64); err == nil {
				tokens = append(tokens, privilegeToken{text: text, kind: 'n', value: float})
			} else {
				return nil, fmt.Errorf("invalid number %q", text)
			}
		case unicode.IsLetter(r) || r == '_':
			start := i
			for i < len(runes) && (unicode.IsLetter(runes[i]) || unicode.IsDigit(runes[i]) || runes[i] == '_') {
				i++
			}
			tokens = append(tokens, privilegeToken{text: string(runes[start:i]), kind: 'w'})
		default:
			two := ""
			if i+1 < len(runes) {
				two = string(runes[i : i+2])
			}
			switch two {
			case "<>", "<=", ">=", "->", "<-":
				tokens = append(tokens, privilegeToken{text: two, kind: 'o'})
				i += 2
				continue
			}
			if strings.ContainsRune("(){}[]:,.=<>-*", r) {
				tokens = append(tokens, privilegeToken{text: string(r), kind: 'o'})
				i++
				continue
			}
			return nil, fmt.Errorf("unexpected %q", string(r))
		}
	}
	return tokens, nil
}

type privilegeParser struct {
	tokens []privilegeToken
	pos    int
	params map[string]interface{}
}

func (p *privilegeParser) peek() privilegeToken {
	if p.pos < len(p.tokens) {
		return p.tokens[p.pos]
	}
	return privilegeToken{}
}

func (p *privilegeParser) keyword(word string) bool {
	if t := p.peek(); t.kind == 'w' && strings.EqualFold(t.text, word) {
		p.pos++
		return true
	}
	return false
}

func (p *privilegeParser) punct(text string) bool {
	if t := p.peek(); t.kind == 'o' && t.text == text {
		p.pos++
		return true
	}
	return false
}

func (p *privilegeParser) expectPunct(text string) error {
	if !p.punct(text) {
		return fmt.Errorf("expected %q", text)
	}
	return nil
}

// name reads an identifier, a quoted name or a string $parameter.
func (p *privilegeParser) name() (string, error) {
	t := p.peek()
	switch t.kind {
	case 'w', 'q':
		p.pos++
		return t.text, nil
	case 'p':
		p.pos++
		value, ok := p.params[t.text].(string)
		if !ok || strings.TrimSpace(value) == "" {
			return "", fmt.Errorf("parameter $%s must be a non-empty string", t.text)
		}
		return value, nil
	}
	return "", errors.New("expected a name")
}

func (p *privilegeParser) names() ([]string, error) {
	var out []string
	for {
		name, err := p.name()
		if err != nil {
			return nil, err
		}
		out = append(out, name)
		if !p.punct(",") {
			return out, nil
		}
	}
}

// literal reads a rule value: a string, number, boolean, list of those, or
// a $parameter (its value now).
func (p *privilegeParser) literal() (interface{}, error) {
	t := p.peek()
	switch t.kind {
	case 's', 'n':
		p.pos++
		return t.value, nil
	case 'p':
		p.pos++
		value, ok := p.params[t.text]
		if !ok {
			return nil, fmt.Errorf("expected parameter $%s", t.text)
		}
		return normalizePrivilegeValue(value)
	case 'w':
		switch strings.ToLower(t.text) {
		case "true":
			p.pos++
			return true, nil
		case "false":
			p.pos++
			return false, nil
		}
	case 'o':
		if t.text == "[" {
			p.pos++
			var list []interface{}
			if p.punct("]") {
				return []interface{}{}, nil
			}
			for {
				item, err := p.literal()
				if err != nil {
					return nil, err
				}
				list = append(list, item)
				if p.punct("]") {
					return list, nil
				}
				if err := p.expectPunct(","); err != nil {
					return nil, err
				}
			}
		}
	}
	return nil, errors.New("expected a literal value")
}

// normalizePrivilegeValue keeps a parameter value a rule can hold: a string,
// boolean, number (integers as int64) or a list of those.
func normalizePrivilegeValue(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case string, bool, int64, float64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case float32:
		return float64(v), nil
	case []interface{}:
		out := make([]interface{}, len(v))
		for i, item := range v {
			normalized, err := normalizePrivilegeValue(item)
			if err != nil {
				return nil, err
			}
			out[i] = normalized
		}
		return out, nil
	case []string:
		out := make([]interface{}, len(v))
		for i, item := range v {
			out[i] = item
		}
		return out, nil
	}
	return nil, fmt.Errorf("a rule value must be a string, number, boolean or list, got %T", value)
}

// segments reads the optional segment after the graphs: NODE[S] … or
// FOR (…) [WHERE …]. None means every node.
func (p *privilegeParser) segments() ([]graphSegment, error) {
	switch {
	case p.keyword("NODES"), p.keyword("NODE"):
		if p.punct("*") {
			p.optionalElementStar()
			return []graphSegment{{kind: segmentNodes}}, nil
		}
		labels, err := p.names()
		if err != nil {
			return nil, err
		}
		p.optionalElementStar()
		segments := make([]graphSegment, 0, len(labels))
		for _, label := range labels {
			segments = append(segments, graphSegment{kind: segmentNodes, label: label})
		}
		return segments, nil
	case p.keyword("FOR"):
		segment, err := p.forRule()
		if err != nil {
			return nil, err
		}
		return []graphSegment{segment}, nil
	case p.keyword("RELATIONSHIPS"), p.keyword("RELATIONSHIP"), p.keyword("ELEMENTS"), p.keyword("ELEMENT"):
		return nil, errUnsupportedPrivilege
	}
	return []graphSegment{{kind: segmentNodes}}, nil
}

// optionalElementStar reads NODES … (*).
func (p *privilegeParser) optionalElementStar() {
	start := p.pos
	if p.punct("(") && p.punct("*") && p.punct(")") {
		return
	}
	p.pos = start
}

var errUnsupportedPrivilege = errors.New("unsupported privilege")

// forRule reads FOR (n[:Label] [{p: v}]) [WHERE rule].
func (p *privilegeParser) forRule() (graphSegment, error) {
	var segment graphSegment
	if err := p.expectPunct("("); err != nil {
		return segment, err
	}
	variable, err := p.name()
	if err != nil {
		return segment, err
	}
	if p.punct(":") {
		if segment.label, err = p.name(); err != nil {
			return segment, err
		}
	}
	if p.punct("{") {
		segment.kind = segmentProperty
		if segment.property, err = p.name(); err != nil {
			return segment, err
		}
		if err := p.expectPunct(":"); err != nil {
			return segment, err
		}
		segment.operator = "="
		if segment.value, err = p.literal(); err != nil {
			return segment, err
		}
		if err := p.expectPunct("}"); err != nil {
			return segment, err
		}
		if err := p.expectPunct(")"); err != nil {
			return segment, err
		}
		return segment, nil
	}
	if err := p.expectPunct(")"); err != nil {
		return segment, err
	}
	if !p.keyword("WHERE") {
		return graphSegment{kind: segmentNodes, label: segment.label}, nil
	}
	if p.peek().kind == 'o' && p.peek().text == "(" {
		return p.collectionRule(segment, variable)
	}
	propertyOf := func() (string, error) {
		owner, err := p.name()
		if err != nil {
			return "", err
		}
		if owner != variable {
			return "", fmt.Errorf("a rule reads the properties of %s", variable)
		}
		if err := p.expectPunct("."); err != nil {
			return "", err
		}
		return p.name()
	}
	segment.kind = segmentProperty
	if t := p.peek(); t.kind == 'w' && t.text == variable || t.kind == 'q' && t.text == variable {
		if segment.property, err = propertyOf(); err != nil {
			return segment, err
		}
		switch {
		case p.keyword("IS"):
			segment.operator = "IS NULL"
			if p.keyword("NOT") {
				segment.operator = "IS NOT NULL"
			}
			if !p.keyword("NULL") {
				return segment, errors.New("expected NULL")
			}
			return segment, nil
		case p.keyword("IN"):
			segment.operator = "IN"
			if segment.value, err = p.literal(); err != nil {
				return segment, err
			}
			if _, isList := segment.value.([]interface{}); !isList {
				return segment, errors.New("IN needs a list")
			}
			return segment, nil
		}
		for _, operator := range []string{"=", "<>", "<=", ">=", "<", ">"} {
			if p.punct(operator) {
				segment.operator = operator
				segment.value, err = p.literal()
				return segment, err
			}
		}
		return segment, errors.New("expected a comparison")
	}
	// value IN n.p
	if segment.value, err = p.literal(); err != nil {
		return segment, err
	}
	if !p.keyword("IN") {
		return segment, errors.New("expected IN")
	}
	segment.operator = "VALUE IN"
	segment.property, err = propertyOf()
	return segment, err
}

// collectionRule reads (n)-[:R]->(:C {k: v}) or (n)<-[:R]-(:C {k: v}).
func (p *privilegeParser) collectionRule(segment graphSegment, variable string) (graphSegment, error) {
	segment.kind = segmentCollection
	if err := p.expectPunct("("); err != nil {
		return segment, err
	}
	if name, err := p.name(); err != nil || name != variable {
		return segment, fmt.Errorf("a collection rule starts at %s", variable)
	}
	if err := p.expectPunct(")"); err != nil {
		return segment, err
	}
	switch {
	case p.punct("-"):
		segment.outgoing = true
	case p.punct("<-"):
	default:
		return segment, errors.New("expected a relationship")
	}
	if err := p.expectPunct("["); err != nil {
		return segment, err
	}
	if err := p.expectPunct(":"); err != nil {
		return segment, err
	}
	var err error
	if segment.relType, err = p.name(); err != nil {
		return segment, err
	}
	if err := p.expectPunct("]"); err != nil {
		return segment, err
	}
	if segment.outgoing {
		err = p.expectPunct("->")
	} else {
		err = p.expectPunct("-")
	}
	if err != nil {
		return segment, err
	}
	if err := p.expectPunct("("); err != nil {
		return segment, err
	}
	if err := p.expectPunct(":"); err != nil {
		return segment, err
	}
	if segment.target, err = p.name(); err != nil {
		return segment, err
	}
	if err := p.expectPunct("{"); err != nil {
		return segment, err
	}
	if segment.targetKey, err = p.name(); err != nil {
		return segment, err
	}
	if err := p.expectPunct(":"); err != nil {
		return segment, err
	}
	value, err := p.literal()
	if err != nil {
		return segment, err
	}
	key, ok := value.(string)
	if !ok || key == "" {
		return segment, errors.New("a collection key is a non-empty string")
	}
	segment.targetValue = key
	if err := p.expectPunct("}"); err != nil {
		return segment, err
	}
	return segment, p.expectPunct(")")
}

// isGraphPrivilegeCommand reports a GRANT / DENY / REVOKE of a graph read
// privilege (… MATCH | READ | TRAVERSE … ON GRAPH …).
func isGraphPrivilegeCommand(cypher string) bool {
	trimmed := strings.TrimSpace(cypher)
	if !startsWithKeywordFold(trimmed, "GRANT") && !startsWithKeywordFold(trimmed, "DENY") && !startsWithKeywordFold(trimmed, "REVOKE") {
		return false
	}
	head := strings.ToUpper(trimmed)
	if len(head) > 120 {
		head = head[:120]
	}
	fields := strings.Fields(strings.NewReplacer("{", " { ", "}", " } ").Replace(head))
	for i, field := range fields {
		if i > 3 {
			break
		}
		if field == "MATCH" || field == "READ" || field == "TRAVERSE" {
			return true
		}
	}
	return false
}

// parseGraphPrivilegeCommand parses a graph privilege command.
func parseGraphPrivilegeCommand(cypher string, params map[string]interface{}) (graphPrivilegeCommand, error) {
	var command graphPrivilegeCommand
	tokens, err := lexPrivilegeStatement(cypher)
	if err != nil {
		return command, err
	}
	p := &privilegeParser{tokens: tokens, params: params}
	switch {
	case p.keyword("GRANT"):
		command.verb = "GRANT"
	case p.keyword("DENY"):
		command.verb = "DENY"
	case p.keyword("REVOKE"):
		command.verb = "REVOKE"
		switch {
		case p.keyword("GRANT"):
			command.revokeOf = "GRANT"
		case p.keyword("DENY"):
			command.revokeOf = "DENY"
		}
	}
	command.immutable = p.keyword("IMMUTABLE")
	switch {
	case p.keyword("MATCH"):
	case p.keyword("READ"), p.keyword("TRAVERSE"):
		return command, errUnsupportedPrivilege
	default:
		return command, errors.New("expected MATCH")
	}
	if err := p.expectPunct("{"); err != nil {
		return command, err
	}
	if !p.punct("*") {
		return command, errUnsupportedPrivilege
	}
	if err := p.expectPunct("}"); err != nil {
		return command, err
	}
	if !p.keyword("ON") {
		return command, errors.New("expected ON")
	}
	if p.keyword("HOME") || p.keyword("DEFAULT") {
		return command, errUnsupportedPrivilege
	}
	if !p.keyword("GRAPHS") && !p.keyword("GRAPH") {
		return command, errors.New("expected GRAPH")
	}
	if p.punct("*") {
		command.graphs = []string{"*"}
	} else if command.graphs, err = p.names(); err != nil {
		return command, err
	}
	if command.segments, err = p.segments(); err != nil {
		return command, err
	}
	preposition := "TO"
	if command.verb == "REVOKE" {
		preposition = "FROM"
	}
	if !p.keyword(preposition) {
		return command, errors.New("expected " + preposition)
	}
	if command.roles, err = p.names(); err != nil {
		return command, err
	}
	if p.pos != len(p.tokens) {
		return command, fmt.Errorf("unexpected %q", p.peek().text)
	}
	return command, nil
}

// parseGraphSegment reads a stored segment's canonical text.
func parseGraphSegment(text string) (graphSegment, error) {
	tokens, err := lexPrivilegeStatement(text)
	if err != nil {
		return graphSegment{}, err
	}
	p := &privilegeParser{tokens: tokens}
	segments, err := p.segments()
	if err != nil {
		return graphSegment{}, err
	}
	if len(segments) != 1 || p.pos != len(p.tokens) {
		return graphSegment{}, fmt.Errorf("invalid stored segment %q", text)
	}
	return segments[0], nil
}

// executeGraphPrivilegeCommand runs GRANT / DENY / REVOKE MATCH. It returns
// no rows. GRANT and DENY need every role to exist (Neo4j's ArgumentError);
// REVOKE of a privilege a role doesn't hold changes nothing. IMMUTABLE
// privileges are refused as unsupported.
func (e *StorageExecutor) executeGraphPrivilegeCommand(ctx context.Context, cypher string) (*ExecuteResult, error) {
	command, err := parseGraphPrivilegeCommand(cypher, getParamsFromContext(ctx))
	if errors.Is(err, errUnsupportedPrivilege) {
		return nil, unsupportedAdministrationCommandError(cypher)
	}
	if err != nil {
		return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax",
			localization.CypherAdminRoleCommandInvalid(strings.TrimSpace(cypher), err.Error()))
	}
	if command.immutable {
		return nil, unsupportedAdministrationCommandError(cypher)
	}
	system, err := e.privilegeStorage(true)
	if err != nil {
		return nil, err
	}
	var privileges []auth.GraphPrivilege
	for _, role := range command.roles {
		for _, graph := range command.graphs {
			for _, segment := range command.segments {
				privileges = append(privileges, auth.GraphPrivilege{Role: role, Graph: graph, Segment: segment.canonical()})
			}
		}
	}
	empty := &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}
	if command.verb == "REVOKE" {
		store := auth.NewGraphPrivilegeStore(system)
		for _, privilege := range privileges {
			for _, denied := range []bool{false, true} {
				if command.revokeOf == "GRANT" && denied || command.revokeOf == "DENY" && !denied {
					continue
				}
				privilege.Denied = denied
				if _, err := store.Remove(ctx, privilege); err != nil {
					return nil, err
				}
			}
		}
		return empty, nil
	}
	err = auth.WithRoleRecords(ctx, system, publicExcluded(command.roles), func(view storage.Engine) error {
		store := auth.NewGraphPrivilegeStore(view)
		for _, privilege := range privileges {
			privilege.Denied = command.verb == "DENY"
			if err := store.Put(ctx, privilege); err != nil {
				return err
			}
		}
		return nil
	})
	var missing *auth.RoleNotFoundError
	if errors.As(err, &missing) {
		return nil, roleCommandError(auth.ErrRoleNotFound, missing.Role, "")
	}
	if err != nil {
		return nil, err
	}
	return empty, nil
}

// publicRole is Neo4j's PUBLIC role, which every user holds; it has no role
// record, so it is never locked or required to exist.
const publicRole = "public"

func publicExcluded(roles []string) []string {
	out := make([]string, 0, len(roles))
	for _, role := range roles {
		if auth.NormalizeRoleName(role) != publicRole {
			out = append(out, role)
		}
	}
	return out
}

// graphPrivilegeRows lists graph privileges as SHOW PRIVILEGES rows (access,
// action, resource, graph, segment, role, immutable).
func graphPrivilegeRows(privileges []auth.GraphPrivilege, only map[string]bool) [][]interface{} {
	var rows [][]interface{}
	for _, privilege := range privileges {
		if len(only) > 0 && !only[privilege.Role] {
			continue
		}
		segment, err := parseGraphSegment(privilege.Segment)
		display := privilege.Segment
		if err == nil {
			display = segment.display()
		}
		access := "GRANTED"
		if privilege.Denied {
			access = "DENIED"
		}
		rows = append(rows, []interface{}{access, "match", "all_properties", privilege.Graph, display, privilege.Role, false})
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i][5].(string) < rows[j][5].(string) })
	return rows
}
