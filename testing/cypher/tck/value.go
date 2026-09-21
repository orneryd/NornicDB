package tck

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
)

// ParseValue parses the canonical value syntax used in TCK tables.
func ParseValue(input string) (any, error) {
	p := valueParser{input: strings.TrimSpace(input)}
	value, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	p.skipSpace()
	if !p.done() {
		return nil, fmt.Errorf("unexpected value suffix %q", p.input[p.pos:])
	}
	return value, nil
}

type valueParser struct {
	input string
	pos   int
}

func (p *valueParser) done() bool { return p.pos >= len(p.input) }

func (p *valueParser) skipSpace() {
	for !p.done() && unicode.IsSpace(rune(p.input[p.pos])) {
		p.pos++
	}
}

func (p *valueParser) parseValue() (any, error) {
	p.skipSpace()
	if p.done() {
		return nil, fmt.Errorf("empty value")
	}
	switch p.input[p.pos] {
	case '\'':
		return p.parseString()
	case '[':
		if strings.HasPrefix(p.input[p.pos:], "[:") {
			return p.parseRelationship()
		}
		return p.parseList()
	case '{':
		return p.parseMap()
	case '(':
		return p.parseNode()
	case '<':
		return p.parsePath()
	default:
		return p.parseAtom()
	}
}

func (p *valueParser) parseString() (string, error) {
	p.pos++
	var out strings.Builder
	for !p.done() {
		ch := p.input[p.pos]
		p.pos++
		if ch == '\'' {
			return out.String(), nil
		}
		if ch == '\\' && !p.done() {
			next := p.input[p.pos]
			p.pos++
			switch next {
			case 'n':
				out.WriteByte('\n')
			case 'r':
				out.WriteByte('\r')
			case 't':
				out.WriteByte('\t')
			default:
				out.WriteByte(next)
			}
			continue
		}
		out.WriteByte(ch)
	}
	return "", fmt.Errorf("unterminated string")
}

func (p *valueParser) parseAtom() (any, error) {
	start := p.pos
	for !p.done() && !strings.ContainsRune(",]}>) ", rune(p.input[p.pos])) {
		p.pos++
	}
	atom := p.input[start:p.pos]
	switch atom {
	case "null":
		return nil, nil
	case "true":
		return true, nil
	case "false":
		return false, nil
	case "NaN":
		return math.NaN(), nil
	case "Inf":
		return math.Inf(1), nil
	case "-Inf":
		return math.Inf(-1), nil
	}
	if strings.ContainsAny(atom, ".eE") {
		value, err := strconv.ParseFloat(atom, 64)
		if err == nil {
			return value, nil
		}
	}
	value, err := strconv.ParseInt(atom, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse atom %q: %w", atom, err)
	}
	return value, nil
}

func (p *valueParser) parseList() ([]any, error) {
	p.pos++
	values := []any{}
	for {
		p.skipSpace()
		if p.consume(']') {
			return values, nil
		}
		value, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
		p.skipSpace()
		if p.consume(']') {
			return values, nil
		}
		if !p.consume(',') {
			return nil, fmt.Errorf("expected comma in list at %q", p.input[p.pos:])
		}
	}
}

func (p *valueParser) parseMap() (map[string]any, error) {
	p.pos++
	values := map[string]any{}
	for {
		p.skipSpace()
		if p.consume('}') {
			return values, nil
		}
		key, err := p.parseKey()
		if err != nil {
			return nil, err
		}
		p.skipSpace()
		if !p.consume(':') {
			return nil, fmt.Errorf("expected colon after map key %q", key)
		}
		value, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		values[key] = value
		p.skipSpace()
		if p.consume('}') {
			return values, nil
		}
		if !p.consume(',') {
			return nil, fmt.Errorf("expected comma in map at %q", p.input[p.pos:])
		}
	}
}

func (p *valueParser) parseKey() (string, error) {
	p.skipSpace()
	if !p.done() && p.input[p.pos] == '\'' {
		return p.parseString()
	}
	start := p.pos
	for !p.done() && p.input[p.pos] != ':' && !unicode.IsSpace(rune(p.input[p.pos])) {
		p.pos++
	}
	if start == p.pos {
		return "", fmt.Errorf("empty map key")
	}
	return p.input[start:p.pos], nil
}

func (p *valueParser) parseNode() (NodeValue, error) {
	p.pos++
	node := NodeValue{Properties: map[string]any{}}
	for {
		p.skipSpace()
		if p.consume(')') {
			return node, nil
		}
		if p.consume(':') {
			label := p.parseIdentifier()
			if label == "" {
				return NodeValue{}, fmt.Errorf("empty node label")
			}
			node.Labels = append(node.Labels, label)
			continue
		}
		if !p.done() && p.input[p.pos] == '{' {
			properties, err := p.parseMap()
			if err != nil {
				return NodeValue{}, err
			}
			node.Properties = properties
			continue
		}
		return NodeValue{}, fmt.Errorf("unexpected node content at %q", p.input[p.pos:])
	}
}

func (p *valueParser) parseRelationship() (RelationshipValue, error) {
	p.pos += 2
	relationship := RelationshipValue{Properties: map[string]any{}}
	relationship.Type = p.parseIdentifier()
	if relationship.Type == "" {
		return RelationshipValue{}, fmt.Errorf("empty relationship type")
	}
	p.skipSpace()
	if !p.done() && p.input[p.pos] == '{' {
		properties, err := p.parseMap()
		if err != nil {
			return RelationshipValue{}, err
		}
		relationship.Properties = properties
	}
	p.skipSpace()
	if !p.consume(']') {
		return RelationshipValue{}, fmt.Errorf("unterminated relationship")
	}
	return relationship, nil
}

func (p *valueParser) parsePath() (PathValue, error) {
	p.pos++
	path := PathValue{}
	first, err := p.parseNode()
	if err != nil {
		return PathValue{}, err
	}
	path.Nodes = append(path.Nodes, first)
	for {
		p.skipSpace()
		if p.consume('>') {
			return path, nil
		}
		forward := true
		if strings.HasPrefix(p.input[p.pos:], "<-") {
			forward = false
			p.pos += 2
		} else if p.consume('-') {
			forward = true
		} else {
			return PathValue{}, fmt.Errorf("expected path direction at %q", p.input[p.pos:])
		}
		relationship, err := p.parseRelationship()
		if err != nil {
			return PathValue{}, err
		}
		if forward {
			if !strings.HasPrefix(p.input[p.pos:], "->") {
				return PathValue{}, fmt.Errorf("expected forward path arrow")
			}
			p.pos += 2
		} else {
			if !p.consume('-') {
				return PathValue{}, fmt.Errorf("expected reverse path tail")
			}
		}
		node, err := p.parseNode()
		if err != nil {
			return PathValue{}, err
		}
		path.Segments = append(path.Segments, PathSegment{Relationship: relationship, Forward: forward})
		path.Nodes = append(path.Nodes, node)
	}
}

func (p *valueParser) parseIdentifier() string {
	p.skipSpace()
	start := p.pos
	for !p.done() {
		ch := rune(p.input[p.pos])
		if !(unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_') {
			break
		}
		p.pos++
	}
	return p.input[start:p.pos]
}

func (p *valueParser) consume(want byte) bool {
	if !p.done() && p.input[p.pos] == want {
		p.pos++
		return true
	}
	return false
}
