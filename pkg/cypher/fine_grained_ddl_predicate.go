package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/auth"
)

func parseFineGrainedPredicate(ctx context.Context, source string) ([]string, *auth.FineGrainedPredicate, error) {
	cursor := fineGrainedPrivilegeCursor{text: source}
	if !cursor.takeKeyword("FOR") || cursor.at >= len(source) || source[cursor.at] != '(' {
		return nil, nil, fineGrainedPrivilegeSyntaxError("expected a FOR node pattern")
	}
	body, end, _, err := fineGrainedPatternTarget(source, cursor.at)
	if err != nil {
		return nil, nil, err
	}
	pattern := fineGrainedPrivilegeCursor{text: body}
	pattern.skipGap()
	variable, _ := pattern.readName()
	pattern.skipGap()
	var labels []string
	if pattern.at < len(pattern.text) && pattern.text[pattern.at] == ':' {
		pattern.at++
		pattern.skipGap()
		for {
			label, found := pattern.readName()
			if !found {
				return nil, nil, fineGrainedPrivilegeSyntaxError("expected a label or relationship type in the FOR pattern")
			}
			labels = append(labels, label)
			pattern.skipGap()
			if pattern.at >= len(pattern.text) || pattern.text[pattern.at] != '|' {
				break
			}
			pattern.at++
			pattern.skipGap()
		}
	}
	cursor.at = end
	cursor.skipGap()
	if expression := strings.TrimSpace(pattern.text[pattern.at:]); expression != "" {
		if cursor.at != len(source) || expression[0] != '{' || expression[len(expression)-1] != '}' {
			return nil, nil, fineGrainedPrivilegeSyntaxError("expected one literal property rule in the FOR pattern")
		}
		body := expression[1 : len(expression)-1]
		key := fineGrainedPrivilegeCursor{text: body}
		key.skipGap()
		property, found := key.readName()
		key.skipGap()
		if !found || key.at >= len(body) || body[key.at] != ':' {
			return nil, nil, fineGrainedPrivilegeSyntaxError("expected a property in the FOR pattern")
		}
		value, found := parseLiteralValue(body[key.at+1:])
		if !found || value == nil {
			return nil, nil, fineGrainedPrivilegeSyntaxError("property rules require a scalar literal")
		}
		return labels, &auth.FineGrainedPredicate{Property: property, Operator: "=", Value: value}, nil
	}
	if variable == "" || !cursor.takeKeyword("WHERE") {
		return nil, nil, fineGrainedPrivilegeSyntaxError("FOR requires a single property rule")
	}
	predicate, ok := bindFineGrainedPredicate(ctx, variable, source[cursor.at:])
	if !ok {
		return nil, nil, fineGrainedPrivilegeSyntaxError("expected one supported property comparison with literal operands")
	}
	return labels, predicate, nil
}

func bindFineGrainedPredicate(ctx context.Context, variable, expression string) (*auth.FineGrainedPredicate, bool) {
	expression = strings.TrimSpace(expression)
	cypher25 := cypherVersionFromContext(ctx) == "25"
	negated := false
	if cypher25 && startsWithKeywordFold(expression, "NOT") {
		negated = true
		expression = strings.TrimSpace(expression[len("NOT"):])
	}
	for _, operator := range []string{"IS NOT NULL", "IS NULL"} {
		if at := findMultiWordKeywordIndex(ctx, expression, "IS", operator[len("IS "):]); at >= 0 &&
			strings.EqualFold(strings.TrimSpace(expression[at:]), operator) {
			property, ok := fineGrainedRuleProperty(ctx, variable, expression[:at])
			if negated {
				if operator == "IS NULL" {
					operator = "IS NOT NULL"
				} else {
					operator = "IS NULL"
				}
			}
			return &auth.FineGrainedPredicate{Property: property, Operator: operator}, ok
		}
	}
	if at := findKeywordIndex(ctx, expression, "IN"); at >= 0 {
		left, right := expression[:at], expression[at+len("IN"):]
		if property, ok := fineGrainedRuleProperty(ctx, variable, left); ok {
			if cypher25 && strings.EqualFold(strings.TrimSpace(right), "abac.native.user_tags()") {
				return &auth.FineGrainedPredicate{Property: property, Operator: "NATIVE TAG"}, !negated
			}
			operator := "IN"
			if negated {
				operator = "NOT IN"
			}
			values, found := parseLiteralListForPipeline(right)
			return &auth.FineGrainedPredicate{Property: property, Operator: operator, Value: values}, found
		}
		if !cypher25 {
			return nil, false
		}
		property, isProperty := fineGrainedRuleProperty(ctx, variable, right)
		value, literal := parseLiteralValue(left)
		operator := "CONTAINS VALUE"
		if negated {
			operator = "LACKS VALUE"
		}
		return &auth.FineGrainedPredicate{Property: property, Operator: operator, Value: value}, isProperty && literal && value != nil
	}
	for _, operator := range []string{"<>", "<=", ">=", "=", "<", ">"} {
		at := findTopLevelOperator(expression, operator, false, true)
		if at < 0 {
			continue
		}
		left, right := expression[:at], expression[at+len(operator):]
		if negated {
			operator = map[string]string{"=": "<>", "<>": "=", "<": ">=", ">": "<=", "<=": ">", ">=": "<"}[operator]
		}
		if property, ok := fineGrainedRuleProperty(ctx, variable, left); ok {
			value, found := parseLiteralValue(right)
			return &auth.FineGrainedPredicate{Property: property, Operator: operator, Value: value}, found && value != nil
		}
		if !cypher25 {
			return nil, false
		}
		property, isProperty := fineGrainedRuleProperty(ctx, variable, right)
		value, literal := parseLiteralValue(left)
		if operator != "=" && operator != "<>" {
			operator = mirroredComparison(operator)
		}
		return &auth.FineGrainedPredicate{Property: property, Operator: operator, Value: value}, isProperty && literal && value != nil
	}
	return nil, false
}

func fineGrainedRuleProperty(ctx context.Context, variable, text string) (string, bool) {
	cursor := fineGrainedPrivilegeCursor{text: text}
	cursor.skipGap()
	name, found := cursor.readName()
	cursor.skipGap()
	if !found || name != variable || cursor.at >= len(text) || text[cursor.at] != '.' {
		return "", false
	}
	at, _ := readQueryGap(text, cursor.at+1)
	property, found := cyphertext.ScanIdentifier(text, at)
	if !found {
		return "", false
	}
	cursor.at = property.End
	cursor.skipGap()
	return property.Value, cursor.at == len(text)
}

// fineGrainedPatternTarget reads a single node predicate pattern or the
// anonymous-endpoint relationship form. It returns only the property
// carrier's pattern body; endpoint restrictions aren't legal in this syntax.
func fineGrainedPatternTarget(text string, at int) (body string, end int, kind string, err error) {
	if at >= len(text) || text[at] != '(' {
		return "", 0, "", fineGrainedPrivilegeSyntaxError("expected a parenthesized FOR pattern")
	}
	first, ok := fineGrainedPrivilegePatternEnd(text, at)
	if !ok {
		return "", 0, "", fineGrainedPrivilegeSyntaxError("unclosed FOR pattern")
	}
	node := text[at+1 : first-1]
	start, _ := readQueryGap(text, first)
	if start >= len(text) || text[start] != '-' {
		return node, first, fineGrainedNodeKind, nil
	}
	if strings.TrimSpace(node) != "" {
		return "", 0, "", fineGrainedPrivilegeSyntaxError("relationship privilege endpoints must be anonymous")
	}
	start, _ = readQueryGap(text, start+1)
	if start >= len(text) || text[start] != '[' {
		return "", 0, "", fineGrainedPrivilegeSyntaxError("expected a relationship pattern")
	}
	last := findMatchingBracket(text, start)
	if last <= start {
		return "", 0, "", fineGrainedPrivilegeSyntaxError("unclosed relationship pattern")
	}
	next, _ := readQueryGap(text, last+1)
	if next >= len(text) || text[next] != '-' {
		return "", 0, "", fineGrainedPrivilegeSyntaxError("expected a relationship endpoint")
	}
	next, _ = readQueryGap(text, next+1)
	if next >= len(text) || text[next] != '(' {
		return "", 0, "", fineGrainedPrivilegeSyntaxError("expected an anonymous relationship endpoint")
	}
	end, ok = fineGrainedPrivilegePatternEnd(text, next)
	if !ok || strings.TrimSpace(text[next+1:end-1]) != "" {
		return "", 0, "", fineGrainedPrivilegeSyntaxError("relationship privilege endpoints must be anonymous")
	}
	return text[start+1 : last], end, fineGrainedRelationshipKind, nil
}

func renderFineGrainedPredicate(predicate *auth.FineGrainedPredicate, kind auth.FineGrainedResourceKind, label string) string {
	binding := "n"
	if label != "" {
		binding += ":" + fineGrainedDisplayName(label)
	}
	pattern := "(" + binding + ")"
	if kind == auth.FineGrainedRelationship {
		pattern = "()-[" + binding + "]-()"
	}
	property := "n." + fineGrainedDisplayName(predicate.Property)
	condition := property + " " + predicate.Operator
	if predicate.Operator == "IS NULL" || predicate.Operator == "IS NOT NULL" {
		return "FOR " + pattern + " WHERE " + condition
	}
	value, _ := cypher25ValueText(predicate.Value)
	switch predicate.Operator {
	case "NATIVE TAG":
		condition = property + " IN abac.native.user_tags()"
	case "NOT IN":
		condition = "NOT " + property + " IN " + value
	case "CONTAINS VALUE":
		condition = value + " IN " + property
	case "LACKS VALUE":
		condition = "NOT " + value + " IN " + property
	default:
		condition += " " + value
	}
	return "FOR " + pattern + " WHERE " + condition
}
