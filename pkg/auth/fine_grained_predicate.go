package auth

import (
	"math/big"
	"reflect"
	"slices"
	"strings"

	"github.com/orneryd/nornicdb/pkg/convert"
)

// FineGrainedPredicate compares one stored property to literal values.
// For example, use Property "status", Operator "=", and Value "published".
// A read action can have a predicate even when it does not expose the property
// being tested to the caller.
type FineGrainedPredicate struct {
	Property string `json:"property"`
	Operator string `json:"operator"`
	Value    any    `json:"value,omitempty"`
}

func (p *FineGrainedPredicate) clone() *FineGrainedPredicate {
	if p == nil {
		return nil
	}
	cloned := *p
	if values, ok := p.Value.([]any); ok {
		cloned.Value = slices.Clone(values)
	}
	return &cloned
}

func (p *FineGrainedPredicate) validFor(action FineGrainedAction) bool {
	if p == nil {
		return true
	}
	switch action.ID {
	case "graph.node.traverse", "graph.node.property.read",
		"graph.relationship.traverse", "graph.relationship.property.read":
	default:
		return false
	}
	if p.Property == "" {
		return false
	}
	switch p.Operator {
	case "IS NULL", "IS NOT NULL", "NATIVE TAG":
		return p.Value == nil
	case "IN", "NOT IN":
		values, ok := p.Value.([]any)
		if !ok {
			return false
		}
		for _, value := range values {
			if !fineGrainedLiteral(value) {
				return false
			}
		}
		return true
	case "=", "<>", "<", "<=", ">", ">=", "CONTAINS VALUE", "LACKS VALUE":
		return fineGrainedLiteral(p.Value)
	default:
		return false
	}
}

func fineGrainedLiteral(value any) bool {
	switch value.(type) {
	case bool, string:
		return true
	default:
		number, ok := convert.ReadNumber(value)
		return ok && fineGrainedRational(number) != nil
	}
}

func fineGrainedRational(number convert.Number) *big.Rat {
	if value, ok := number.Signed(); ok {
		return new(big.Rat).SetInt64(value)
	}
	if value, ok := number.Unsigned(); ok {
		return new(big.Rat).SetUint64(value)
	}
	return new(big.Rat).SetFloat64(number.Float64())
}

func (p *FineGrainedPredicate) matches(properties map[string]any, nativeABAC bool, tags []string) bool {
	if p == nil {
		return true
	}
	subject := properties[p.Property]
	switch p.Operator {
	case "IS NULL":
		return subject == nil
	case "IS NOT NULL":
		return subject != nil
	}
	if subject == nil {
		return false
	}
	switch p.Operator {
	case "NATIVE TAG":
		if !nativeABAC {
			return false
		}
		tag, isString := subject.(string)
		return isString && slices.Contains(tags, tag)
	case "IN", "NOT IN":
		for _, value := range p.Value.([]any) {
			if comparison, ordered := compareFineGrainedValues(subject, value); ordered && comparison == 0 {
				return p.Operator == "IN"
			}
		}
		return p.Operator == "NOT IN"
	case "CONTAINS VALUE", "LACKS VALUE":
		v := reflect.ValueOf(subject)
		if v.Kind() != reflect.Array && v.Kind() != reflect.Slice {
			return false
		}
		for i := 0; i < v.Len(); i++ {
			if comparison, ordered := compareFineGrainedValues(v.Index(i).Interface(), p.Value); ordered && comparison == 0 {
				return p.Operator == "CONTAINS VALUE"
			}
		}
		return p.Operator == "LACKS VALUE"
	default:
		return fineGrainedComparisonMatches(p.Operator, subject, p.Value)
	}
}

func fineGrainedComparisonMatches(operator string, left, right any) bool {
	comparison, ordered := compareFineGrainedValues(left, right)
	switch operator {
	case "=":
		return ordered && comparison == 0
	case "<>":
		return !ordered || comparison != 0
	case "<":
		return ordered && comparison < 0
	case "<=":
		return ordered && comparison <= 0
	case ">":
		return ordered && comparison > 0
	case ">=":
		return ordered && comparison >= 0
	}
	return false
}

func compareFineGrainedValues(left, right any) (int, bool) {
	switch left := left.(type) {
	case string:
		right, ok := right.(string)
		return strings.Compare(left, right), ok
	case bool:
		right, ok := right.(bool)
		switch {
		case left == right:
			return 0, ok
		case left:
			return 1, ok
		default:
			return -1, ok
		}
	}
	first, firstOK := convert.ReadNumber(left)
	second, secondOK := convert.ReadNumber(right)
	if !firstOK || !secondOK {
		return 0, false
	}
	a, b := fineGrainedRational(first), fineGrainedRational(second)
	if a == nil || b == nil {
		return 0, false
	}
	return a.Cmp(b), true
}
