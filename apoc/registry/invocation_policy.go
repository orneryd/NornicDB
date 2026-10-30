package registry

// Author: Codex NOR-184 lane. Argument policy data for the sole callable invoker.

import (
	"reflect"

	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
)

// ArgumentPolicy preserves a consumer's accepted arguments without exposing a
// second invoker. All policies bind into Callable.InvokeValues.
type ArgumentPolicy uint8

const (
	GoArguments ArgumentPolicy = iota
	APOCArguments
	CypherArguments
)

type argumentBinding uint8

const (
	declaredArgument argumentBinding = iota
	numericArgument
	textNumericArgument
	numericListArgument
	textNumericListArgument
	listArgument
	stringListArgument
	stringArgument
)

// These are signature metadata, never invocable functions. Root APOC's
// supported surface stays closed; Cypher preserves its text-to-number rules.
var apocArgumentPlans = map[reflect.Type][]argumentBinding{
	reflect.TypeOf((func([]interface{}) float64)(nil)):           {listArgument},
	reflect.TypeOf((func([]interface{}) interface{})(nil)):       {listArgument},
	reflect.TypeOf((func([]interface{}, interface{}) bool)(nil)): {listArgument, declaredArgument},
	reflect.TypeOf((func([]string, string) string)(nil)):         {stringListArgument, stringArgument},
	reflect.TypeOf((func(string, string) []string)(nil)):         {stringArgument, stringArgument},
	reflect.TypeOf((func(string) string)(nil)):                   {stringArgument},
	reflect.TypeOf((func(string, string) int)(nil)):              {stringArgument, stringArgument},
	reflect.TypeOf((func(string, string) float64)(nil)):          {stringArgument, stringArgument},
	reflect.TypeOf((func(float64) float64)(nil)):                 {numericArgument},
	reflect.TypeOf((func([]float64) []float64)(nil)):             {numericListArgument},
	reflect.TypeOf((func([]float64, []float64) float64)(nil)):    {numericListArgument, numericListArgument},
	reflect.TypeOf((func(int64, int64) (int64, error))(nil)):     {declaredArgument, declaredArgument},
}

var cypherArgumentPlans = map[reflect.Type][]argumentBinding{
	reflect.TypeOf((func(float64) float64)(nil)):              {textNumericArgument},
	reflect.TypeOf((func([]float64) []float64)(nil)):          {textNumericListArgument},
	reflect.TypeOf((func([]float64, []float64) float64)(nil)): {textNumericListArgument, textNumericListArgument},
}

func (h *Callable) argumentPlan(policy ArgumentPolicy) ([]argumentBinding, bool) {
	switch policy {
	case GoArguments:
		return nil, true
	case APOCArguments:
		plan, ok := apocArgumentPlans[h.signature]
		return plan, ok
	case CypherArguments:
		return cypherArgumentPlans[h.signature], true
	default:
		return nil, false
	}
}

func bindPolicyArgument(value interface{}, target reflect.Type, binding argumentBinding) (reflect.Value, bool) {
	switch binding {
	case numericArgument, textNumericArgument:
		var number float64
		var ok bool
		if binding == numericArgument {
			number, ok = valueconvert.NumericFloat64(value)
		} else {
			number, ok = valueconvert.ToFloat64(value)
		}
		return reflect.ValueOf(number), ok
	case numericListArgument, textNumericListArgument:
		if value == nil && binding == textNumericListArgument {
			return reflect.Zero(target), true
		}
		var numbers []float64
		var ok bool
		if binding == numericListArgument {
			numbers, ok = valueconvert.NumericFloat64Slice(value)
		} else {
			numbers, ok = valueconvert.ToFloat64Slice(value)
		}
		return reflect.ValueOf(numbers), ok
	case listArgument:
		list, ok := valueconvert.ListValue(value)
		return reflect.ValueOf(list), ok
	case stringListArgument:
		view, ok := valueconvert.ReadList(value)
		list := valueconvert.ToStringSlice(value)
		return reflect.ValueOf(list), ok && (list != nil || view.Len() == 0)
	case stringArgument:
		text, ok := value.(string)
		return reflect.ValueOf(text), ok
	default:
		return BindArgument(reflect.ValueOf(value), target)
	}
}
