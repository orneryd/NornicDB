package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/pkg/math/angle"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	cyphertext "github.com/orneryd/nornicdb/pkg/cypher/internal/text"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/text/unicode/norm"
)

// Neo4j 5 functions (#698). Each is registered once in the function
// registry, which both expression evaluators dispatch to, and listed in
// cypherFunctionCatalog. Results and errors follow Neo4j 5.26: null in,
// null out; an argument of a type the signature excludes is the "Type
// mismatch: expected … but was …" SyntaxError (cypherfn.TypeMismatchError).
func init() {
	cypherfn.Register("radians", fnRadians)
	cypherfn.Register("pi", fnMathConstant("pi", math.Pi))
	cypherfn.Register("e", fnMathConstant("e", math.E))
	cypherfn.Register("round", fnRound)
	for name, operation := range map[string]func(float64) float64{
		"sin":      math.Sin,
		"cos":      math.Cos,
		"tan":      math.Tan,
		"cot":      rowMathCot,
		"asin":     math.Asin,
		"acos":     math.Acos,
		"atan":     math.Atan,
		"exp":      math.Exp,
		"log":      math.Log,
		"log10":    math.Log10,
		"sqrt":     math.Sqrt,
		"degrees":  rowMathDegrees,
		"haversin": rowMathHaversin,
		"sinh":     math.Sinh,
		"cosh":     math.Cosh,
		"tanh":     math.Tanh,
		"coth":     rowMathCoth,
	} {
		cypherfn.Register(name, fnMathUnary(name, operation))
	}
	cypherfn.Register("ceil", fnMathUnary("ceil", math.Ceil))
	cypherfn.Register("floor", fnMathUnary("floor", math.Floor))
	cypherfn.Register("atan2", fnMathBinary("atan2", math.Atan2))
	cypherfn.Register("power", fnMathBinary("power", math.Pow))
	cypherfn.Register("isnan", fnIsNaN)
	cypherfn.Register("char_length", fnCharLength)
	cypherfn.Register("character_length", fnCharLength)
	cypherfn.Register("upper", fnStringCase(func(text string) string { return cases.Upper(language.Und).String(text) }, "upper"))
	cypherfn.Register("lower", fnStringCase(func(text string) string { return cases.Lower(language.Und).String(text) }, "lower"))
	cypherfn.Register("btrim", fnTrimFunction("btrim", true, true))
	cypherfn.Register("ltrim", fnTrimFunction("ltrim", true, false))
	cypherfn.Register("rtrim", fnTrimFunction("rtrim", false, true))
	cypherfn.Register("trim", fnTrim)
	cypherfn.Register("normalize", fnNormalize)
	cypherfn.Register("tointegerlist", fnListConversion("toIntegerList", sameInEveryVersion(convertToIntegerOrNull)))
	cypherfn.Register("tofloatlist", fnListConversion("toFloatList", sameInEveryVersion(convertToFloatOrNull)))
	cypherfn.Register("tostringlist", fnListConversion("toStringList", convertToStringInVersion))
	cypherfn.Register("tobooleanlist", fnListConversion("toBooleanList", sameInEveryVersion(convertToBooleanOrNull)))
	cypherfn.Register("valuetype", fnValueType)
	cypherfn.Register("nullif", fnNullIf)
	cypherfn.Register("tail", fnTail)
	for name, fn := range singleValueFunctions {
		cypherfn.Register(name, singleValueFunction(name, fn))
	}
	for _, name := range []string{"substring", "left", "right", "replace", "split"} {
		cypherfn.Register(name, fnStringOperation(name))
	}
	for _, name := range []string{"tointeger", "toint", "tofloat", "toboolean", "tostring"} {
		convert := map[string]versionedConversion{
			"tointeger": sameInEveryVersion(convertToIntegerOrNull), "toint": sameInEveryVersion(convertToIntegerOrNull),
			"tofloat": sameInEveryVersion(convertToFloatOrNull), "toboolean": sameInEveryVersion(convertToBooleanOrNull),
			"tostring": convertToStringInVersion,
		}[name]
		cypherfn.Register(name, fnScalarConversion(name, convert, false))
		if name != "toint" {
			cypherfn.Register(name+"ornull", fnScalarConversion(name, convert, true))
		}
	}
}

func fnMathConstant(name string, value float64) cypherfn.Func {
	return func(_ cypherfn.Context, args []string) (interface{}, error) {
		if len(args) != 0 {
			return nil, argumentCountError(name, "0", len(args))
		}
		return value, nil
	}
}

func fnMathUnary(name string, operation func(float64) float64) cypherfn.Func {
	return func(ctx cypherfn.Context, args []string) (interface{}, error) {
		if len(args) != 1 {
			return nil, argumentCountError(name, "1", len(args))
		}
		values, err := evalArgs(ctx, args)
		if err != nil || values[0] == nil {
			return nil, err
		}
		number, ok := toFloat64(values[0])
		if !ok {
			return nil, &cypherfn.TypeMismatchError{Function: name, Expected: "Float or Integer", Value: values[0]}
		}
		return operation(number), nil
	}
}

func fnMathBinary(name string, operation func(float64, float64) float64) cypherfn.Func {
	return func(ctx cypherfn.Context, args []string) (interface{}, error) {
		if len(args) != 2 {
			return nil, argumentCountError(name, "2", len(args))
		}
		values, err := evalArgs(ctx, args)
		if err != nil || values[0] == nil || values[1] == nil {
			return nil, err
		}
		left, leftOK := toFloat64(values[0])
		right, rightOK := toFloat64(values[1])
		if !leftOK {
			return nil, &cypherfn.TypeMismatchError{Function: name, Expected: "Float or Integer", Value: values[0]}
		}
		if !rightOK {
			return nil, &cypherfn.TypeMismatchError{Function: name, Expected: "Float or Integer", Value: values[1]}
		}
		return operation(left, right), nil
	}
}

func fnRound(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) < 1 || len(args) > 3 {
		return nil, argumentCountError("round", "1 to 3", len(args))
	}
	values, err := evalArgs(ctx, args)
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		if value == nil {
			return nil, nil
		}
	}
	number, ok := toFloat64(values[0])
	if !ok {
		return nil, &cypherfn.TypeMismatchError{Function: "round", Expected: "Float or Integer", Value: values[0]}
	}
	precision := 0
	if len(values) > 1 {
		precision, ok = toInt(values[1])
		if !ok {
			return nil, &cypherfn.TypeMismatchError{Function: "round", Expected: "Integer", Value: values[1]}
		}
		if precision < 0 {
			return nil, newSemanticError("Neo.ClientError.Statement.ArgumentError", "InvalidArgument", "Precision argument to 'round()' cannot be negative")
		}
	}
	var mode string
	if len(values) == 3 {
		mode, ok = values[2].(string)
		if !ok {
			return nil, &cypherfn.TypeMismatchError{Function: "round", Expected: "String", Value: values[2]}
		}
	}
	factor := math.Pow10(precision)
	var rounded float64
	if len(values) == 3 {
		var validMode bool
		rounded, validMode = roundRowNumber(number*factor, mode)
		if !validMode {
			return nil, newSemanticError("Neo.ClientError.Statement.ArgumentError", "InvalidArgument", "Unknown rounding mode. Valid values are: CEILING, FLOOR, UP, DOWN, HALF_EVEN, HALF_UP, HALF_DOWN, UNNECESSARY.")
		}
	} else {
		rounded = math.Floor(number*factor + 0.5)
	}
	return rounded / factor, nil
}

// singleValueFunctions are the one-argument functions computed from their
// argument's value alone (abs, sign, isEmpty). The registry and the row
// evaluator both call them, so each has one implementation; the row
// evaluator passes the value it already has instead of an evaluation
// callback.
var singleValueFunctions = map[string]func(interface{}) (interface{}, error){
	"abs":     absValue,
	"sign":    signValue,
	"isempty": isEmptyValue,
}

// singleValueFunction registers fn as a one-argument registry function
// that evaluates its argument and calls fn with the value.
func singleValueFunction(function string, fn func(interface{}) (interface{}, error)) cypherfn.Func {
	return func(ctx cypherfn.Context, args []string) (interface{}, error) {
		if len(args) != 1 {
			return nil, argumentCountError(function, "1", len(args))
		}
		value, err := ctx.Eval(args[0])
		if err != nil {
			return nil, err
		}
		return fn(value)
	}
}

// numberValue is a numeric function's argument: null is (nil, false, nil),
// and any other non-number Neo4j's TypeError.
func numberValue(value interface{}, function string) (interface{}, bool, error) {
	if value == nil {
		return nil, false, nil
	}
	if !isRuntimeNumber(value) {
		return nil, false, &cypherfn.TypeMismatchError{Function: function, Expected: "Float or Integer", Value: value}
	}
	return value, true, nil
}

// absValue is abs(number): an integer's or float's absolute value, of its
// type.
func absValue(value interface{}) (interface{}, error) {
	value, ok, err := numberValue(value, "abs")
	if !ok {
		return nil, err
	}
	if integer, isInteger := cypherIntegerValue(value); isInteger {
		if integer < 0 {
			return -integer, nil
		}
		return integer, nil
	}
	number, _ := cypherFloatValue(value)
	return math.Abs(number), nil
}

// signValue is sign(number): -1, 0 or 1, an integer.
func signValue(value interface{}) (interface{}, error) {
	value, ok, err := numberValue(value, "sign")
	if !ok {
		return nil, err
	}
	number, _ := toFloat64(value)
	switch {
	case number < 0:
		return int64(-1), nil
	case number > 0:
		return int64(1), nil
	}
	return int64(0), nil
}

// isEmptyValue is isEmpty(list, map or string): null for null, and Neo4j's
// TypeError for any other value (a number, a node).
func isEmptyValue(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if text, isString := value.(string); isString {
		return len(text) == 0, nil
	}
	if entries, isMap := value.(map[string]interface{}); isMap && cypherValueKindOf(value) == valueKindMap {
		return len(entries) == 0, nil
	}
	if items, isList := cypherListValue(value); isList {
		return len(items) == 0, nil
	}
	return nil, &cypherfn.TypeMismatchError{Function: "isEmpty", Expected: "List, Map, or String", Value: value}
}

func fnTail(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, argumentCountError("tail", "1", len(args))
	}
	value, err := ctx.Eval(args[0])
	if err != nil || value == nil {
		return nil, err
	}
	// A value that isn't a list has no tail: Neo4j gives [] (tail(n.age)); a
	// node or other non-list literal is the static check's SyntaxError.
	items, _ := cypherListValue(value)
	if len(items) < 2 {
		return []interface{}{}, nil
	}
	return append([]interface{}{}, items[1:]...), nil
}

func fnStringOperation(name string) cypherfn.Func {
	return func(ctx cypherfn.Context, args []string) (interface{}, error) {
		minimum, maximum := 2, 2
		if name == "substring" {
			maximum = 3
		} else if name == "replace" {
			// replace(text, search, replacement[, limit]) (Neo4j 2025.06).
			minimum, maximum = 3, 4
		}
		if len(args) < minimum || len(args) > maximum {
			return nil, argumentCountError(name, strconv.Itoa(minimum), len(args))
		}
		values, err := evalArgs(ctx, args)
		if err != nil {
			return nil, err
		}
		for _, value := range values {
			if value == nil {
				return nil, nil
			}
		}
		text, _, err := stringArgument(name, values, 0)
		if err != nil {
			return nil, err
		}
		if name == "split" {
			if delimiters, isList := values[1].([]interface{}); isList {
				return splitAtAnyDelimiter(name, text, delimiters)
			}
		}
		if name == "replace" || name == "split" {
			separator, _, err := stringArgument(name, values, 1)
			if err != nil {
				return nil, err
			}
			if name == "replace" {
				replacement, _, err := stringArgument(name, values, 2)
				if err != nil {
					return nil, err
				}
				limit := -1
				if len(values) == 4 {
					if limit, err = replaceLimit(args[3], values[3]); err != nil {
						return nil, err
					}
				}
				return strings.Replace(text, separator, replacement, limit), nil
			}
			parts := strings.Split(text, separator)
			result := make([]interface{}, len(parts))
			for index, part := range parts {
				result[index] = part
			}
			return result, nil
		}
		position, ok := toInt(values[1])
		if !ok {
			return nil, &cypherfn.TypeMismatchError{Function: name, Expected: "Integer", Value: values[1]}
		}
		if position < 0 {
			return nil, stringOperationOutOfRange(ctx.Cypher25, name)
		}
		switch name {
		case "left":
			return cyphertext.Left(text, position), nil
		case "right":
			return cyphertext.Right(text, position), nil
		}
		if len(values) == 2 {
			return cyphertext.From(text, position), nil
		}
		length, ok := toInt(values[2])
		if !ok {
			return nil, &cypherfn.TypeMismatchError{Function: name, Expected: "Integer", Value: values[2]}
		}
		if length < 0 {
			return nil, stringOperationOutOfRange(ctx.Cypher25, name)
		}
		return cyphertext.Substring(text, position, length), nil
	}
}

// versionedConversion is a conversion function's value of value in a
// statement of the version (cypher25): toString differs in Cypher 25
// (convertToStringInVersion); the others are the same in every version.
type versionedConversion func(value interface{}, cypher25 bool) interface{}

// sameInEveryVersion is a conversion that doesn't depend on the version.
func sameInEveryVersion(convert func(interface{}) interface{}) versionedConversion {
	return func(value interface{}, _ bool) interface{} { return convert(value) }
}

func fnScalarConversion(name string, convert versionedConversion, orNull bool) cypherfn.Func {
	return func(ctx cypherfn.Context, args []string) (interface{}, error) {
		if len(args) != 1 {
			return nil, argumentCountError(name, "1", len(args))
		}
		value, err := ctx.Eval(args[0])
		if err != nil || value == nil {
			return nil, err
		}
		if !orNull && !validConversionArgument(name, value, ctx.Cypher25) {
			return nil, newSemanticError("Neo.ClientError.Statement.TypeError", "InvalidArgumentValue",
				fmt.Sprintf("Invalid input for function '%s()': Expected %s, got: %s", conversionFunctionNames[name], conversionFunctionInputs[name], neo4jValueRepr(value)))
		}
		return convert(value, ctx.Cypher25), nil
	}
}

// evalArgs evaluates a function's argument expressions.
func evalArgs(ctx cypherfn.Context, args []string) ([]interface{}, error) {
	values := make([]interface{}, len(args))
	for i, arg := range args {
		value, err := ctx.Eval(strings.TrimSpace(arg))
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return values, nil
}

// replaceLimit is replace()'s limit, the most occurrences it replaces. A
// negative literal is Neo4j's compile-time SyntaxError; a negative value known
// only at run time is out of range (ArgumentError).
func replaceLimit(argument string, value interface{}) (int, error) {
	limit, ok := cypherIntegerValue(value)
	if !ok {
		return 0, &cypherfn.TypeMismatchError{Function: "replace", Expected: "Integer", Value: value}
	}
	if limit >= 0 {
		return int(limit), nil
	}
	if _, err := strconv.ParseInt(strings.TrimSpace(argument), 10, 64); err == nil {
		return 0, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidArgument",
			localization.CypherCoreReplaceLimitNegative())
	}
	return 0, functionArgumentOutOfRange("replace")
}

// argumentCountError is the error of a function called with the wrong number
// of arguments. It carries no status code of its own, so clients get
// Statement.SyntaxError (errors.Neo4jStatus's default), as before it was
// localized.
func argumentCountError(function string, want string, got int) error {
	return localizedError(localization.CypherCoreFunctionArgumentCount(function, want, got), nil)
}

func fnRadians(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, argumentCountError("radians", "1", len(args))
	}
	values, err := evalArgs(ctx, args)
	if err != nil || values[0] == nil {
		return nil, err
	}
	number, ok := toFloat64(values[0])
	if !ok {
		return nil, &cypherfn.TypeMismatchError{Function: "radians", Expected: "Float", Value: values[0]}
	}
	return angle.ToRadians(number), nil
}

func fnIsNaN(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, argumentCountError("isNaN", "1", len(args))
	}
	values, err := evalArgs(ctx, args)
	if err != nil || values[0] == nil {
		return nil, err
	}
	switch value := values[0].(type) {
	case float64:
		return math.IsNaN(value), nil
	case float32:
		return math.IsNaN(float64(value)), nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return false, nil
	}
	return nil, &cypherfn.TypeMismatchError{Function: "isNaN", Expected: "Float or Integer", Value: values[0]}
}

// stringArgument is values[index] as a string; ok is false for null.
// splitAtAnyDelimiter is split(text, delimiters) with a list of delimiters,
// as Neo4j does it: the text is cut wherever one of them starts, the first in
// the list winning at a position; an empty delimiter cuts between
// characters, an empty list leaves the text whole, a null delimiter gives
// null and any other non-string one is a type error.
func splitAtAnyDelimiter(function, text string, delimiters []interface{}) (interface{}, error) {
	separators := make([]string, 0, len(delimiters))
	for _, delimiter := range delimiters {
		if delimiter == nil {
			return nil, nil
		}
		separator, isString := delimiter.(string)
		if !isString {
			return nil, &cypherfn.TypeMismatchError{Function: function, Expected: "String", Value: delimiter}
		}
		separators = append(separators, separator)
	}
	parts := []interface{}{}
	start := 0
	for position := 0; position < len(text); {
		cut := -1
		for _, separator := range separators {
			if separator == "" {
				if position > start {
					cut = 0
					break
				}
				continue
			}
			if strings.HasPrefix(text[position:], separator) {
				cut = len(separator)
				break
			}
		}
		if cut > 0 {
			parts = append(parts, text[start:position])
			position += cut
			start = position
			continue
		}
		if cut == 0 {
			parts = append(parts, text[start:position])
			start = position
		}
		_, size := utf8.DecodeRuneInString(text[position:])
		position += size
	}
	return append(parts, text[start:]), nil
}

func stringArgument(function string, values []interface{}, index int) (string, bool, error) {
	if values[index] == nil {
		return "", false, nil
	}
	text, isString := values[index].(string)
	if !isString {
		return "", false, &cypherfn.TypeMismatchError{Function: function, Expected: "String", Value: values[index]}
	}
	return text, true, nil
}

func fnCharLength(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, argumentCountError("char_length", "1", len(args))
	}
	values, err := evalArgs(ctx, args)
	if err != nil {
		return nil, err
	}
	text, ok, err := stringArgument("char_length", values, 0)
	if err != nil || !ok {
		return nil, err
	}
	return int64(utf8.RuneCountInString(text)), nil
}

func fnStringCase(convert func(string) string, name string) cypherfn.Func {
	return func(ctx cypherfn.Context, args []string) (interface{}, error) {
		if len(args) != 1 {
			return nil, argumentCountError(name, "1", len(args))
		}
		values, err := evalArgs(ctx, args)
		if err != nil {
			return nil, err
		}
		text, ok, err := stringArgument(name, values, 0)
		if err != nil || !ok {
			return nil, err
		}
		return convert(text), nil
	}
}

// trimCharacters trims the characters of cutset (whitespace when cutset is
// "") from the start and/or end of text.
func trimCharacters(text, cutset string, leading, trailing bool) string {
	if cutset == "" {
		if leading {
			text = strings.TrimLeftFunc(text, unicode.IsSpace)
		}
		if trailing {
			text = strings.TrimRightFunc(text, unicode.IsSpace)
		}
		return text
	}
	if leading {
		text = strings.TrimLeft(text, cutset)
	}
	if trailing {
		text = strings.TrimRight(text, cutset)
	}
	return text
}

// fnTrimFunction is btrim / ltrim / rtrim(original [, characters]).
func fnTrimFunction(name string, leading, trailing bool) cypherfn.Func {
	return func(ctx cypherfn.Context, args []string) (interface{}, error) {
		if len(args) < 1 || len(args) > 2 {
			return nil, argumentCountError(name, "1 or 2", len(args))
		}
		values, err := evalArgs(ctx, args)
		if err != nil {
			return nil, err
		}
		text, ok, err := stringArgument(name, values, 0)
		if err != nil || !ok {
			return nil, err
		}
		cutset := ""
		if len(values) == 2 {
			characters, present, err := stringArgument(name, values, 1)
			if err != nil || !present {
				return nil, err
			}
			cutset = characters
		}
		trimmed := trimCharacters(text, cutset, leading, trailing)
		if name == "rtrim" && cutset != "" && trimmed == "" && text != "" {
			// Neo4j's rtrim with characters never removes a first character
			// of one byte: rtrim('xx', 'x') is 'x', rtrim('ab', 'ab') is 'a'
			// (a multi-byte one goes: rtrim('éé', 'é') is '').
			if _, size := utf8.DecodeRuneInString(text); size == 1 {
				trimmed = text[:1]
			}
		}
		return trimmed, nil
	}
}

// fnTrim is trim(original) and Neo4j's
// trim([[LEADING | TRAILING | BOTH] [character] FROM] original): the
// character is a one-character string.
// trimSpecificationForm is trim(specification, original) and
// trim(specification, character, original), the forms Neo4j's FROM syntax
// stands for, which Neo4j also accepts as written. A null original or
// character is null, and a character that isn't one character long an
// ArgumentError. In Cypher 5 (Neo4j 5.26) the specification is matched
// exactly: 'LEADING' trims the start, 'TRAILING' the end, and any other
// string, 'leading' included, both ends; a null specification is a
// TypeError. In Cypher 25 (Neo4j 2026.09) it is matched in any case, a null
// specification is null and any other text an ArgumentError.
func trimSpecificationForm(ctx cypherfn.Context, args []string) (interface{}, error) {
	values, err := evalArgs(ctx, args)
	if err != nil {
		return nil, err
	}
	original := values[len(values)-1]
	if original == nil || len(values) == 3 && values[1] == nil || ctx.Cypher25 && values[0] == nil {
		return nil, nil
	}
	specification, isString := values[0].(string)
	if !isString {
		return nil, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
			localization.CypherCoreFunctionArgumentInvalid("trim", "a String", neo4jValueRepr(values[0])))
	}
	text, _, err := stringArgument("trim", values, len(values)-1)
	if err != nil {
		return nil, err
	}
	cutset := ""
	if len(values) == 3 {
		character, _, err := stringArgument("trim", values, 1)
		if err != nil {
			return nil, err
		}
		if utf8.RuneCountInString(character) != 1 {
			return nil, localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgument",
				localization.CypherCoreTrimCharacterLength())
		}
		cutset = character
	}
	if ctx.Cypher25 {
		switch specification = strings.ToUpper(specification); specification {
		case "LEADING", "TRAILING", "BOTH":
		default:
			return nil, localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgument",
				localization.CypherCoreTrimSpecificationUnknown())
		}
	}
	return trimCharacters(text, cutset, specification != "TRAILING", specification != "LEADING"), nil
}

func fnTrim(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) == 2 || len(args) == 3 {
		return trimSpecificationForm(ctx, args)
	}
	if len(args) != 1 {
		return nil, argumentCountError("trim", "1 to 3", len(args))
	}
	expression := strings.TrimSpace(args[0])
	leading, trailing := true, true
	characterExpression := ""
	if from := topLevelKeywordIndex(expression, "FROM"); from >= 0 {
		spec := strings.TrimSpace(expression[:from])
		expression = strings.TrimSpace(expression[from+len("FROM"):])
		for _, mode := range []string{"BOTH", "LEADING", "TRAILING"} {
			if startsWithKeywordFold(spec, mode) {
				leading, trailing = mode != "TRAILING", mode != "LEADING"
				spec = strings.TrimSpace(spec[len(mode):])
				break
			}
		}
		characterExpression = spec
	}
	text, err := ctx.Eval(expression)
	if err != nil {
		return nil, err
	}
	cutset := ""
	if characterExpression != "" {
		character, err := ctx.Eval(characterExpression)
		if err != nil {
			return nil, err
		}
		if character == nil {
			return nil, nil
		}
		value, isString := character.(string)
		if !isString {
			return nil, &cypherfn.TypeMismatchError{Function: "trim", Expected: "String", Value: character}
		}
		if utf8.RuneCountInString(value) != 1 {
			return nil, localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgument",
				localization.CypherCoreTrimCharacterLength())
		}
		cutset = value
	}
	if text == nil {
		return nil, nil
	}
	value, isString := text.(string)
	if !isString {
		return nil, &cypherfn.TypeMismatchError{Function: "trim", Expected: "String", Value: text}
	}
	return trimCharacters(value, cutset, leading, trailing), nil
}

// unicodeNormalForm is the normal form a keyword names (NFC, NFD, NFKC or
// NFKD, in any case), as normalize()'s second argument takes it.
func unicodeNormalForm(keyword string) (norm.Form, bool) {
	switch strings.ToUpper(strings.TrimSpace(keyword)) {
	case "NFC":
		return norm.NFC, true
	case "NFD":
		return norm.NFD, true
	case "NFKC":
		return norm.NFKC, true
	case "NFKD":
		return norm.NFKD, true
	}
	return norm.NFC, false
}

// fnNormalize is normalize(input [, NFC | NFD | NFKC | NFKD]); the normal
// form is a keyword, NFC by default.
func fnNormalize(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, argumentCountError("normalize", "1 or 2", len(args))
	}
	form := norm.NFC
	if len(args) == 2 {
		named, valid := unicodeNormalForm(args[1])
		if !valid {
			return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidArgument",
				localization.CypherCoreNormalizeFormInvalid(strings.TrimSpace(args[1])))
		}
		form = named
	}
	values, err := evalArgs(ctx, args[:1])
	if err != nil {
		return nil, err
	}
	text, ok, err := stringArgument("normalize", values, 0)
	if err != nil || !ok {
		return nil, err
	}
	return form.String(text), nil
}

// fnListConversion converts every item of a list with convert: an item it
// can't convert becomes null.
func fnListConversion(name string, convert versionedConversion) cypherfn.Func {
	return func(ctx cypherfn.Context, args []string) (interface{}, error) {
		if len(args) != 1 {
			return nil, argumentCountError(name, "1", len(args))
		}
		values, err := evalArgs(ctx, args)
		if err != nil || values[0] == nil {
			return nil, err
		}
		items, isList := cypherListValue(values[0])
		if vector, isVector := values[0].(CypherVector); isVector && (name == "toIntegerList" || name == "toFloatList") {
			// A vector's coordinates convert as a list's items would; Neo4j
			// takes a vector in toIntegerList and toFloatList only.
			items, isList = vector.coordinates(), true
		}
		if !isList {
			return nil, &cypherfn.TypeMismatchError{Function: name, Expected: "List<T>", Value: values[0]}
		}
		converted := make([]interface{}, len(items))
		for i, item := range items {
			converted[i] = convert(item, ctx.Cypher25)
		}
		return converted, nil
	}
}

// convertToIntegerOrNull is toIntegerOrNull: integers, floats (truncated),
// booleans (1 / 0) and integer strings.
func convertToIntegerOrNull(value interface{}) interface{} {
	switch typed := value.(type) {
	case bool:
		if typed {
			return int64(1)
		}
		return int64(0)
	case string:
		if integer, err := strconv.ParseInt(strings.TrimSpace(typed), 10, 64); err == nil {
			return integer
		}
		if number, err := strconv.ParseFloat(strings.TrimSpace(typed), 64); err == nil && !math.IsNaN(number) && !math.IsInf(number, 0) {
			return int64(number)
		}
		return nil
	}
	if integer, ok := cypherIntegerValue(value); ok {
		return integer
	}
	if number, ok := toFloat64(value); ok && !math.IsNaN(number) && !math.IsInf(number, 0) {
		return int64(number)
	}
	return nil
}

// convertToFloatOrNull is toFloatOrNull: numbers and numeric strings.
func convertToFloatOrNull(value interface{}) interface{} {
	if text, isString := value.(string); isString {
		if number, err := strconv.ParseFloat(strings.TrimSpace(text), 64); err == nil {
			return number
		}
		return nil
	}
	if _, isBool := value.(bool); isBool {
		return nil
	}
	if number, ok := toFloat64(value); ok {
		return number
	}
	return nil
}

// convertToStringOrNull is toStringOrNull: numbers, booleans, strings and
// temporal values; other values (lists, maps, entities) become null.
func convertToStringOrNull(value interface{}) interface{} {
	switch typed := value.(type) {
	case nil:
		return nil
	case string:
		return typed
	case bool:
		return strconv.FormatBool(typed)
	case float64:
		return formatCypherValueString(typed)
	case float32:
		return formatCypherValueString(typed)
	case time.Time, *time.Time, CypherLocalTime, *CypherLocalTime, CypherTime, *CypherTime,
		CypherLocalDateTime, *CypherLocalDateTime, CypherDateTime, *CypherDateTime, CypherPoint, *CypherPoint:
		return formatCypherValueString(typed)
	case []interface{}, map[string]interface{}, *storage.Node, *storage.Edge:
		return nil
	}
	if integer, ok := cypherIntegerValue(value); ok {
		return strconv.FormatInt(integer, 10)
	}
	if stringer, ok := value.(fmt.Stringer); ok {
		return stringer.String()
	}
	return nil
}

// convertToBooleanOrNull is toBooleanOrNull: booleans, 'true' / 'false'
// strings and integers (0 is false).
func convertToBooleanOrNull(value interface{}) interface{} {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "true":
			return true
		case "false":
			return false
		}
		return nil
	case float64, float32:
		return nil
	}
	if integer, ok := cypherIntegerValue(value); ok {
		return integer != 0
	}
	return nil
}

func fnNullIf(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) != 2 {
		return nil, argumentCountError("nullIf", "2", len(args))
	}
	values, err := evalArgs(ctx, args)
	if err != nil {
		return nil, err
	}
	if equal, _ := cypherEquality(values[0], values[1]).(bool); equal {
		return nil, nil
	}
	return values[0], nil
}

func fnValueType(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, argumentCountError("valueType", "1", len(args))
	}
	values, err := evalArgs(ctx, args)
	if err != nil {
		return nil, err
	}
	if values[0] == nil {
		return "NULL", nil
	}
	return valueTypeOf(values[0]).render(true), nil
}

// valueType is the Cypher type of a value as valueType() names it. A LIST
// holds its element types: merged, in Neo4j's type order, nullable when an
// element is null; no elements is NOTHING, only nulls is NULL.
type valueType struct {
	order    int
	name     string
	elements []valueType
	elemNull bool
}

// Neo4j's order of types in a union.
var valueTypeOrder = map[string]int{
	"BOOLEAN": 0, "STRING": 1, "UUID": 2, "INTEGER": 3, "FLOAT": 4, "DATE": 5, "LOCAL TIME": 6, "ZONED TIME": 7,
	"LOCAL DATETIME": 8, "ZONED DATETIME": 9, "DURATION": 10, "POINT": 11, "NODE": 12, "RELATIONSHIP": 13,
	"VECTOR": 14, "MAP": 15, "LIST": 16, "PATH": 17, "ANY": 18,
}

func namedValueType(name string) valueType {
	return valueType{order: valueTypeOrder[name], name: name}
}

// valueTypeOf names a value's type from the one classifier and table of
// type names (cypherValueKindOf, valueTypeNames, #657); a LIST also holds
// its element types.
func valueTypeOf(value interface{}) valueType {
	kind := cypherValueKindOf(value)
	if kind == valueKindVector {
		// VECTOR<INTEGER NOT NULL>(3): its coordinate type and dimension.
		vector, _ := vectorArgument(value)
		return valueType{order: valueTypeOrder["VECTOR"], name: "VECTOR<" + vectorCoordinateTypeNames[vector.Type].valueType + " NOT NULL>(" + strconv.Itoa(vector.Dimension()) + ")"}
	}
	if kind != valueKindList {
		return namedValueType(valueTypeNames[kind].typeSystem)
	}
	list := namedValueType("LIST")
	items, _ := cypherListValue(value)
	for _, item := range items {
		if item == nil {
			list.elemNull = true
			continue
		}
		list.elements = mergeValueType(list.elements, valueTypeOf(item))
	}
	sortValueTypes(list.elements)
	return list
}

// mergeValueType adds t to a union: a type already there absorbs it; two
// lists merge when they hold the same element types (nullability aside) or
// either holds none.
func mergeValueType(union []valueType, t valueType) []valueType {
	for i, member := range union {
		if member.name != t.name {
			continue
		}
		if member.name != "LIST" {
			return union
		}
		if len(member.elements) == 0 || len(t.elements) == 0 || sameElementNames(member.elements, t.elements) {
			merged := member
			merged.elemNull = member.elemNull || t.elemNull
			for _, element := range t.elements {
				merged.elements = mergeValueType(merged.elements, element)
			}
			sortValueTypes(merged.elements)
			union[i] = merged
			return union
		}
	}
	return append(union, t)
}

func sameElementNames(a, b []valueType) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].render(true) != b[i].render(true) && a[i].name != b[i].name {
			return false
		}
		if a[i].name == "LIST" && !sameElementNames(a[i].elements, b[i].elements) {
			return false
		}
	}
	return true
}

func sortValueTypes(types []valueType) {
	sort.SliceStable(types, func(i, j int) bool { return lessValueType(types[i], types[j]) })
}

func lessValueType(a, b valueType) bool {
	if a.order != b.order {
		return a.order < b.order
	}
	for i := 0; i < len(a.elements) && i < len(b.elements); i++ {
		if lessValueType(a.elements[i], b.elements[i]) {
			return true
		}
		if lessValueType(b.elements[i], a.elements[i]) {
			return false
		}
	}
	return len(a.elements) < len(b.elements)
}

func (t valueType) render(notNull bool) string {
	name := t.name
	if t.name == "LIST" {
		inner := "NOTHING"
		switch {
		case len(t.elements) > 0:
			parts := make([]string, len(t.elements))
			for i, element := range t.elements {
				parts[i] = element.render(!t.elemNull)
			}
			inner = strings.Join(parts, " | ")
		case t.elemNull:
			inner = "NULL"
		}
		name = "LIST<" + inner + ">"
	}
	if notNull {
		return name + " NOT NULL"
	}
	return name
}

// stringOperationOutOfRange is the error for a negative start or length of
// substring(), left() or right(): Neo4j 5.26's ExecutionFailed for a Cypher
// 5 statement, Neo4j 2026.09's ArgumentError ("out of range") for a Cypher
// 25 one.
func stringOperationOutOfRange(cypher25 bool, function string) error {
	if cypher25 {
		return localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgumentValue",
			localization.CypherCoreFunctionArgumentOutOfRange(function))
	}
	return newSemanticError("Neo.DatabaseError.Statement.ExecutionFailed", "InvalidArgumentValue", "Cannot handle negative start index nor negative length")
}
