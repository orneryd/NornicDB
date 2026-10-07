package cypher

import (
	"fmt"
	"math"
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
		"acos":     fdlibmAcos,
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
	cypherfn.Register("ceiling", fnMathUnary("ceiling", math.Ceil))
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
	cypherfn.Register("tointegerlist", fnListConversion("toIntegerList", convertToIntegerOrNull))
	cypherfn.Register("tofloatlist", fnListConversion("toFloatList", convertToFloatOrNull))
	cypherfn.Register("tostringlist", fnListConversion("toStringList", convertToStringOrNull))
	cypherfn.Register("tobooleanlist", fnListConversion("toBooleanList", convertToBooleanOrNull))
	cypherfn.Register("valuetype", fnValueType)
	cypherfn.Register("nullif", fnNullIf)
	cypherfn.Register("tail", fnTail)
	for _, name := range []string{"substring", "left", "right", "replace", "split"} {
		cypherfn.Register(name, fnStringOperation(name))
	}
	for _, name := range []string{"tointeger", "toint", "tofloat", "toboolean", "tostring"} {
		convert := map[string]func(interface{}) interface{}{
			"tointeger": convertToIntegerOrNull, "toint": convertToIntegerOrNull,
			"tofloat": convertToFloatOrNull, "toboolean": convertToBooleanOrNull, "tostring": convertToStringOrNull,
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

func fnTail(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, argumentCountError("tail", "1", len(args))
	}
	value, err := ctx.Eval(args[0])
	if err != nil || value == nil {
		return nil, err
	}
	items, list := cypherListValue(value)
	if !list {
		return nil, &cypherfn.TypeMismatchError{Function: "tail", Expected: "List<T>", Value: value}
	}
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
			minimum, maximum = 3, 3
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
				return strings.ReplaceAll(text, separator, replacement), nil
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
			return nil, newSemanticError("Neo.DatabaseError.Statement.ExecutionFailed", "InvalidArgumentValue", "Cannot handle negative start index nor negative length")
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
			return nil, newSemanticError("Neo.DatabaseError.Statement.ExecutionFailed", "InvalidArgumentValue", "Cannot handle negative start index nor negative length")
		}
		return cyphertext.Substring(text, position, length), nil
	}
}

func fnScalarConversion(name string, convert func(interface{}) interface{}, orNull bool) cypherfn.Func {
	return func(ctx cypherfn.Context, args []string) (interface{}, error) {
		if len(args) != 1 {
			return nil, argumentCountError(name, "1", len(args))
		}
		value, err := ctx.Eval(args[0])
		if err != nil || value == nil {
			return nil, err
		}
		if !orNull && !validConversionArgument(name, value) {
			return nil, newSemanticError("Neo.ClientError.Statement.TypeError", "InvalidArgumentValue",
				fmt.Sprintf("Invalid input for function '%s()': Expected %s, got: %s", conversionFunctionNames[name], conversionFunctionInputs[name], neo4jValueRepr(value)))
		}
		return convert(value), nil
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
	return number * math.Pi / 180, nil
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
// stands for, which Neo4j also accepts as written. The specification is
// matched exactly: 'LEADING' trims the start, 'TRAILING' the end, and any
// other string, 'leading' included, both ends. A null original or
// character is null; otherwise a specification that isn't a string is a
// TypeError and a character that isn't one character long an ArgumentError,
// as in Neo4j.
func trimSpecificationForm(ctx cypherfn.Context, args []string) (interface{}, error) {
	values, err := evalArgs(ctx, args)
	if err != nil {
		return nil, err
	}
	original := values[len(values)-1]
	if original == nil || len(values) == 3 && values[1] == nil {
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
	return trimCharacters(text, cutset, specification != "TRAILING", specification != "LEADING"), nil
}

func fnTrim(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) == 2 || len(args) == 3 {
		return trimSpecificationForm(ctx, args)
	}
	if len(args) != 1 {
		return nil, argumentCountError("trim", "1", len(args))
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
func fnListConversion(name string, convert func(interface{}) interface{}) cypherfn.Func {
	return func(ctx cypherfn.Context, args []string) (interface{}, error) {
		if len(args) != 1 {
			return nil, argumentCountError(name, "1", len(args))
		}
		values, err := evalArgs(ctx, args)
		if err != nil || values[0] == nil {
			return nil, err
		}
		items, isList := cypherListValue(values[0])
		if !isList {
			return nil, &cypherfn.TypeMismatchError{Function: name, Expected: "List<T>", Value: values[0]}
		}
		converted := make([]interface{}, len(items))
		for i, item := range items {
			converted[i] = convert(item)
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
	"BOOLEAN": 0, "STRING": 1, "INTEGER": 2, "FLOAT": 3, "DATE": 4, "LOCAL TIME": 5, "ZONED TIME": 6,
	"LOCAL DATETIME": 7, "ZONED DATETIME": 8, "DURATION": 9, "POINT": 10, "NODE": 11, "RELATIONSHIP": 12,
	"MAP": 13, "LIST": 14, "PATH": 15, "ANY": 16,
}

func namedValueType(name string) valueType {
	return valueType{order: valueTypeOrder[name], name: name}
}

// valueTypeOf names a value's type from the one classifier and table of
// type names (cypherValueKindOf, valueTypeNames, #657); a LIST also holds
// its element types.
func valueTypeOf(value interface{}) valueType {
	kind := cypherValueKindOf(value)
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
