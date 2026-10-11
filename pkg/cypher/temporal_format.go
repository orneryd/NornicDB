package cypher

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// format(value [, pattern]) (Neo4j 2026.02): a temporal value as its ISO
// string, or printed with a Java DateTimeFormatter pattern; a duration with
// Neo4j's duration pattern. A null value or pattern gives null. A string
// first argument is NornicDB's printf extension, format(template, values…),
// which Neo4j rejects as a type mismatch.
func init() {
	cypherfn.Register("format", fnFormat)
}

// temporalFormatTypes is the type mismatch text of a format() value that
// isn't temporal.
const temporalFormatTypes = "Duration, Date, Time, LocalTime, LocalDateTime or DateTime"

func fnFormat(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) == 0 {
		return nil, functionParameterCountError("format", false)
	}
	values, err := evalArgs(ctx, args)
	if err != nil {
		return nil, err
	}
	if template, isString := values[0].(string); isString {
		return formatPrintf(template, values[1:])
	}
	if len(values) > 2 {
		return nil, functionParameterCountError("format", true)
	}
	for _, value := range values {
		if value == nil {
			return nil, nil
		}
	}
	duration, isDuration := values[0].(*CypherDuration)
	temporal, isTemporal := patternTemporalOf(values[0])
	if !isDuration && !isTemporal {
		return nil, &cypherfn.TypeMismatchError{Function: "format", Expected: temporalFormatTypes, Value: values[0]}
	}
	if len(values) == 1 {
		return formatTemporalISO(values[0]), nil
	}
	pattern, isString := values[1].(string)
	if !isString {
		return nil, &cypherfn.TypeMismatchError{Function: "format", Expected: "String", Value: values[1]}
	}
	if isDuration {
		return formatDurationPattern(duration, pattern)
	}
	if items, ok := compileTemporalPattern(pattern); ok {
		if text, ok := formatTemporalPattern(items, temporal); ok {
			return text, nil
		}
	}
	return nil, localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgument",
		localization.CypherCoreTemporalPatternInvalidCharacter(temporal.typeName))
}

// formatTemporalISO is format()'s ISO 8601 text of a temporal value, which,
// unlike toString(), always has the seconds (12:00:00, not 12:00).
func formatTemporalISO(value interface{}) string {
	switch typed := value.(type) {
	case CypherLocalTime:
		return formatTemporalClock(typed.Time, false, "", true)
	case CypherTime:
		return formatTemporalClock(typed.Time, true, "", true)
	case CypherLocalDateTime:
		return formatTemporalDateTime(typed.Time, false, "", true)
	case CypherDateTime:
		return formatTemporalDateTime(typed.Time, true, typed.ZoneID, true)
	case time.Time:
		return formatTemporalDateTime(typed, true, "", true)
	}
	return fmt.Sprint(value)
}

// formatDurationPattern prints a duration with Neo4j's duration pattern. A
// run of a unit letter prints that unit's amount, zero-padded to the run's
// length: years (y, u, Y), quarters (Q, q), months (M, L), weeks (w, W), days
// (d, D), hours (H, h, k, K), minutes (m), seconds (s), the first digits of
// the fraction of a second (S, n), and the whole time part in milliseconds
// (A) or nanoseconds (N). The largest unit of each group (months, days,
// time) in the pattern takes the group's whole amount and smaller ones what
// remains (y M of 14 months prints 1 2; M alone prints 14; s A of 6.007
// seconds prints 6 7). Other characters print as they are, and quoted text
// as written, two quotes making one.
func formatDurationPattern(duration *CypherDuration, pattern string) (string, error) {
	months, days, seconds, nanos := durationGroups(duration)
	seconds += nanos / 1_000_000_000
	nanos %= 1_000_000_000
	if nanos < 0 {
		seconds--
		nanos += 1_000_000_000
	}
	present := func(letters string) bool { return strings.ContainsAny(stripDurationQuotes(pattern), letters) }
	hasYear, hasQuarter, hasWeek := present("yuY"), present("Qq"), present("wW")
	hasHour, hasMinute, hasSecond := present("HhkK"), present("m"), present("s")
	// The seconds A and N count: those smaller than the largest time unit.
	remainingSeconds := seconds
	switch {
	case hasSecond:
		remainingSeconds = 0
	case hasMinute:
		remainingSeconds = seconds % 60
	case hasHour:
		remainingSeconds = seconds % 3_600
	}
	var out strings.Builder
	for index := 0; index < len(pattern); {
		c := pattern[index]
		if c == '\'' {
			end := index + 1
			for {
				if end >= len(pattern) {
					return "", localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgument",
						localization.CypherCoreDurationPatternUnbalancedEscapes())
				}
				if pattern[end] == '\'' {
					if end+1 < len(pattern) && pattern[end+1] == '\'' {
						out.WriteByte('\'')
						end += 2
						continue
					}
					break
				}
				out.WriteByte(pattern[end])
				end++
			}
			index = end + 1
			continue
		}
		end := index + 1
		for end < len(pattern) && pattern[end] == c {
			end++
		}
		count := end - index
		var value int64
		switch c {
		case 'y', 'u', 'Y':
			value = months / 12
		case 'Q', 'q':
			value = months / 3
			if hasYear {
				value = months % 12 / 3
			}
		case 'M', 'L':
			value = months
			if hasQuarter {
				value = months % 3
			} else if hasYear {
				value = months % 12
			}
		case 'w', 'W':
			value = days / 7
		case 'd', 'D':
			value = days
			if hasWeek {
				value = days % 7
			}
		case 'H', 'h', 'k', 'K':
			value = seconds / 3_600
		case 'm':
			value = seconds / 60
			if hasHour {
				value = seconds % 3_600 / 60
			}
		case 's':
			value = seconds
			if hasMinute {
				value = seconds % 60
			} else if hasHour {
				value = seconds % 3_600
			}
		case 'S', 'n':
			fraction := padDigits(strconv.FormatInt(nanos, 10), 9)
			if count > 9 {
				fraction += strings.Repeat("0", count-9)
			}
			out.WriteString(fraction[:count])
			index = end
			continue
		case 'A':
			value = remainingSeconds*1_000 + nanos/1_000_000
		case 'N':
			value = remainingSeconds*1_000_000_000 + nanos
		default:
			out.WriteString(pattern[index:end])
			index = end
			continue
		}
		out.WriteString(padDigits(strconv.FormatInt(value, 10), count))
		index = end
	}
	return out.String(), nil
}

// stripDurationQuotes is a duration pattern without its quoted text, which
// names no unit.
func stripDurationQuotes(pattern string) string {
	if strings.IndexByte(pattern, '\'') < 0 {
		return pattern
	}
	var out strings.Builder
	quoted := false
	for index := 0; index < len(pattern); index++ {
		if pattern[index] == '\'' {
			quoted = !quoted
			continue
		}
		if !quoted {
			out.WriteByte(pattern[index])
		}
	}
	return out.String()
}

// temporalPatternTypeNames are the Cypher 25 type names a temporal
// constructor's pattern errors name.
var temporalPatternTypeNames = map[string]string{
	"date": "DATE", "localtime": "LOCAL TIME", "time": "ZONED TIME", "localdatetime": "LOCAL DATETIME", "datetime": "ZONED DATETIME",
}

// constructTemporalWithPattern is date(input, pattern) and the other
// constructors' pattern form: input read with a Java DateTimeFormatter
// pattern. A null input or pattern gives null; an input that isn't a string
// is ProcedureCallFailed, a pattern that isn't a string a type mismatch, and
// text the pattern doesn't read as the type a SyntaxError.
func constructTemporalWithPattern(kind string, input, pattern interface{}) (interface{}, error) {
	if input == nil || pattern == nil {
		return nil, nil
	}
	text, isString := input.(string)
	if !isString {
		return nil, localizedStatusError("Neo.ClientError.Procedure.ProcedureCallFailed", "InvalidArgument",
			localization.CypherCoreTemporalPatternRequiresString())
	}
	patternText, isString := pattern.(string)
	if !isString {
		return nil, typeMismatchFromFunctionError(&cypherfn.TypeMismatchError{Function: kind, Expected: "String", Value: pattern})
	}
	typeName := temporalPatternTypeNames[kind]
	if value, ok := parseTemporalPattern(kind, text, patternText); ok {
		return value, nil
	}
	return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidArgument",
		localization.CypherCoreTemporalPatternMismatch(patternText, text, typeName))
}

// formatPrintf is NornicDB's printf extension, format(template, values…):
// each verb of the template prints the next value. %s, %q and %v take any
// value, written as Cypher writes it ('a', 1, true, [1, 2]); %d, %b, %o, %c
// and %U an integer; %x and %X an integer or a string; %e, %E, %f, %F, %g and
// %G a number; %t a boolean. Flags, width and precision are Go's; %% is a %.
// A null value gives null. A verb without its value, a value without its
// verb, a value its verb can't print and any other verb (%*d, %[1]d, %z) are
// errors, never text in the result.
func formatPrintf(template string, values []interface{}) (interface{}, error) {
	for _, value := range values {
		if value == nil {
			return nil, nil
		}
	}
	arguments := make([]interface{}, 0, len(values))
	verbs := 0
	for i := 0; i < len(template); i++ {
		if template[i] != '%' {
			continue
		}
		start := i
		i++
		for i < len(template) && strings.IndexByte("+-# 0", template[i]) >= 0 {
			i++
		}
		for i < len(template) && (isDigitByte(template[i]) || template[i] == '.') {
			i++
		}
		if i >= len(template) {
			return nil, formatTemplateVerbInvalid(template[start:])
		}
		verb := template[i]
		if verb == '%' && i == start+1 {
			continue
		}
		if verbs >= len(values) {
			verbs++
			continue
		}
		argument, err := formatPrintfArgument(template[start:i+1], verb, values[verbs])
		if err != nil {
			return nil, err
		}
		arguments = append(arguments, argument)
		verbs++
	}
	if verbs != len(values) {
		return nil, localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgumentValue",
			localization.CypherCoreFormatTemplateValueCount(verbs, len(values)))
	}
	return fmt.Sprintf(template, arguments...), nil
}

// formatPrintfArgument is value as verb prints it, or the error for a value
// verb can't print.
func formatPrintfArgument(spec string, verb byte, value interface{}) (interface{}, error) {
	switch verb {
	case 's', 'q', 'v':
		switch typed := value.(type) {
		case string:
			return typed, nil
		case []interface{}, map[string]interface{}:
			return valueToCypherLiteral(typed), nil
		}
		return formatCypherValueString(value), nil
	case 'd', 'b', 'o', 'c', 'U', 'x', 'X':
		if integer, isInteger := cypherIntegerValue(value); isInteger {
			return integer, nil
		}
		if text, isString := value.(string); isString && (verb == 'x' || verb == 'X') {
			return text, nil
		}
	case 'e', 'E', 'f', 'F', 'g', 'G':
		if number, isNumber := toFloat64(value); isNumber && isRuntimeNumber(value) {
			return number, nil
		}
	case 't':
		if boolean, isBool := value.(bool); isBool {
			return boolean, nil
		}
	default:
		return nil, formatTemplateVerbInvalid(spec)
	}
	return nil, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
		localization.CypherCoreFormatTemplateValueType(spec, neo4jValueRepr(value)))
}

func formatTemplateVerbInvalid(spec string) error {
	return localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgumentValue",
		localization.CypherCoreFormatTemplateVerbInvalid(spec))
}
