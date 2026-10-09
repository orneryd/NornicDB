package cypher

import (
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// checkStaticLiteralArguments is Neo4j's compile-time check of literal
// argument values, which fails the statement whatever the data:
//   - percentileCont / percentileDisc with a number literal outside 0.0..1.0,
//     or the null literal, as the percentile (an expression such as 0.5 + 1 is checked when it
//     runs, an ArgumentError);
//   - point() with a map literal whose keys don't describe a point (neither x
//     and y nor latitude and longitude); a map value is checked when it runs
//     (newPointFromMap).
func checkStaticLiteralArguments(function string, arguments []string) error {
	switch {
	case strings.EqualFold(function, "percentileCont") || strings.EqualFold(function, "percentileDisc"):
		if len(arguments) < 2 {
			return nil
		}
		text := strings.TrimSpace(arguments[1])
		value, literal := parseLiteralValueFromComputedRow(text)
		number, numeric := toFloat64(value)
		// A null literal is no percentile either (Neo4j 5.26: "Invalid
		// input 'NULL' is not a valid argument"); a null reaching it through
		// a variable or parameter is checked when it runs.
		if literal && (value == nil || numeric && (number < 0 || number > 1)) {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidArgument",
				localization.CypherCorePercentileOutOfRange(text))
		}
	case strings.EqualFold(function, "point"):
		if len(arguments) != 1 {
			return nil
		}
		var x, y, latitude, longitude bool
		isMap := visitStaticMapLiteralKeys(arguments[0], func(key string) {
			switch key {
			case "x":
				x = true
			case "y":
				y = true
			case "latitude":
				latitude = true
			case "longitude":
				longitude = true
			}
		})
		if !isMap || x && y || latitude && longitude {
			return nil
		}
		keys, _ := staticMapLiteralKeys(arguments[0])
		quoted := make([]string, 0, len(keys))
		for key := range keys {
			quoted = append(quoted, "'"+key+"'")
		}
		sort.Strings(quoted)
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidPoint",
			localization.CypherCorePointMapKeysInvalid(strings.Join(quoted, ", ")))
	}
	return nil
}

// staticMapLiteralKeys returns the keys of a map literal ({x: 1, `y`: 2});
// isMap is false for any other expression.
func staticMapLiteralKeys(expression string) (keys map[string]bool, isMap bool) {
	keys = make(map[string]bool)
	if !visitStaticMapLiteralKeys(expression, func(key string) { keys[key] = true }) {
		return nil, false
	}
	return keys, true
}

// visitStaticMapLiteralKeys calls visit with each key of a map literal
// ({x: 1, `y`: 2}), unquoted, without allocating for an ordinary map; it
// reports false for any other expression, whose keys visit may have seen in
// part. point() checks the keys of a map literal argument (newPointFromMap
// reads x and y or latitude and longitude).
func visitStaticMapLiteralKeys(expression string, visit func(key string)) bool {
	expression = strings.TrimSpace(expression)
	if !strings.HasPrefix(expression, "{") || findMatchingDelimiter(expression, 0, '{', '}') != len(expression)-1 {
		return false
	}
	var buffer [8]string
	for _, entry := range appendTopLevelComma(buffer[:0], expression[1:len(expression)-1]) {
		colon := strings.IndexByte(entry, ':')
		if colon < 0 {
			return false
		}
		key := strings.TrimSpace(entry[:colon])
		if len(key) >= 2 && key[0] == '`' && key[len(key)-1] == '`' {
			key = strings.ReplaceAll(key[1:len(key)-1], "``", "`")
		}
		visit(key)
	}
	return true
}
