package cypher

import (
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// checkStaticLiteralArguments is Neo4j's compile-time check of literal
// argument values, which fails the statement whatever the data:
//   - percentileCont / percentileDisc with a number literal outside 0.0..1.0
//     as the percentile (an expression such as 0.5 + 1 is checked when it
//     runs, an ArgumentError);
//   - point() with a map literal whose keys don't describe a point (neither x
//     and y nor latitude and longitude); a map value is checked when it runs
//     (newPointFromMap).
func checkStaticLiteralArguments(function string, arguments []string) error {
	switch lowerASCII(function) {
	case "percentilecont", "percentiledisc":
		if len(arguments) < 2 {
			return nil
		}
		text := strings.TrimSpace(arguments[1])
		value, literal := parseLiteralValueFromComputedRow(text)
		number, numeric := toFloat64(value)
		if literal && numeric && (number < 0 || number > 1) {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidArgument",
				localization.CypherCorePercentileOutOfRange(text))
		}
	case "point":
		if len(arguments) != 1 {
			return nil
		}
		keys, isMap := staticMapLiteralKeys(arguments[0])
		if !isMap || pointMapDescribesPoint(keys) {
			return nil
		}
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
	expression = strings.TrimSpace(expression)
	if !strings.HasPrefix(expression, "{") || findMatchingDelimiter(expression, 0, '{', '}') != len(expression)-1 {
		return nil, false
	}
	keys = make(map[string]bool)
	for _, entry := range splitTopLevelComma(expression[1 : len(expression)-1]) {
		colon := strings.IndexByte(entry, ':')
		if colon < 0 {
			return nil, false
		}
		key := strings.TrimSpace(entry[:colon])
		if len(key) >= 2 && key[0] == '`' && key[len(key)-1] == '`' {
			key = strings.ReplaceAll(key[1:len(key)-1], "``", "`")
		}
		keys[key] = true
	}
	return keys, true
}

// pointMapDescribesPoint reports whether a map with these keys describes a
// point: x and y (cartesian) or latitude and longitude (geographic), as
// newPointFromMap reads it.
func pointMapDescribesPoint(keys map[string]bool) bool {
	return keys["x"] && keys["y"] || keys["latitude"] && keys["longitude"]
}
