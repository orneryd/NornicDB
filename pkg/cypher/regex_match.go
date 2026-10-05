package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// cypherRegexMatch is Cypher's text =~ pattern, shared by every evaluator
// (#462, #610):
//
//   - the pattern must match the whole string ('Tom' =~ 'o' is false), as
//     Neo4j's Java Pattern.matches does;
//   - a null text or pattern gives null;
//   - a text that is not a string gives null, as in Neo4j at run time
//     (n.big =~ 'x');
//   - a string text with a pattern that is not a string is a TypeError
//     ('x' =~ n.big);
//   - an invalid pattern is "Invalid Regex: ..." (SemanticError).
//
// Operands whose type is known before the statement runs ('x' =~ 1) are
// rejected earlier with a SyntaxError (validateStaticOperatorTypes).
func cypherRegexMatch(text, pattern interface{}) (interface{}, error) {
	if text == nil || pattern == nil {
		return nil, nil
	}
	textString, textOK := text.(string)
	if !textOK {
		return nil, nil
	}
	patternString, patternOK := pattern.(string)
	if !patternOK {
		return nil, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType", localization.CypherCoreRegexPatternTypeMismatch(neo4jValueRepr(pattern)))
	}
	re, err := GetCachedRegex(anchoredRegexPattern(patternString))
	if err != nil {
		return nil, newSemanticError(
			"Neo.ClientError.Statement.SemanticError",
			"InvalidRegex",
			"Invalid Regex: "+err.Error(),
		)
	}
	return re.MatchString(textString), nil
}

// anchoredRegexPattern makes a pattern match only the whole input.
func anchoredRegexPattern(pattern string) string {
	var b strings.Builder
	b.Grow(len(pattern) + 8)
	b.WriteString(`^(?:`)
	b.WriteString(pattern)
	b.WriteString(`)$`)
	return b.String()
}
