package cypher

import (
	"fmt"
	"strings"
)

// cypherRegexMatch is Cypher's text =~ pattern, shared by every evaluator
// (#462, #610):
//
//   - the pattern must match the whole string ('Tom' =~ 'o' is false), as
//     Neo4j's Java Pattern.matches does;
//   - a null text or pattern gives null;
//   - a text or pattern that is not a string is Neo4j's
//     "Type mismatch: expected String but was <type>" SyntaxError;
//   - an invalid pattern is "Invalid Regex: ..." (SemanticError).
func cypherRegexMatch(text, pattern interface{}) (interface{}, error) {
	if text == nil || pattern == nil {
		return nil, nil
	}
	textString, textOK := text.(string)
	patternString, patternOK := pattern.(string)
	if !textOK || !patternOK {
		operand := text
		if textOK {
			operand = pattern
		}
		return nil, newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidArgumentType",
			fmt.Sprintf("Type mismatch: expected String but was %s", cypherTypeName(operand)),
		)
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
