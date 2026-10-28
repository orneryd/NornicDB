package cyphertext

import "strings"

// FunctionArgumentModifier is a supported leading function argument modifier.
// Values may be combined when selecting which modifiers a consumer admits.
type FunctionArgumentModifier uint8

const (
	NoArgumentModifier       FunctionArgumentModifier = 0
	AllArgumentModifier      FunctionArgumentModifier = 1
	DistinctArgumentModifier FunctionArgumentModifier = 2
)

// FunctionArgumentSource retains modifier and argument extents in the supplied
// original written argument body. NoArgumentModifier leaves the argument intact.
type FunctionArgumentSource struct {
	Modifier     FunctionArgumentModifier
	ModifierSpan Span
	Arguments    Span
}

// ReadFunctionArgumentSource classifies one original argument body once. ALL
// owns the existing lone-modifier zero-argument rule; DISTINCT retains its
// supplied contextual name policy. For example, DISTINCT all has one argument,
// while ALL alone has none. Quoted names and compound expressions stay arguments.
func ReadFunctionArgumentSource(text string, allowed FunctionArgumentModifier, distinctAsName func(string) bool) FunctionArgumentSource {
	trimmed := strings.TrimSpace(text)
	start := len(text) - len(strings.TrimLeftFunc(text, IsWhitespace))
	end := start + len(trimmed)
	product := FunctionArgumentSource{Arguments: Span{Start: start, End: end}}
	leading, unfinished := GapEnd(text[:end], start)
	if unfinished >= 0 {
		return product
	}
	product.Arguments.Start = leading
	for _, kind := range [...]FunctionArgumentModifier{AllArgumentModifier, DistinctArgumentModifier} {
		if allowed&kind == 0 {
			continue
		}
		keyword := "ALL"
		if kind == DistinctArgumentModifier {
			keyword = "DISTINCT"
		}
		tokenEnd := KeywordEnd(text[:end], leading, keyword, CypherWord)
		if tokenEnd < 0 {
			continue
		}
		body, unfinished := GapEnd(text[:end], tokenEnd)
		if unfinished >= 0 {
			return product
		}
		if kind == AllArgumentModifier && body != end {
			return product
		}
		if kind == DistinctArgumentModifier && body < end && distinctAsName != nil && distinctAsName(text[body:end]) {
			return product
		}
		product.Modifier = kind
		product.ModifierSpan = Span{Start: leading, End: tokenEnd}
		product.Arguments = Span{Start: body, End: end}
		return product
	}
	return product
}
