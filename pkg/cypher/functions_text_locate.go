package cypher

import (
	"regexp"
	"strings"
	"unicode/utf8"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

// text.locate, a NornicDB extension: where a quoted passage stands in a text,
// for a reader that cites facts as a line and a quote.
func init() {
	cypherfn.Register("text.locate", cypherfn.PrivilegeNone, fnTextLocate)
}

// textLocateTolerance is how many lines from the cited line an occurrence
// may start on when none starts on it.
const textLocateTolerance = 3

// fnTextLocate is text.locate(text, quote, options = {}): the span
// {start, end} of the quote's best occurrence in text, in code points
// ([start, end), as substring takes them), or null when it doesn't occur.
// The quote's words match in order, as a reader copies a quote
// (textLocatePattern): without case; with any run of whitespace, JSON escapes
// (\n, \r, \t) or a reply's > markers after a line break between them;
// typographic and plain quotation marks, apostrophes and dashes alike; and
// markdown emphasis (* ** _ __) or link markup ([ … ](target)) allowed around
// each word, never inside one. The span is the text's own: markup around its
// first and last words is in it. A copied leading line label ("12: ") and
// JSON escapes in the quote are dropped.
//
// options: line, the 1-based line the quote was cited on (the occurrence
// starting on it wins, else the nearest within tolerance lines, the lower
// line first; a line that isn't one of the text's is null); tolerance (3);
// from and to, the code-point range the occurrence must lie in. Without line,
// the first occurrence in the range wins. Null text or quote is null.
func fnTextLocate(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, argumentCountError("text.locate", "2 or 3", len(args))
	}
	values, err := evalArgs(ctx, args)
	if err != nil || values[0] == nil || values[1] == nil {
		return nil, err
	}
	text, isText := values[0].(string)
	if !isText {
		return nil, &cypherfn.TypeMismatchError{Function: "text.locate", Expected: "String", Value: values[0]}
	}
	quote, isText := values[1].(string)
	if !isText {
		return nil, &cypherfn.TypeMismatchError{Function: "text.locate", Expected: "String", Value: values[1]}
	}
	options := textLocateOptions{tolerance: textLocateTolerance, from: 0, to: -1}
	if len(values) == 3 && values[2] != nil {
		fields, isMap := values[2].(map[string]interface{})
		if !isMap {
			return nil, &cypherfn.TypeMismatchError{Function: "text.locate", Expected: "Map", Value: values[2]}
		}
		for name, target := range map[string]*int{"line": &options.line, "tolerance": &options.tolerance, "from": &options.from, "to": &options.to} {
			value, present := fields[name]
			if !present || value == nil {
				continue
			}
			if !isIntegerProcedureValue(value) {
				return nil, &cypherfn.TypeMismatchError{Function: "text.locate", Expected: "Integer", Value: value}
			}
			*target = int(toInt64(value))
			if name == "line" {
				options.hasLine = true
			}
		}
	}
	start, end, found := locateQuote(text, quote, options)
	if !found {
		return nil, nil
	}
	return map[string]interface{}{"start": int64(start), "end": int64(end)}, nil
}

type textLocateOptions struct {
	hasLine                   bool
	line, tolerance, from, to int
}

var (
	// textLocateLineLabel is a copied "n: " label at the quote's start.
	textLocateLineLabel = regexp.MustCompile(`^\s*[0-9]+:\s`)
	// textLocateEscapedBreak is a JSON string's escaped line break or tab,
	// as two characters: a JSON message file's body is one line whose breaks
	// are these.
	textLocateEscapedBreak = regexp.MustCompile(`\\[nrt]`)
)

const (
	// textLocateGap is what may stand between two quoted words: whitespace,
	// an escaped break, or a reply's > markers after a line break.
	textLocateGap = `(?:[\s\v\x{1c}-\x{1f}\x{85}\x{a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}]|\\[nrt]|\n[ \t]*>[> \t]*)+`
	// textLocateMarkupOpen and textLocateMarkupClose are markdown a reader
	// leaves out around a word: emphasis, and a link's brackets and target.
	textLocateMarkupOpen  = `(?:\*{1,2}|_{1,2}|\[)?`
	textLocateMarkupClose = `(?:\*{1,2}|_{1,2}|\]\([^)\s]*\))?`
)

// textLocateMarks are the forms of a character that match it: typographic
// and plain marks, and the letters equal to i without case.
var textLocateMarks = map[rune]string{}

func init() {
	// İ (U+0130) is i without case, by its simple lower-case mapping; Go's case
	// folding leaves it alone.
	for _, group := range []string{`'‘’`, `"“”`, `-‐‑–—`, `iIİ`} {
		class := "[" + regexp.QuoteMeta(group) + "]"
		for _, mark := range group {
			textLocateMarks[mark] = class
		}
	}
}

// textLocatePattern is the quote's words as one case-insensitive pattern
// (fnTextLocate's rules), or nil when the quote has no word.
func textLocatePattern(quote string) *regexp.Regexp {
	quote = textLocateLineLabel.ReplaceAllString(quote, "")
	words := strings.Fields(textLocateEscapedBreak.ReplaceAllString(quote, " "))
	if len(words) == 0 {
		return nil
	}
	parts := make([]string, len(words))
	for index, word := range words {
		var core strings.Builder
		for _, r := range word {
			if class, mark := textLocateMarks[r]; mark {
				core.WriteString(class)
			} else {
				core.WriteString(regexp.QuoteMeta(string(r)))
			}
		}
		parts[index] = textLocateMarkupOpen + core.String() + textLocateMarkupClose
	}
	return regexp.MustCompile(`(?i)` + strings.Join(parts, textLocateGap))
}

// locateQuote finds quote's best occurrence in text (fnTextLocate). It works
// on byte offsets and converts only the result to code points; each search
// starts at a candidate line (or from) and stops at the first match.
func locateQuote(text, quote string, options textLocateOptions) (start, end int, found bool) {
	pattern := textLocatePattern(quote)
	if pattern == nil {
		return 0, 0, false
	}
	low, high := codePointByteOffset(text, options.from), len(text)
	if options.to >= 0 {
		high = codePointByteOffset(text, options.to)
	}
	if low > high {
		return 0, 0, false
	}
	at := func(from int) (int, int, bool) {
		match := pattern.FindStringIndex(text[from:high])
		if match == nil {
			return 0, 0, false
		}
		return from + match[0], from + match[1], true
	}
	toCodePoints := func(s, e int) (int, int, bool) {
		first := utf8.RuneCountInString(text[:s])
		return first, first + utf8.RuneCountInString(text[s:e]), true
	}
	if !options.hasLine {
		if s, e, ok := at(low); ok {
			return toCodePoints(s, e)
		}
		return 0, 0, false
	}
	lineStarts := []int{0}
	for index := strings.IndexByte(text, '\n'); index >= 0; {
		lineStarts = append(lineStarts, index+1)
		next := strings.IndexByte(text[index+1:], '\n')
		if next < 0 {
			break
		}
		index += next + 1
	}
	lines := len(lineStarts)
	if options.line < 1 || options.line > lines {
		return 0, 0, false
	}
	for distance := 0; distance <= options.tolerance; distance++ {
		for _, number := range []int{options.line - distance, options.line + distance} {
			if number < 1 || number > lines || distance == 0 && number != options.line {
				continue
			}
			from := max(lineStarts[number-1], low)
			if from > high {
				continue
			}
			s, e, ok := at(from)
			if ok && (number == lines || s < lineStarts[number]) {
				return toCodePoints(s, e)
			}
			if distance == 0 {
				break
			}
		}
	}
	return 0, 0, false
}

// codePointByteOffset is the byte offset of text's code point at index,
// clamped to the text.
func codePointByteOffset(text string, index int) int {
	if index <= 0 {
		return 0
	}
	for offset := range text {
		if index == 0 {
			return offset
		}
		index--
	}
	return len(text)
}
