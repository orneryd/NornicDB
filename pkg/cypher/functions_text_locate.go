package cypher

import (
	"sort"
	"strings"
	"unicode"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

// text.locate, a NornicDB extension: where a quoted passage stands in a text,
// for a reader that cites facts as a line and a quote (Personal Documents R5).
func init() {
	cypherfn.Register("text.locate", fnTextLocate)
}

// textLocateTolerance is how many lines from the cited line an occurrence
// may start on when none starts on it.
const textLocateTolerance = 3

// fnTextLocate is text.locate(text, quote, options = {}): the span
// {start, end} of the quote's best occurrence in text, in code points
// ([start, end), as substring takes them), or null when it doesn't occur.
// The match is loose, as a quote copied by a reader is:
//   - letters match without case, and any run of whitespace matches any run;
//   - typographic and plain quotation marks, apostrophes and dashes match
//     each other;
//   - markdown emphasis (* and _) and link markup ([label](target)) around
//     words are skipped, in the text and the quote;
//   - a reply's > markers at a line start are skipped, in both;
//   - in the quote, a JSON escape \n, \r or \t is whitespace, and a copied
//     leading line label ("12: ") is dropped.
//
// options: line, the 1-based line the quote was cited on (the occurrence
// starting on it wins, else the nearest within tolerance lines, else none);
// tolerance (3); from and to, the code-point range the occurrence must lie
// in. Without line, the first occurrence in the range wins. Null text or
// quote is null.
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
	options := textLocateOptions{line: -1, tolerance: textLocateTolerance, from: 0, to: -1}
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
		}
	}
	start, end, found := locateQuote(text, quote, options)
	if !found {
		return nil, nil
	}
	return map[string]interface{}{"start": int64(start), "end": int64(end)}, nil
}

type textLocateOptions struct {
	line, tolerance, from, to int
}

// looseText is a text reduced for loose matching: its folded runes, each
// with the code-point offset of the rune it came from.
type looseText struct {
	runes   []rune
	offsets []int
}

// locateQuote finds quote's best occurrence in text (fnTextLocate).
func locateQuote(text, quote string, options textLocateOptions) (start, end int, found bool) {
	needle := looseReduce(unescapeQuote(quote)).runes
	for len(needle) > 0 && needle[0] == ' ' {
		needle = needle[1:]
	}
	for len(needle) > 0 && needle[len(needle)-1] == ' ' {
		needle = needle[:len(needle)-1]
	}
	if len(needle) == 0 {
		return 0, 0, false
	}
	haystack := looseReduce(text)
	var lineStarts []int // code-point offsets where lines start, for line numbers
	if options.line >= 0 {
		lineStarts = append(lineStarts, 0)
		offset := 0
		for _, r := range text {
			offset++
			if r == '\n' {
				lineStarts = append(lineStarts, offset)
			}
		}
	}
	bestDistance := -1
	for i := 0; i+len(needle) <= len(haystack.runes); i++ {
		if !runesEqualAt(haystack.runes, i, needle) {
			continue
		}
		s, e := haystack.offsets[i], haystack.offsets[i+len(needle)-1]+1
		if s < options.from || options.to >= 0 && e > options.to {
			continue
		}
		if options.line < 0 {
			return s, e, true
		}
		line := sort.Search(len(lineStarts), func(k int) bool { return lineStarts[k] > s }) // 1-based
		distance := line - options.line
		if distance < 0 {
			distance = -distance
		}
		if distance <= options.tolerance && (bestDistance < 0 || distance < bestDistance) {
			start, end, bestDistance = s, e, distance
			if distance == 0 {
				break
			}
		}
	}
	return start, end, bestDistance >= 0
}

func runesEqualAt(runes []rune, at int, needle []rune) bool {
	for k, r := range needle {
		if runes[at+k] != r {
			return false
		}
	}
	return true
}

// unescapeQuote turns a quote's JSON escapes \n, \r and \t into spaces and
// drops a copied leading line label ("12: ").
func unescapeQuote(quote string) string {
	quote = strings.NewReplacer(`\n`, " ", `\r`, " ", `\t`, " ").Replace(quote)
	trimmed := strings.TrimLeft(quote, " \t")
	digits := 0
	for digits < len(trimmed) && trimmed[digits] >= '0' && trimmed[digits] <= '9' {
		digits++
	}
	if digits > 0 && digits+1 < len(trimmed) && trimmed[digits] == ':' && (trimmed[digits+1] == ' ' || trimmed[digits+1] == '\t') {
		return trimmed[digits+2:]
	}
	return quote
}

// looseReduce folds text for loose matching (fnTextLocate's rules): one
// space per whitespace run, lower case, plain quotes and dashes, without
// markdown emphasis, link markup or a line's leading > markers.
func looseReduce(text string) looseText {
	out := looseText{runes: make([]rune, 0, len(text)), offsets: make([]int, 0, len(text))}
	runes := []rune(text)
	space := func(at int) {
		if n := len(out.runes); n > 0 && out.runes[n-1] != ' ' {
			out.runes = append(out.runes, ' ')
			out.offsets = append(out.offsets, at)
		}
	}
	lineStart := true
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		if lineStart {
			// A reply's > markers (and the spaces among them) at a line start.
			j := i
			for j < len(runes) && (runes[j] == ' ' || runes[j] == '\t' || runes[j] == '>') {
				j++
			}
			if j > i && strings.ContainsRune(string(runes[i:j]), '>') {
				space(i)
				i = j - 1
				lineStart = false
				continue
			}
			lineStart = false
		}
		switch {
		case r == '\n':
			space(i)
			lineStart = true
			continue
		case unicode.IsSpace(r):
			space(i)
			continue
		case r == '*' || r == '_':
			continue
		case r == '[':
			continue
		case r == ']':
			// ](target): the rest of a markdown link.
			if i+1 < len(runes) && runes[i+1] == '(' {
				if close := indexRune(runes, i+2, ')'); close >= 0 {
					i = close
				}
			}
			continue
		}
		out.runes = append(out.runes, looseRune(r))
		out.offsets = append(out.offsets, i)
	}
	return out
}

func indexRune(runes []rune, from int, target rune) int {
	for i := from; i < len(runes); i++ {
		if runes[i] == target {
			return i
		}
	}
	return -1
}

// looseRune is r lower-cased, with typographic quotation marks, apostrophes
// and dashes as their plain forms.
func looseRune(r rune) rune {
	switch r {
	case '‘', '’', '‚', '‛', '′', '`':
		return '\''
	case '“', '”', '„', '‟', '″', '«', '»':
		return '"'
	case '‐', '‑', '‒', '–', '—', '―', '−':
		return '-'
	}
	return unicode.ToLower(r)
}
