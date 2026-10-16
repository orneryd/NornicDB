package fn

import (
	cyphertext "github.com/orneryd/nornicdb/pkg/cypher/internal/text"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/text/transform"
)

// CharacterCount counts Unicode code points, borrowing the caller's work
// budget for a large string. Invalid UTF-8 retains RuneCountInString semantics.
func CharacterCount(text string, cancellation Cancellation) (int, error) {
	length, ok := cyphertext.LengthWithWork(text, cancellation)
	if !ok {
		return 0, cancellation.Err()
	}
	return length, nil
}

// MapStringCase is the Unicode case mapping shared by upper/lower spellings.
// x/text's root lower-caser resets word state on each Transform call, so a
// transform.Writer alone changes final sigma at a chunk boundary. A bounded
// window carries the preceding word state and the mapper's bounded lookahead.
func MapStringCase(text string, upper bool, cancellation Cancellation) (string, error) {
	if cancellation == nil || len(text) <= 4096 {
		if upper {
			return upperCaseText(text), nil
		}
		return lowerCaseText(text), nil
	}
	if upper {
		return TransformString(text, cases.Upper(language.Und), cancellation)
	}
	caser := cases.Lower(language.Und)
	var result strings.Builder
	prefix := ""
	for start := 0; start < len(text); {
		if !continueStringWork(cancellation, min(4096, len(text)-start)) {
			return "", cancellation.Err()
		}
		end := min(start+4096, len(text))
		// Finish a UTF-8 rune without allowing malformed continuation bytes
		// to make the chunk arbitrarily long.
		for end < len(text) && end < start+4099 && !utf8.RuneStart(text[end]) {
			end++
		}
		lookahead := end
		// x/text's final-sigma reader examines at most 31 following runes.
		for count := 0; count < 32 && lookahead < len(text); count++ {
			_, size := utf8.DecodeRuneInString(text[lookahead:])
			lookahead += size
		}
		core := prefix + text[start:end]
		mapped := caser.String(prefix + text[start:lookahead])
		// Root lower-casing's contextual sigma forms have equal byte length;
		// the bounded suffix determines how much output to leave for later.
		suffixLength := len(caser.String(text[end:lookahead]))
		if !cyphertext.AppendString(&result, mapped[len(prefix):len(mapped)-suffixLength], cancellation) {
			return "", cancellation.Err()
		}
		// This bounded probe asks the same mapping owner whether a following
		// sigma is mid-word. It handles arbitrarily long case-ignorable runs
		// without copying Unicode property tables or retaining the whole word.
		if strings.HasSuffix(caser.String(core+"Σ"), "ς") {
			prefix = "A"
		} else {
			prefix = " "
		}
		start = end
	}
	return result.String(), nil
}

// TransformString applies a stateful Unicode transform in bounded input
// chunks. The supplied cancellation budget remains the only polling owner.
func TransformString(text string, transformer transform.Transformer, cancellation Cancellation) (string, error) {
	if cancellation == nil {
		result, _, err := transform.String(transformer, text)
		return result, err
	}
	var result strings.Builder
	writer := transform.NewWriter(stringTransformOutput{result: &result, cancellation: cancellation}, transformer)
	for offset := 0; offset < len(text); offset += 4096 {
		if !continueStringWork(cancellation, min(4096, len(text)-offset)) {
			return "", cancellation.Err()
		}
		if _, err := writer.Write([]byte(text[offset:min(offset+4096, len(text))])); err != nil {
			return "", err
		}
	}
	if err := writer.Close(); err != nil {
		return "", err
	}
	return result.String(), nil
}

type stringTransformOutput struct {
	result       *strings.Builder
	cancellation Cancellation
}

func (out stringTransformOutput) Write(value []byte) (int, error) {
	if !cyphertext.AppendBytes(out.result, value, out.cancellation) {
		return 0, out.cancellation.Err()
	}
	return len(value), nil
}

// Unicode transforms do substantially more work per byte than a copy. Charge
// 256 bytes to one unit of the same shared budget; keep the bounded 4 KiB
// transform buffers without allowing 512 KiB of case mapping between polls.
func continueStringWork(cancellation Cancellation, bytes int) bool {
	for offset := 0; offset < bytes; offset += 256 {
		if !cancellation.Continue() {
			return false
		}
	}
	return true
}

func labelValues(labels []string, cancellation Cancellation) ([]interface{}, error) {
	capacity := len(labels)
	if cancellation != nil {
		capacity = min(capacity, 128)
	}
	result := make([]interface{}, 0, capacity)
	for _, label := range labels {
		if cancellation != nil && !cancellation.Continue() {
			return nil, cancellation.Err()
		}
		var ok bool
		result, ok = cyphertext.AppendItem(result, interface{}(label), cancellation)
		if !ok {
			return nil, cancellation.Err()
		}
	}
	return result, nil
}
