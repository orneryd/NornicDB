package cypher

import (
	"math/rand"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// Compare the shortcut to the original lexical walk at every byte boundary,
// including partial strings and comments, and multi-byte whitespace.
func TestPR1682LastLiveByteParity(t *testing.T) {
	reference := func(text string, position int) int {
		last := -1
		for index := 0; index < position && index < len(text); {
			if end := cyphertext.CommentEnd(text, index); end >= 0 {
				index = end
				continue
			}
			if cyphertext.IsQuote(text[index]) {
				end, closed := cyphertext.ScanQuotedText(text, index)
				last = index
				if closed && end <= position {
					last = end - 1
				}
				index = end
				continue
			}
			if !cyphertext.IsWhitespaceAt(text, index) {
				last = index
			}
			index++
		}
		return last
	}
	r := rand.New(rand.NewSource(1682))
	alphabet := []rune(" abc\t\n\r/*'\"`\\é\u2003()")
	for n := 0; n < 5000; n++ {
		chars := make([]rune, r.Intn(100))
		for i := range chars {
			chars[i] = alphabet[r.Intn(len(alphabet))]
		}
		text := string(chars)
		for position := -1; position <= len(text)+1; position++ {
			if got, want := lastLiveByte(text, position), reference(text, position); got != want {
				t.Fatalf("text=%q position=%d: got %d want %d", text, position, got, want)
			}
		}
	}
}
