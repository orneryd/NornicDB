package cypher

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
)

// liveByteTexts hold every lexical item lastLiveByte tells apart: comments,
// the three quotes, escapes, unclosed spans, multi-byte whitespace and names.
var liveByteTexts = []string{
	"",
	"   ",
	"RETURN 1",
	"RETURN 1 // WITH\nWITH 1 AS x RETURN x",
	"MATCH (n) /* a 'quoted' comment */ WHERE n.name = 'it''s // not a comment' RETURN n",
	"RETURN \"a /* b */ c\" AS `x // y`, 'd\\'e' AS z /* tail",
	"RETURN 'unclosed string // still a string",
	"RETURN `unclosed name",
	"RETURN 1 /",
	"RETURN 1 /* unclosed comment",
	"RETURN  n.café // c\r\n+ 1\t",
	"a/b/*c*/d//e\nf'g'h\"i\"j`k`l",
}

// referenceLastLiveByte is lastLiveByte as it was before a long text got an
// index: the walk of the whole prefix, for every lookup.
func referenceLastLiveByte(text string, position int) int {
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

// Every position's answer is the same from the reference walk, from the
// direct read and from the text's index.
func TestLiveTextIndexAgreesWithTheReferenceWalk(t *testing.T) {
	for _, text := range liveByteTexts {
		index := newLiveTextIndex(text)
		require.Len(t, index.last, len(text)+1)
		for position := -2; position <= len(text)+2; position++ {
			clamped := min(max(position, 0), len(text))
			reference := referenceLastLiveByte(text, position)
			require.Equal(t, reference, lastLiveByte(text, position), "direct read of %q at %d", text, position)
			require.Equal(t, reference, int(index.last[clamped]), "index of %q at %d", text, position)
		}
	}
}

// A long statement is answered from its index from its second lookup on, and
// the index gives the reference walk's answer at every position.
func TestLastLiveByteOfALongStatementUsesItsIndex(t *testing.T) {
	for _, separator := range []string{"\n", "'\n", " /* c */ ", "`"} {
		var builder strings.Builder
		for builder.Len() < 4*liveTextIndexMinimum {
			for _, text := range liveByteTexts[2:] {
				builder.WriteString(text)
				builder.WriteString(separator)
			}
		}
		text := builder.String()
		require.Nil(t, liveTextIndexFor(text), "a text is read directly at its first lookup")
		index := liveTextIndexFor(text)
		require.NotNil(t, index, "a text has its index from its second lookup")
		require.Same(t, index, liveTextIndexFor(text))
		for position := 0; position <= len(text); position++ {
			reference := referenceLastLiveByte(text, position)
			require.Equal(t, reference, lastLiveByte(text, position), "separator %q at %d", separator, position)
			require.Equal(t, reference, int(index.last[position]), "separator %q at %d", separator, position)
		}
		require.Same(t, index, liveTextIndexFor(text), "the lookups above were answered from the index")
	}
}
