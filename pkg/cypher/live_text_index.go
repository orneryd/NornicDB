package cypher

import (
	"sync"
	"unsafe"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// liveStep reads the lexical item that starts at index: a comment, a quoted
// span or one byte. It returns where the next item starts, the last live byte
// seen from a position inside the item, and the last live byte seen from the
// positions after it. live is the last live byte before index.
//
// A comment is not live. A quoted span's quote bytes are live: its opening
// quote from inside the span, its closing quote after it (the opening quote
// again when the span is not closed). Whitespace is not live.
func liveStep(text string, index, live int) (next, inside, after int) {
	if end := cyphertext.CommentEnd(text, index); end >= 0 {
		return end, live, live
	}
	if cyphertext.IsQuote(text[index]) {
		end, closed := cyphertext.ScanQuotedText(text, index)
		if closed {
			return end, index, end - 1
		}
		return end, index, index
	}
	if cyphertext.IsWhitespaceAt(text, index) {
		return index + 1, live, live
	}
	return index + 1, index, index
}

// liveTextIndex holds lastLiveByte's answer for every position of one text.
// Backward word lookups ask it once per word of a statement; reading the
// statement from its start for each of them made a long statement's rewrites
// and validations quadratic in its length (NOR-407).
type liveTextIndex struct {
	text string
	last []int32
}

func newLiveTextIndex(text string) *liveTextIndex {
	last := make([]int32, len(text)+1)
	live := -1
	for index := 0; index < len(text); {
		last[index] = int32(live)
		next, inside, after := liveStep(text, index, live)
		for position := index + 1; position < next && position <= len(text); position++ {
			last[position] = int32(inside)
		}
		index, live = next, after
	}
	last[len(text)] = int32(live)
	return &liveTextIndex{text: text, last: last}
}

const (
	// liveTextIndexMinimum is the position from which a lookup uses the
	// text's index; a shorter prefix is read directly.
	liveTextIndexMinimum = 1024
	// liveTextIndexMaximum is the longest text that gets an index: its table
	// is four bytes per text byte.
	liveTextIndexMaximum = 8 << 20
	liveTextIndexSlots   = 8
)

// liveTextIndexes keeps the indexes of the texts being read now. A text is
// recognised by its identity (the same bytes at the same address), so a
// lookup never compares or hashes statement text. A text gets its index at
// its second lookup: one that is read once is read directly, as before.
var liveTextIndexes struct {
	sync.Mutex
	slots [liveTextIndexSlots]*liveTextIndex
	next  int
}

// liveTextIndexFor returns text's index, or nil at the first lookup of text.
func liveTextIndexFor(text string) *liveTextIndex {
	data := unsafe.StringData(text)
	cache := &liveTextIndexes
	cache.Lock()
	for slot, index := range cache.slots {
		if index == nil || len(index.text) != len(text) || unsafe.StringData(index.text) != data {
			continue
		}
		if index.last != nil {
			cache.Unlock()
			return index
		}
		cache.Unlock()
		built := newLiveTextIndex(text)
		cache.Lock()
		if cache.slots[slot] == index {
			cache.slots[slot] = built
		}
		cache.Unlock()
		return built
	}
	cache.slots[cache.next] = &liveTextIndex{text: text}
	cache.next = (cache.next + 1) % liveTextIndexSlots
	cache.Unlock()
	return nil
}
