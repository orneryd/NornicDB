package cyphertext

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// sourceNavigation is immutable byte navigation for one source window. It uses
// the same quote/comment/whitespace owners as normalization and grammar input.
// Positions inside a quote name its opening byte; comments contribute no byte.
type sourceNavigation struct {
	text     string
	previous []int
}
type navigationKey struct {
	start  *byte
	length int
}

var recentNavigation atomic.Pointer[sourceNavigation]
var sourceNavigations = struct {
	sync.Mutex
	entries map[navigationKey]*sourceNavigation
	order   []navigationKey
}{entries: make(map[navigationKey]*sourceNavigation)}

// PreviousLiveByte returns the live predecessor at an original byte offset.
// Identity keys avoid hashing an entire long source for every token lookup.
// Cached windows keep their immutable strings alive, so an identity cannot be
// reused while its navigation entry is held. Eviction changes only reuse.
func PreviousLiveByte(text string, position int) int {
	if position <= 0 || len(text) == 0 {
		return -1
	}
	if position > len(text) {
		position = len(text)
	}
	key := navigationKey{unsafe.StringData(text), len(text)}
	if navigation := recentNavigation.Load(); navigation != nil && unsafe.StringData(navigation.text) == key.start && len(navigation.text) == key.length {
		return navigation.previous[position]
	}
	sourceNavigations.Lock()
	navigation := sourceNavigations.entries[key]
	if navigation == nil {
		navigation = buildSourceNavigation(text)
		if len(sourceNavigations.order) == 64 {
			old := sourceNavigations.order[0]
			delete(sourceNavigations.entries, old)
			copy(sourceNavigations.order, sourceNavigations.order[1:])
			sourceNavigations.order = sourceNavigations.order[:63]
		}
		sourceNavigations.entries[key] = navigation
		sourceNavigations.order = append(sourceNavigations.order, key)
	}
	sourceNavigations.Unlock()
	recentNavigation.Store(navigation)
	return navigation.previous[position]
}

func buildSourceNavigation(text string) *sourceNavigation {
	previous := make([]int, len(text)+1)
	previous[0] = -1
	last := -1
	for index := 0; index < len(text); {
		if end := CommentEnd(text, index); end >= 0 {
			for at := index + 1; at <= end; at++ {
				previous[at] = last
			}
			index = end
			continue
		}
		if IsQuote(text[index]) {
			end, closed := ScanQuotedText(text, index)
			for at := index + 1; at <= end; at++ {
				previous[at] = index
			}
			last = index
			if closed {
				last = end - 1
				previous[end] = last
			}
			index = end
			continue
		}
		if !IsWhitespaceAt(text, index) {
			last = index
		}
		previous[index+1] = last
		index++
	}
	return &sourceNavigation{text: text, previous: previous}
}

// PreviousLiveByte shares the source-window navigation owner with standalone
// lexical admission. Retained consumers pass the source's existing text.
func (source *Source) PreviousLiveByte(position int) int {
	return PreviousLiveByte(source.Text, position)
}
