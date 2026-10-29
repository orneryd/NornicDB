package cyphertext

import "strings"

// These are the existing executor label-chain and node-head phases, moved
// below the storage import boundary. All callers share their written-token
// classification; protocol admission is separate from scanning.

func EachChainItem(chain string, visit func(text string, quoted, dynamic bool)) bool {
	for start := 0; ; {
		item, end, ok := readChainItem(chain, start, 0)
		if !ok {
			return false
		}
		visit(item.text, item.quoted, item.dynamic)
		if end >= len(chain) {
			return true
		}
		start = end + 1
	}
}

// readChainItem is the written-token phase for both a detached label chain
// and a node head still inside its enclosing pattern. end points at the next
// colon, the enclosing stop byte, or EOF; quoted/dynamic contents stay opaque.
// Protocol consumers can decline an item without a callback or a second walk
// to locate the enclosing node's closing parenthesis.
type chainItem struct {
	text            string
	quoted, dynamic bool

	identifier bool
}

func readChainItem(chain string, start int, stop byte) (chainItem, int, bool) {
	index := start
	index = spaceEnd(chain, index)
	if index+1 < len(chain) && chain[index] == '$' && chain[index+1] == '(' {
		closing := FindMatchingDelimiter(chain, index+1, '(', ')', OpeningDelimiter)
		if closing < 0 {
			return chainItem{}, start, false
		}
		end := closing + 1
		end = spaceEnd(chain, end)
		if end < len(chain) && chain[end] != ':' && (stop == 0 || chain[end] != stop) {
			return chainItem{}, start, false
		}
		return chainItem{text: strings.TrimSpace(chain[index+2 : closing]), dynamic: true}, end, true
	}
	if index < len(chain) && IsBacktick(chain[index]) {
		name, end, ok := ScanQuotedName(chain, index)
		if !ok {
			return chainItem{}, start, false
		}
		end = spaceEnd(chain, end)
		if end < len(chain) && chain[end] != ':' && (stop == 0 || chain[end] != stop) {
			return chainItem{}, start, false
		}
		return chainItem{text: name, quoted: true}, end, true
	}
	// The name reader already proved the common plain item. Preserve that
	// proof for strict consumers rather than finding and validating it again.
	if name, ok := ScanIdentifier(chain, index); ok {
		end := spaceEnd(chain, name.End)
		if end == len(chain) || chain[end] == ':' || stop != 0 && chain[end] == stop {
			return chainItem{text: name.Value, identifier: true}, end, true
		}
	}
	end := strings.IndexByte(chain[start:], ':')
	if end < 0 {
		end = len(chain)
	} else {
		end += start
	}
	if stop != 0 {
		if closing := strings.IndexByte(chain[start:end], stop); closing >= 0 {
			end = start + closing
		}
	}
	segment := chain[start:end]
	if strings.IndexByte(segment, '`') >= 0 {
		return chainItem{}, start, false
	}
	name := strings.TrimSpace(segment)
	return chainItem{text: name, identifier: IsIdentifier(name)}, end, true
}

func SplitNodeHead(head string, quotedValues bool) (variable, chain string, hasLabels bool) {
	colon := -1
	if quotedValues {
		colon = IndexByteOutsideQuotedText(head, ':')
	} else {
		colon = IndexByteOutsideBackticks(head, ':')
	}
	if colon < 0 {
		return strings.TrimSpace(head), "", false
	}
	return strings.TrimSpace(head[:colon]), head[colon+1:], true
}

func IndexByteOutsideBackticks(s string, b byte) int {
	for index := 0; index < len(s); index++ {
		if IsBacktick(s[index]) {
			end, _ := ScanQuotedText(s, index)
			index = end - 1
			continue
		}
		if s[index] == b {
			return index
		}
	}
	return -1
}

func StrictNodeHeadLabels(head string, quotedValues bool) (variable string, labels []string, ok bool) {
	variable, chain, hasLabels := SplitNodeHead(head, quotedValues)
	if !hasLabels {
		return variable, []string{}, true
	}
	labels = make([]string, 0, strings.Count(chain, ":")+1)
	ok = true
	wellFormed := eachChainLabel(chain, func(name string, quoted bool) {
		if name == "" {
			ok = false
			return
		}
		labels = append(labels, name)
	})
	if !wellFormed || !ok {
		return "", nil, false
	}
	return variable, labels, true
}

func ScanQuotedName(s string, start int) (name string, end int, ok bool) {
	if start < 0 || start >= len(s) || !IsBacktick(s[start]) {
		return "", start, false
	}
	token, valid := ScanIdentifier(s, start)
	return token.Value, token.End, valid
}

func eachChainLabel(chain string, visit func(string, bool)) bool {
	return EachChainItem(chain, func(text string, quoted, dynamic bool) {
		if dynamic {
			visit("$("+text+")", false)
			return
		}
		visit(text, quoted)
	})
}

func IndexByteOutsideQuotedText(s string, b byte) int {
	for index := 0; index < len(s); index++ {
		if end, quoted := ScanQuotedText(s, index); quoted {
			index = end - 1
			continue
		}
		if s[index] == b {
			return index
		}
	}
	return -1
}
