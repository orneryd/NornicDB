package cyphertext

import "strings"

// SplitConstraintEntries keeps every nested group opaque to the REQUIRE
// adapter. Quotes, comments and group boundaries come from the lexical owner;
// a delimiter inside a relationship name never becomes an entry separator.
func SplitConstraintEntries(body string) ([]string, bool) {
	entries := make([]string, 0)
	start := 0
	appendEntry := func(end int) {
		if entry := strings.TrimSpace(body[start:end]); entry != "" {
			entries = append(entries, entry)
		}
	}
	for index := 0; index < len(body); index++ {
		if IsQuote(body[index]) {
			end, closed := ScanQuotedText(body, index)
			if !closed {
				return nil, false
			}
			index = end - 1
			continue
		}
		if end := CommentEnd(body, index); end >= 0 {
			index = end - 1
			continue
		}
		switch character := body[index]; character {
		case '(', '[', '{':
			closer := rune(')')
			if character == '[' {
				closer = ']'
			} else if character == '{' {
				closer = '}'
			}
			end := FindMatchingDelimiter(body, index, rune(character), closer)
			if end < 0 {
				return nil, false
			}
			index = end
		case '}':
			return nil, false
		case '\n', ';':
			appendEntry(index)
			start = index + 1
		}
	}
	appendEntry(len(body))
	return entries, true
}

// ConstraintPattern is a compiled admission plan for the existing constraint
// protocol. Its names remain private: storage can match values, never turn a
// decoded name back into syntax. Compilation reuses the executor's connection,
// identifier, label-chain and relationship-type phases, including cold reads.
type ConstraintPattern struct {
	incoming     bool
	typeFact     RelationshipType
	targetLabels []string
}

func (pattern ConstraintPattern) Direction() string {
	if pattern.incoming {
		return "INCOMING"
	}
	return "OUTGOING"
}
func (pattern ConstraintPattern) MatchesType(name string) bool {
	return pattern.typeFact.StorageName() != "" && pattern.typeFact.MatchesStorageName(name)
}
func (pattern ConstraintPattern) TargetLabelCount() int { return len(pattern.targetLabels) }
func (pattern ConstraintPattern) MatchesTargetLabels(labels []string) bool {
	for _, wanted := range pattern.targetLabels {
		found := false
		for _, actual := range labels {
			if actual == wanted {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// CompileConstraintPattern admits one unnamed, directed, literal-type hop.
// The subset restrictions belong to this protocol; the owner phases below
// also serve ordinary executor parsing, with no generated-tree allocation or
// memoization needed for this restricted admission path.
func CompileConstraintPattern(text string) (ConstraintPattern, bool) {
	from := GapEnd(text, 0)
	if from >= len(text) || text[from] != '(' {
		return ConstraintPattern{}, false
	}
	_, fromEnd, ok := constraintNodeLabels(text, from+1, false)
	if !ok {
		return ConstraintPattern{}, false
	}
	var typeFact RelationshipType
	frame, ok := readConnectionFrame(text, fromEnd+1, nil, func(text string, open int) int {
		at := spaceEnd(text, open+1)
		if at >= len(text) || text[at] != ':' {
			return -1
		}
		var end int
		var valid bool
		typeFact, end, valid = readLiteralType(text, spaceEnd(text, at+1))
		end = spaceEnd(text, end)
		if !valid || end >= len(text) || text[end] != ']' {
			return -1
		}
		return end
	})
	if !ok || !frame.complete || !frame.bracketed || frame.incoming == frame.outgoing {
		return ConstraintPattern{}, false
	}
	to := frame.end
	if to >= len(text) || text[to] != '(' {
		return ConstraintPattern{}, false
	}
	labels, toEnd, ok := constraintNodeLabels(text, to+1, true)
	if !ok || GapEnd(text, toEnd+1) != len(text) {
		return ConstraintPattern{}, false
	}
	return ConstraintPattern{incoming: frame.incoming, typeFact: typeFact, targetLabels: labels}, true
}

func constraintNodeLabels(text string, at int, retain bool) ([]string, int, bool) {
	// This protocol accepts only a variable and literal labels. Consume the
	// shared name/chain phases in order, rather than finding ')' then splitting
	// and validating the same node head again. Comments inside a node head
	// remain outside the existing constraint protocol.
	at = spaceEnd(text, at)
	if at < len(text) && text[at] != ':' && text[at] != ')' {
		_, end, ok := ScanIdentifier(text, at)
		if !ok {
			return nil, at, false
		}
		at = spaceEnd(text, end)
	}
	var labels []string
	for at < len(text) && text[at] == ':' {
		item, end, ok := readChainItem(text, at+1, ')')
		if !ok || item.dynamic || item.text == "" || !item.quoted && !item.identifier {
			return nil, at, false
		}
		if retain {
			labels = append(labels, item.text)
		}
		at = end
	}
	return labels, at, at < len(text) && text[at] == ')'
}
