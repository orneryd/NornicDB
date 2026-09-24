package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// Label chains (n:L1:`L 2`) are parsed by one scanner for every clause that
// names labels: node patterns in MATCH / CREATE / MERGE / OPTIONAL MATCH and
// the fast paths, SET and REMOVE label items, and WHERE label predicates.
// A backtick-quoted label may contain any character, including ':' and '{';
// a doubled backtick inside it is one backtick. Unquoted labels must be
// identifiers and not reserved words (validateChainLabel).

// eachChainLabel calls visit for every label of a label chain (the text after
// the variable's first colon, "L1:`L 2`"), in order, with backtick labels
// unquoted. An empty segment ("A::B", a trailing ':') is visited as an empty
// unquoted name. It returns false when the chain is malformed: a backtick left
// open, or text other than spaces between a quoted label and the next ':'.
// Names are slices of chain unless a doubled backtick has to be unescaped, so
// scanning allocates nothing for ordinary labels.
func eachChainLabel(chain string, visit func(name string, quoted bool)) bool {
	for start := 0; ; {
		index := start
		for index < len(chain) && isASCIISpace(chain[index]) {
			index++
		}
		if index < len(chain) && chain[index] == '`' {
			name, end, ok := scanQuotedName(chain, index)
			if !ok {
				return false
			}
			for end < len(chain) && isASCIISpace(chain[end]) {
				end++
			}
			if end < len(chain) && chain[end] != ':' {
				return false
			}
			visit(name, true)
			if end >= len(chain) {
				return true
			}
			start = end + 1
			continue
		}
		end := strings.IndexByte(chain[start:], ':')
		if end < 0 {
			end = len(chain)
		} else {
			end += start
		}
		segment := chain[start:end]
		if strings.IndexByte(segment, '`') >= 0 {
			return false
		}
		visit(strings.TrimSpace(segment), false)
		if end >= len(chain) {
			return true
		}
		start = end + 1
	}
}

// validateChainLabel applies the label rules to one label: a quoted label is
// not empty; an unquoted label is an identifier and not a reserved word.
func validateChainLabel(name string, quoted bool) error {
	if name == "" || (!quoted && !isValidIdentifier(name)) {
		return localizedError(localization.CypherMutationsInvalidLabelName(name), nil)
	}
	if !quoted && containsReservedKeyword(name) {
		return localizedError(localization.CypherMutationsInvalidLabelReserved(name), nil)
	}
	return nil
}

// parseLabelChain returns the label names of a chain (empty unquoted segments
// skipped) and the label-rule error, if any: a malformed chain, a chain
// without labels, or a label that breaks validateChainLabel.
func parseLabelChain(chain string) ([]string, error) {
	names := make([]string, 0, strings.Count(chain, ":")+1)
	var labelErr error
	wellFormed := eachChainLabel(chain, func(name string, quoted bool) {
		if name == "" && !quoted {
			return
		}
		if name != "" {
			names = append(names, name)
		}
		if labelErr == nil {
			labelErr = validateChainLabel(name, quoted)
		}
	})
	if labelErr == nil && (!wellFormed || len(names) == 0) {
		labelErr = localizedError(localization.CypherMutationsInvalidLabelName(chain), nil)
	}
	return names, labelErr
}

// setLabelChain parses and validates the label part of a SET n:L1:`L 2` or
// REMOVE n:L1 item into label names. An empty chain or an invalid unquoted
// label is an error.
func setLabelChain(chain string) ([]string, error) {
	names, err := parseLabelChain(chain)
	if err != nil {
		return nil, err
	}
	return names, nil
}

// labelChainNames returns the label names of a chain without validating them,
// for readers (MATCH patterns, fast paths, REMOVE, WHERE label tests) whose
// statements are validated elsewhere.
func labelChainNames(chain string) []string {
	names, _ := parseLabelChain(chain)
	return names
}

// splitNodeHead splits the part of a node pattern before its property map,
// "n:L1:`L 2`", into the variable and the label chain at the first colon
// outside backticks. hasLabels reports whether there was a colon.
func splitNodeHead(head string) (variable, chain string, hasLabels bool) {
	colon := indexByteOutsideBackticks(head, ':')
	if colon < 0 {
		return strings.TrimSpace(head), "", false
	}
	return strings.TrimSpace(head[:colon]), head[colon+1:], true
}

// indexByteOutsideBackticks returns the index of the first b in s that is not
// inside a backtick-quoted name, or -1.
func indexByteOutsideBackticks(s string, b byte) int {
	inBacktick := false
	for index := 0; index < len(s); index++ {
		switch s[index] {
		case '`':
			inBacktick = !inBacktick // a doubled backtick toggles twice
		case b:
			if !inBacktick {
				return index
			}
		}
	}
	return -1
}

// strictNodeHeadLabels parses a node head for fast paths that decline
// anything unusual: every label must be non-empty (no "n::A" or "n:") and
// the chain well formed. Quoted labels are unquoted.
func strictNodeHeadLabels(head string) (variable string, labels []string, ok bool) {
	variable, chain, hasLabels := splitNodeHead(head)
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

// scanQuotedName reads a backtick-quoted name starting at s[start] == '`'
// and returns it unquoted (a doubled backtick is one backtick) with the index
// after the closing backtick. The name is a slice of s unless it contains a
// doubled backtick.
func scanQuotedName(s string, start int) (name string, end int, ok bool) {
	var unescaped *strings.Builder
	segment := start + 1
	for index := start + 1; index < len(s); index++ {
		if s[index] != '`' {
			continue
		}
		if index+1 < len(s) && s[index+1] == '`' {
			if unescaped == nil {
				unescaped = &strings.Builder{}
			}
			unescaped.WriteString(s[segment : index+1])
			index++
			segment = index + 1
			continue
		}
		if unescaped == nil {
			return s[start+1 : index], index + 1, true
		}
		unescaped.WriteString(s[segment:index])
		return unescaped.String(), index + 1, true
	}
	return "", start, false
}
