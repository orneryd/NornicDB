package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
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
// unquoted (eachChainItem). A dynamic label $(expr) is visited as its text,
// an unquoted name the label rules reject: only SET and REMOVE take them
// (setLabelChainItems).
func eachChainLabel(chain string, visit func(name string, quoted bool)) bool {
	return eachChainItem(chain, func(text string, quoted, dynamic bool) {
		if dynamic {
			visit("$("+text+")", false)
			return
		}
		visit(text, quoted)
	})
}

// eachChainItem calls visit for every item of a label chain, in order: a
// label name (backtick labels unquoted, quoted true) or the expression of a
// dynamic label $(expr) (dynamic true). An empty segment ("A::B", a trailing
// ':') is visited as an empty unquoted name. It returns false when the chain
// is malformed: a backtick or $( left open, or text other than spaces between
// a quoted label or a $(expr) and the next ':'. Names are slices of chain
// unless a doubled backtick has to be unescaped, so scanning allocates
// nothing for ordinary labels.
func eachChainItem(chain string, visit func(text string, quoted, dynamic bool)) bool {
	return cyphertext.EachChainItem(chain, visit)
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

// labelChainItem is one item of a SET or REMOVE label chain: a label name,
// or the expression of a dynamic label $(expr) (dynamicLabelNames reads its
// value).
type labelChainItem struct {
	name       string
	expression string
}

// setLabelChainItems parses and validates the label part of a SET
// n:L1:`L 2`:$(expr) or REMOVE n:L1:$(expr) item into its items, in order.
// An empty chain, an empty $(), or an invalid unquoted label is an error.
func setLabelChainItems(chain string) ([]labelChainItem, error) {
	items := make([]labelChainItem, 0, strings.Count(chain, ":")+1)
	var labelErr error
	wellFormed := eachChainItem(chain, func(text string, quoted, dynamic bool) {
		switch {
		case dynamic:
			items = append(items, labelChainItem{expression: text})
			if text == "" && labelErr == nil {
				labelErr = localizedError(localization.CypherMutationsInvalidLabelName("$()"), nil)
			}
			return
		case text == "" && !quoted:
			return
		case text != "":
			items = append(items, labelChainItem{name: text})
		}
		if labelErr == nil {
			labelErr = validateChainLabel(text, quoted)
		}
	})
	if labelErr == nil && (!wellFormed || len(items) == 0) {
		labelErr = localizedError(localization.CypherMutationsInvalidLabelName(chain), nil)
	}
	if labelErr != nil {
		return nil, &classifiedCypherError{
			cause:  labelErr,
			code:   "Neo.ClientError.Statement.SyntaxError",
			detail: "InvalidLabel",
		}
	}
	return items, nil
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
func splitNodeHead(head string) (string, string, bool) { return cyphertext.SplitNodeHead(head, false) }

// indexByteOutsideBackticks returns the index of the first b in s that is not
// inside a backtick-quoted name, or -1.
func indexByteOutsideBackticks(s string, b byte) int {
	return cyphertext.IndexByteOutsideBackticks(s, b)
}

// strictNodeHeadLabels parses a node head for fast paths that decline
// anything unusual: every label must be non-empty (no "n::A" or "n:") and
// the chain well formed. Quoted labels are unquoted.
func strictNodeHeadLabels(head string) (string, []string, bool) {
	return cyphertext.StrictNodeHeadLabels(head, false)
}

// scanQuotedName reads a backtick-quoted name starting at s[start] == '`'
// and returns it unquoted (a doubled backtick is one backtick) with the index
// after the closing backtick. The name is a slice of s unless it contains a
// doubled backtick.
func scanQuotedName(s string, start int) (string, int, bool) {
	return cyphertext.ScanQuotedName(s, start)
}
