package cyphertext

import "strings"

// IdentifierReferencePolicy preserves each caller's reference-match contract.
type IdentifierReferencePolicy uint8

const (
	// IdentifierExact searches exact spelling, including qualified references.
	IdentifierExact IdentifierReferencePolicy = iota
	// IdentifierUnqualifiedFold trims supplied name wrappers, folds case and
	// excludes references adjacent to a property separator.
	IdentifierUnqualifiedFold
	// IdentifierFold searches case-insensitively, including qualified references.
	IdentifierFold
	// IdentifierFoldAt checks only the supplied offset; its caller owns lexical
	// traversal, so this mode does not skip quoted tokens itself.
	IdentifierFoldAt
)

// FindIdentifier returns the byte offset of a reference at or after from, or -1.
// Search modes skip quotes/comments; IdentifierFoldAt checks just from. Empty
// names never denote a reference. For example, n.x contains n under
// IdentifierExact but not IdentifierUnqualifiedFold.
func FindIdentifier(text, name string, from int, policy IdentifierReferencePolicy) int {
	if policy == IdentifierUnqualifiedFold {
		name = strings.TrimSpace(strings.Trim(name, "`"))
	}
	if name == "" || from < 0 || from > len(text) {
		return -1
	}
	for i := from; i+len(name) <= len(text); i++ {
		if policy != IdentifierFoldAt {
			if end, quoted := SkipToken(text, i); quoted {
				i = end - 1
				continue
			}
		}
		end := i + len(name)
		candidate := text[i:end]
		matched := candidate == name
		if policy != IdentifierExact {
			matched = strings.EqualFold(candidate, name)
		}
		matched = matched && (i == 0 || !NamePartAt(text, i-1) && (policy != IdentifierUnqualifiedFold || text[i-1] != '.'))
		matched = matched && (end == len(text) || !NamePartAt(text, end) && (policy != IdentifierUnqualifiedFold || text[end] != '.'))
		if matched {
			return i
		}
		if policy == IdentifierFoldAt {
			return -1
		}
	}
	return -1
}
