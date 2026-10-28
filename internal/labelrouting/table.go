// Package labelrouting owns normalized label assignment and alias selection.
// Callers retain their existing synchronization and ownership of alias slices.
package labelrouting

import "github.com/orneryd/nornicdb/internal/cyphertext"

// Table retains the existing label map representation and slice identity.
type Table map[string][]string

// New initializes a label table. Assignment to an uninitialized table still panics.
func New() Table { return make(Table) }

// Set assigns the original slice under its canonical decoded-name key.
func (t Table) Set(label string, aliases []string) { t[cyphertext.NameLower(label)] = aliases }

// Lookup returns the configured slice without copying it.
func (t Table) Lookup(label string) ([]string, bool) {
	aliases, exists := t[cyphertext.NameLower(label)]
	return aliases, exists
}

// SelectionKind distinguishes full-scan reasons from known empty mappings.
type SelectionKind uint8

const (
	SelectionNoLabels SelectionKind = iota
	SelectionUnknownLabel
	SelectionEmptyMapping
	SelectionMapped
)

// Complete reports that every supplied label has a configured rule.
func (kind SelectionKind) Complete() bool {
	return kind == SelectionEmptyMapping || kind == SelectionMapped
}

// Selection contains an unsorted alias union; a complete empty union is non-nil.
type Selection struct {
	Kind    SelectionKind
	Aliases []string
}

// Select collects the original union once. The caller decides whether a known
// empty mapping falls through to properties or is a complete empty answer.
func (t Table) Select(labels []string) Selection {
	if len(labels) == 0 {
		return Selection{Kind: SelectionNoLabels}
	}
	set := make(map[string]bool)
	for _, label := range labels {
		aliases, exists := t.Lookup(label)
		if !exists {
			return Selection{Kind: SelectionUnknownLabel}
		}
		for _, alias := range aliases {
			set[alias] = true
		}
	}
	aliases := make([]string, 0, len(set))
	for alias := range set {
		aliases = append(aliases, alias)
	}
	kind := SelectionMapped
	if len(aliases) == 0 {
		kind = SelectionEmptyMapping
	}
	return Selection{Kind: kind, Aliases: aliases}
}
