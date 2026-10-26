package cyphertext

import (
	"fmt"
	"sort"
)

// EntryDeclaration is a producer's emitted representation of an original
// entry. Original must come from the retained lexical record. The producer
// supplies boundaries and separator identities; no emitted text is parsed.
type EntryDeclaration struct {
	Original   BraceEntry
	Entry      BraceEntry
	Separators EntrySeparators
	First      Span
}

// Shift is emission-offset composition, retaining the original record identity.
func (declaration EntryDeclaration) Shift(by int) EntryDeclaration {
	shift := func(span Span) Span {
		if span.Start == span.End {
			return span
		}
		return Span{span.Start + by, span.End + by}
	}
	declaration.Entry.Span = shift(declaration.Entry.Span)
	declaration.Entry.Name = shift(declaration.Entry.Name)
	declaration.Entry.Value = shift(declaration.Entry.Value)
	declaration.Separators.Before = shift(declaration.Separators.Before)
	declaration.Separators.Colon = shift(declaration.Separators.Colon)
	declaration.Separators.After = shift(declaration.Separators.After)
	declaration.First = shift(declaration.First)
	return declaration
}

func (source *Source) WithEntryDeclarations(declarations []EntryDeclaration) (*Source, error) {
	if len(declarations) == 0 {
		return source, nil
	}
	view := source.Window(0, len(source.Text))
	for _, declaration := range declarations {
		if declaration.Original.original == nil {
			return nil, fmt.Errorf("cyphertext: entry declaration names no original record")
		}
		if _, err := source.CopyRelationsFrom(declaration.Original.original.source); err != nil {
			return nil, err
		}
		valid := func(span Span) bool { return span.Start >= 0 && span.End >= span.Start && span.End <= len(source.Text) }
		if !valid(declaration.Entry.Span) || !valid(declaration.Entry.Name) || !valid(declaration.Entry.Value) ||
			!valid(declaration.First) || declaration.First.End <= declaration.First.Start ||
			!valid(declaration.Separators.Before) || !valid(declaration.Separators.Colon) || !valid(declaration.Separators.After) ||
			declaration.Separators.Before.End != declaration.Entry.Span.Start || declaration.Separators.After.Start != declaration.Entry.Span.End {
			return nil, fmt.Errorf("cyphertext: inconsistent emitted entry boundaries")
		}
		declaration.Entry.original = declaration.Original.original
		declaration.Entry.separators = declaration.Separators
		view.entryDeclarations = append(view.entryDeclarations, declaration)
	}
	return view, nil
}

// OriginalEntry returns the immutable original record named by a projected
// entry. Expression semantics remain in the expression owner.
func (entry BraceEntry) OriginalEntry() (BraceEntry, bool) {
	if entry.original == nil {
		return BraceEntry{}, false
	}
	original := entry.original.entry
	original.original = entry.original
	return original, true
}

// BraceRecord returns the immutable containing-brace identity of an original
// entry. It requests no projected extent. Author: #1663 implementation lane.
func (entry BraceEntry) BraceRecord() (BraceRecord, bool) {
	if entry.original == nil {
		return BraceRecord{}, false
	}
	return entry.original.source.BraceFacts().At(entry.original.braceOpen)
}

// OriginalEntryAt reads only the identity of the copied/declaration-bound name
// token. Consumers asking for entry/value boundaries use EntryAt/ProjectEntry,
// whose separator requirements are unchanged. Author: #1663 lane.
func (source *Source) OriginalEntryAt(name Span) (BraceEntry, bool) {
	for stage := source; stage != nil; stage = stage.parent {
		for _, declaration := range stage.entryDeclarations {
			for _, copied := range source.ProjectFrom(stage, declaration.Entry.Name) {
				if copied == name {
					return declaration.Original, true
				}
			}
		}
	}
	return source.BraceFacts().EntryAtView(source, name)
}

// OriginalEntryAtColon selects only a separator token's original entry identity.
// It makes no entry/value extent claim: EntryAt/ProjectEntry/EntryColonAt retain
// their strict projection contracts. Generated tokens require a producer's
// declaration to name an original record. Author: #1663 implementation lane.
func (source *Source) OriginalEntryAtColon(colon Span) (BraceEntry, bool) {
	if colon.End <= colon.Start {
		return BraceEntry{}, false
	}
	for stage := source; stage != nil; stage = stage.parent {
		for _, declaration := range stage.entryDeclarations {
			if declaration.Separators.Colon.End <= declaration.Separators.Colon.Start {
				continue
			}
			for _, copied := range source.ProjectFrom(stage, declaration.Separators.Colon) {
				if copied == colon {
					return declaration.Original, true
				}
			}
		}
	}
	facts := source.BraceFacts()
	for _, record := range facts.braces {
		for _, entry := range record.entries {
			if entry.separators.Colon.End <= entry.separators.Colon.Start {
				continue
			}
			for _, copied := range source.ProjectFrom(facts.source, entry.separators.Colon) {
				if copied == colon {
					return entry, true
				}
			}
		}
	}
	return BraceEntry{}, false
}

// EntryAt is the Source-bound span API for expression consumers. It reads a
// retained original record or a typed producer declaration, never entry text.
func (source *Source) EntryAt(name Span) (BraceEntry, bool, error) {
	for stage := source; stage != nil; stage = stage.parent {
		for _, declaration := range stage.entryDeclarations {
			entries, err := source.ProjectEntry(declaration.Original)
			if err != nil {
				return BraceEntry{}, false, err
			}
			for _, entry := range entries {
				if entry.Name == name {
					return entry, true, nil
				}
			}
		}
	}
	original, found := source.BraceFacts().EntryAtView(source, name)
	if !found {
		return BraceEntry{}, false, nil
	}
	entries, err := source.ProjectEntry(original)
	if err != nil {
		return BraceEntry{}, false, err
	}
	for _, entry := range entries {
		if entry.Name == name {
			return entry, true, nil
		}
	}
	return BraceEntry{}, false, nil
}

// EntryColonAt reads the retained separator identity in this view, including
// a producer's declared replacement. It does not infer entry grammar from
// text. Lexical consumers may use it before upper statement-role admission.
// Author: #1663 implementation lane.
func (source *Source) EntryColonAt(offset int) (bool, error) {
	for _, record := range source.BraceFacts().Braces() {
		for _, original := range record.Entries() {
			if original.Kind != KeyedBraceEntry {
				continue
			}
			entries, err := source.ProjectEntry(original)
			if err != nil {
				return false, err
			}
			for _, entry := range entries {
				colon := entry.SeparatorSpans().Colon
				if colon.Start == offset && colon.End > colon.Start {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

// ProjectEntry projects the record's separator tokens inside the Source
// owner. A removed entry has no first-token occurrence. A kept entry whose
// separator was removed requires a producer declaration and otherwise errors.
func (source *Source) ProjectEntry(entry BraceEntry) ([]BraceEntry, error) {
	if entry.original == nil {
		return nil, fmt.Errorf("cyphertext: entry projection names no original record")
	}
	for stage := source; stage != nil; stage = stage.parent {
		var declared []EntryDeclaration
		for _, declaration := range stage.entryDeclarations {
			if declaration.Original.original == entry.original {
				declared = append(declared, declaration)
			}
		}
		if len(declared) == 0 {
			continue
		}
		var result []BraceEntry
		for _, declaration := range declared {
			projected, err := source.projectEntryRecord(stage, declaration.Entry, declaration.Separators, declaration.First)
			if err != nil {
				return nil, err
			}
			result = append(result, projected...)
		}
		sort.Slice(result, func(i, j int) bool { return result[i].Span.Start < result[j].Span.Start })
		return result, nil
	}
	original := entry.original
	entry = original.entry
	entry.original = original
	return source.projectEntryRecord(original.source, entry, original.separators, original.first)
}

func (source *Source) projectEntryRecord(stage *Source, entry BraceEntry, separators EntrySeparators, first Span) ([]BraceEntry, error) {
	copies, err := source.CopyRelationsFrom(stage)
	if err != nil {
		return nil, err
	}
	positions := func(token Span) []Span {
		if token.Start == token.End {
			return nil
		}
		var spans []Span
		for _, copy := range copies {
			if copy.Original.Start <= token.Start && token.End <= copy.Original.End {
				spans = append(spans, Span{Start: copy.Accepted.Start + token.Start - copy.Original.Start, End: copy.Accepted.Start + token.End - copy.Original.Start})
			}
		}
		return spans
	}
	firsts := positions(first)
	before, after, colon, names := positions(separators.Before), positions(separators.After), positions(separators.Colon), positions(entry.Name)
	if len(firsts) == 0 {
		// A generated name between the retained separator pair is an emitted
		// entry, not removal of the original record. Its producer must bind
		// that representation explicitly; no output grammar is consulted.
		for _, opening := range before {
			for _, closing := range after {
				if opening.End < closing.Start {
					return nil, fmt.Errorf("cyphertext: entry content changed between retained separators without a declaration")
				}
			}
		}
		return nil, nil
	}
	if len(before) != len(firsts) || len(after) != len(firsts) || separators.Colon.End > separators.Colon.Start && len(colon) != len(firsts) || entry.Name.End > entry.Name.Start && len(names) != len(firsts) {
		return nil, fmt.Errorf("cyphertext: kept entry separator/name was removed or generated without a declaration")
	}
	result := make([]BraceEntry, 0, len(firsts))
	for index, first := range firsts {
		if before[index].End > first.Start || first.End > after[index].Start {
			return nil, fmt.Errorf("cyphertext: copied entry endpoints are not an occurrence")
		}
		projected := entry
		projected.Span = Span{Start: before[index].End, End: after[index].Start}
		projected.separators = EntrySeparators{Before: before[index], After: after[index]}
		if len(names) > 0 {
			projected.Name = names[index]
		}
		if len(colon) > 0 {
			projected.separators.Colon = colon[index]
			if colon[index].Start < first.End || colon[index].End > after[index].Start {
				return nil, fmt.Errorf("cyphertext: copied entry colon is not in its occurrence")
			}
			projected.Value = Span{Start: colon[index].End, End: after[index].Start}
		}
		result = append(result, projected)
	}
	return result, nil
}
