package cyphertext

// SourceCopy records bytes copied from an immediate parent source view. The
// intervals have equal lengths and identical bytes. Accepted intervals are
// disjoint and ordered; Original intervals may repeat or appear in any order.
// A copy can occur inside a replacement or insertion described by SourceEdit.
type SourceCopy struct{ Original, Accepted Span }

// SourceOrigin describes a source interval's provenance. Exact is false when
// a legacy replacement supplies only an enclosing extent. Generated marks
// text introduced by a rewrite that opted into copy relations. Such text has
// no original token; Original retains the legacy diagnostic anchor only.
type SourceOrigin struct {
	Original         Span
	Exact, Generated bool
}

// Rebase applies this retained view's existing rewrite/window relations to an
// equal accepted-text ancestor from another statement. Cached structural plans
// retain a recipe, not the previous statement's source identity or brace facts.
// The relation is replayed through Window/Rewrite/RewriteWithCopies, so mapping
// and generated-origin semantics have the same owner as ordinary producers.
func (source *Source) Rebase(ancestor, replacement *Source) *Source {
	if ancestor.Text != replacement.Text {
		panic("cyphertext: source recipe requires equal accepted ancestor text")
	}
	if source == ancestor {
		return replacement
	}
	if source.parent == nil {
		panic("cyphertext: source recipe ancestor is not in this lineage")
	}
	parent := source.parent.Rebase(ancestor, replacement)
	var view *Source
	if source.window {
		view = parent.Window(source.base, source.base+len(source.Text))
	} else if source.projected {
		view = parent.RewriteWithCopies(source.Text, source.Edits, source.copies)
	} else {
		view = parent.Rewrite(source.Text, append([]SourceEdit(nil), source.Edits...))
	}
	var bodies []Span
	for _, record := range source.queryBodies {
		bodies = append(bodies, Span{record.Open, record.Close + 1})
	}
	view = view.WithQueryBodies(bodies)
	var declarations []EntryDeclaration
	for _, declaration := range source.entryDeclarations {
		declaration.Original = rebaseEntryIdentity(declaration.Original, ancestor, replacement)
		declarations = append(declarations, declaration)
	}
	declared, err := view.WithEntryDeclarations(declarations)
	if err != nil {
		panic(err)
	}
	return declared
}

// Author: #1663 lane. Rebind the declaration's original record through its
// exact copied or declared name positions in the cache ancestor. Its emitted
// name may be generated and cannot serve as an original-source locator.
func rebaseEntryIdentity(entry BraceEntry, ancestor, replacement *Source) BraceEntry {
	if entry.original == nil {
		panic("cyphertext: cached entry declaration has no original record")
	}
	identity := entry.original
	names := ancestor.ProjectFrom(identity.source, identity.entry.Name)
	for stage := ancestor; stage != nil; stage = stage.parent {
		for _, declaration := range stage.entryDeclarations {
			if declaration.Original.original == identity {
				names = append(names, ancestor.ProjectFrom(stage, declaration.Entry.Name)...)
			}
		}
	}
	var result BraceEntry
	for _, name := range names {
		candidate, found := replacement.OriginalEntryAt(name)
		if !found {
			continue
		}
		if result.original != nil && result.original != candidate.original {
			panic("cyphertext: cached entry declaration has ambiguous original records in replacement source")
		}
		result = candidate
	}
	if result.original == nil {
		panic("cyphertext: cached entry declaration has no original record in replacement source")
	}
	return result
}

// RewriteWithCopies extends Rewrite with exact provenance for copied slices.
// Bytes inside an edit but outside its copies have explicit generated origin.
// Unchanged bytes keep their parent origin. Rewrite callers that do not opt in
// retain their existing extent mapping and diagnostics.
//
// For example, copying parent[7:13] into text[10:16] maps text[11:12] to
// parent[8:9], even when the surrounding replacement covers a whole clause.
// Invalid relations are producer errors and panic before a view is published.
func (source *Source) RewriteWithCopies(text string, edits []SourceEdit, copies []SourceCopy) *Source {
	previous := 0
	for _, copy := range copies {
		if copy.Accepted.Start < previous || copy.Accepted.End <= copy.Accepted.Start ||
			copy.Accepted.End > len(text) || copy.Original.Start < 0 ||
			copy.Original.End > len(source.Text) ||
			copy.Accepted.End-copy.Accepted.Start != copy.Original.End-copy.Original.Start ||
			text[copy.Accepted.Start:copy.Accepted.End] != source.Text[copy.Original.Start:copy.Original.End] {
			panic("cyphertext: invalid source copy relation")
		}
		previous = copy.Accepted.End
	}
	view := source.Rewrite(text, append([]SourceEdit(nil), edits...))
	view.copies, view.projected = append([]SourceCopy(nil), copies...), true
	return view
}

// Origin maps an interval through the canonical source association and reports
// whether it still identifies original text. Token consumers must not treat a
// generated anchor or a legacy replacement extent as an original token span.
// A span crossing independent copy/generated pieces is not an exact interval;
// consumers retain individual token/entry spans instead of inventing a union.
func (source *Source) Origin(span Span) SourceOrigin {
	start, _ := mapSourceOffset(span.Start, true, SourceExtent, len(source.Edits), func(i int) SourceEdit { return source.Edits[i] }, source.copies)
	end := start
	if span.End != span.Start {
		end, _ = mapSourceOffset(span.End, false, SourceExtent, len(source.Edits), func(i int) SourceEdit { return source.Edits[i] }, source.copies)
	}
	origin := SourceOrigin{
		Original:  Span{start.offset + source.base, end.offset + source.base},
		Exact:     !start.replaced && !end.replaced,
		Generated: source.projected && (start.replaced || end.replaced),
	}
	// A single interval must fit within one copy or within unchanged text.
	// Checking intersections prevents matching endpoints from hiding a generated
	// middle or two different copies with a coincidentally equal end offset.
	if span.End > span.Start {
		covered := false
		for _, copy := range source.copies {
			if copy.Accepted.Start <= span.Start && span.End <= copy.Accepted.End {
				covered = true
				break
			}
			if copy.Accepted.Start < span.End && copy.Accepted.End > span.Start {
				origin.Exact = false
			}
		}
		if !covered {
			for _, edit := range source.Edits {
				if edit.Accepted.Start < span.End && edit.Accepted.End > span.Start {
					origin.Exact = false
					origin.Generated = origin.Generated || source.projected
				}
			}
		}
	}
	if source.parent != nil {
		parent := source.parent.Origin(origin.Original)
		origin.Original = parent.Original
		origin.Exact = origin.Exact && parent.Exact
		origin.Generated = origin.Generated || parent.Generated
	}
	return origin
}

// ProjectFrom projects only exact copied bytes from an identity ancestor.
// Generated and legacy-replacement bytes never complete an interval. Brace
// and entry extents use their retained records through ProjectBrace/ProjectEntry.
func (source *Source) ProjectFrom(ancestor *Source, span Span) []Span {
	copies, err := source.CopyRelationsFrom(ancestor)
	if err != nil {
		return nil
	}
	var result []Span
	for index, copy := range copies {
		if span.Start < copy.Original.Start || span.Start >= copy.Original.End {
			continue
		}
		start := copy.Accepted.Start + span.Start - copy.Original.Start
		end := min(span.End, copy.Original.End)
		acceptedEnd := copy.Accepted.Start + end - copy.Original.Start
		for next := index + 1; end < span.End && next < len(copies); next++ {
			part := copies[next]
			if part.Original.Start != end || part.Accepted.Start != acceptedEnd {
				break
			}
			end = min(span.End, part.Original.End)
			acceptedEnd = part.Accepted.Start + end - part.Original.Start
		}
		if end == span.End {
			result = append(result, Span{Start: start, End: acceptedEnd})
		}
	}
	return result
}
