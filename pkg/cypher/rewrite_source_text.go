package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// rewritePredicate carries the producer's explicit copy relations while text
// moves between pattern elements, predicates and the final statement edit.
// Coordinate interpretation remains in cyphertext.Source.
type rewritePredicate struct {
	text        string
	copies      []cyphertext.SourceCopy
	constructed []braceConstruction
	entries     []cyphertext.EntryDeclaration
}

func generatedPredicate(text string) rewritePredicate { return rewritePredicate{text: text} }

func generatedPredicates(texts ...string) []rewritePredicate {
	result := make([]rewritePredicate, 0, len(texts))
	for _, text := range texts {
		result = append(result, generatedPredicate(text))
	}
	return result
}

func copiedPredicate(query string, span cyphertext.Span) rewritePredicate {
	return rewritePredicate{text: query[span.Start:span.End], copies: []cyphertext.SourceCopy{{Original: span, Accepted: cyphertext.Span{Start: 0, End: span.End - span.Start}}}}
}

func joinRewritePredicates(parts []rewritePredicate, separator string) rewritePredicate {
	var text strings.Builder
	var copies []cyphertext.SourceCopy
	var constructed []braceConstruction
	var entries []cyphertext.EntryDeclaration
	for i, part := range parts {
		if i > 0 {
			text.WriteString(separator)
		}
		at := text.Len()
		text.WriteString(part.text)
		for _, copy := range part.copies {
			copy.Accepted.Start += at
			copy.Accepted.End += at
			copies = append(copies, copy)
		}
		for _, construction := range part.constructed {
			constructed = append(constructed, shiftBraceConstruction(construction, at))
		}
		for _, entry := range part.entries {
			entries = append(entries, entry.Shift(at))
		}
	}
	return rewritePredicate{text: text.String(), copies: copies, constructed: constructed, entries: entries}
}

func wrapRewritePredicate(prefix string, value rewritePredicate, suffix string) rewritePredicate {
	return joinRewritePredicates([]rewritePredicate{generatedPredicate(prefix), value, generatedPredicate(suffix)}, "")
}

func (r *labelExpressionRewriter) editPredicate(start, end int, value rewritePredicate) {
	r.edits = append(r.edits, labelRewriteEdit{start: start, end: end, text: value.text, copies: value.copies, constructed: value.constructed, entries: value.entries})
}

// rewrittenSourceRegion records the slices actually emitted by an existing
// region rewrite. Its copy origins remain relative to the whole input query;
// callers compose the result through Source.RewriteWithCopies.
func rewrittenSourceRegion(query string, start, end int, edits []labelRewriteEdit) rewritePredicate {
	local := make([]labelRewriteEdit, len(edits))
	for i, edit := range edits {
		edit.start -= start
		edit.end -= start
		edit.copies = append([]cyphertext.SourceCopy(nil), edit.copies...)
		for j := range edit.copies {
			edit.copies[j].Original.Start -= start
			edit.copies[j].Original.End -= start
		}
		local[i] = edit
	}
	text, rewrite := applyLabelRewriteEdits(query[start:end], local, true)
	for i := range rewrite.copies {
		rewrite.copies[i].Original.Start += start
		rewrite.copies[i].Original.End += start
	}
	return rewritePredicate{text: text, copies: rewrite.copies, constructed: rewrite.constructed, entries: rewrite.entries}
}
