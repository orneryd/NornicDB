package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// braceConstruction is a producer's typed construction, not a parse of its
// generated text. Source.Origin still marks that text as generated. Ordinary
// source braces and entries retain their original immutable fact identities.
type braceConstruction struct {
	span    cyphertext.Span
	kind    mapBraceKind
	entries []cyphertext.BraceEntry
}

// braceConstructionPart joins a producer-declared opening and closing emitted
// by separate edits. IDs are local to one rewrite, never inferred from text.
type braceConstructionPart struct {
	id, at  int
	opening bool
	kind    mapBraceKind
}

func withBraceConstructions(ctx context.Context, source *cyphertext.Source, constructions []braceConstruction) context.Context {
	return cyphertext.WithSourceView(ctx, sourceWithBraceConstructions(source, constructions))
}

func sourceWithBraceConstructions(source *cyphertext.Source, constructions []braceConstruction) *cyphertext.Source {
	var bodies []cyphertext.Span
	for _, construction := range constructions {
		if construction.kind != subqueryBrace || len(construction.entries) != 0 {
			panic("cypher: producer declared a non-query-body construction")
		}
		bodies = append(bodies, construction.span)
	}
	return source.WithQueryBodies(bodies)
}
func shiftBraceConstruction(construction braceConstruction, by int) braceConstruction {
	construction.span.Start += by
	construction.span.End += by
	construction.entries = append([]cyphertext.BraceEntry(nil), construction.entries...)
	for i := range construction.entries {
		entry := &construction.entries[i]
		entry.Span.Start += by
		entry.Span.End += by
		if entry.Name.End > entry.Name.Start {
			entry.Name.Start += by
			entry.Name.End += by
		}
		if entry.Value.End > entry.Value.Start {
			entry.Value.Start += by
			entry.Value.End += by
		}
	}
	return construction
}
