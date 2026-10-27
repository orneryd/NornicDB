package cyphertext

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSourceOriginalPositionUnitsAndBoundaries(t *testing.T) {
	collapsed := &Source{Original: "π\r\n  RETURN z", Text: "π RETURN z", Edits: []SourceEdit{{Original: Span{2, 6}, Accepted: Span{2, 3}}}}
	crOnly := &Source{Original: "π\r  RETURN z", Text: "π RETURN z", Edits: []SourceEdit{{Original: Span{2, 5}, Accepted: Span{2, 3}}}}
	acceptedLF := &Source{Original: "π\r\n  RETURN z", Text: "π\nRETURN z", Edits: []SourceEdit{{Original: Span{2, 6}, Accepted: Span{2, 3}}}}
	deleted := &Source{Original: "πREMOVED值", Text: "π值", Edits: []SourceEdit{{Original: Span{2, 9}, Accepted: Span{2, 2}}}}
	quoted := &Source{Original: "RETURN `名 字`", Text: "RETURN expanded_name", Edits: []SourceEdit{{Original: Span{7, 16}, Accepted: Span{7, 20}}}}
	for _, tc := range []struct {
		name                               string
		source                             *Source
		line, column, wantLine, wantColumn int
		valid                              bool
	}{
		{"unicode before collapsed CRLF", collapsed, 1, 2, 2, 2, true},
		{"CR only", crOnly, 1, 2, 2, 2, true},
		{"accepted LF", acceptedLF, 2, 0, 2, 2, true},
		{"deleted boundary right bias", deleted, 1, 1, 1, 8, true},
		{"deleted end", deleted, 1, 2, 1, 9, true},
		{"collapsed end", collapsed, 1, 10, 2, 10, true},
		{"invalid line", collapsed, 9, 0, 0, 0, false},
		{"invalid column", collapsed, 1, 100, 0, 0, false},
		{"quoted start", quoted, 1, 7, 1, 7, true},
		{"quoted interior extent", quoted, 1, 8, 1, 7, true},
		{"quoted end rune column", quoted, 1, 20, 1, 12, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			line, column, valid := tc.source.OriginalPosition(tc.line, tc.column)
			require.Equal(t, tc.valid, valid)
			require.Equal(t, tc.wantLine, line)
			require.Equal(t, tc.wantColumn, column)
		})
	}
}

func TestSourceRewriteObservationPolicies(t *testing.T) {
	source := &Source{Original: "a`n n`z", Text: "aexpandedz", Edits: []SourceEdit{{Original: Span{1, 6}, Accepted: Span{1, 9}}}}
	for _, tc := range []struct {
		name  string
		match RewriteMatch
		want  bool
	}{
		{"overlap", RewriteMatch{Policy: RewriteSpanOverlap, Span: Span{2, 3}}, true},
		{"right boundary", RewriteMatch{Policy: RewriteSpanOverlap, Span: Span{9, 10}}, false},
		{"left boundary", RewriteMatch{Policy: RewriteSpanOverlap, Span: Span{0, 1}}, false},
		{"replacement mention", RewriteMatch{Policy: RewriteFragmentMention, Fragment: "expanded.x"}, true},
		{"generated marker", RewriteMatch{Policy: RewriteFragmentMention, Fragment: "internal.x", Markers: []string{"internal"}}, true},
		{"unrelated", RewriteMatch{Policy: RewriteFragmentMention, Fragment: "other"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, source.HasRewrite(tc.match)) })
	}
}
