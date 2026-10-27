package cyphertext

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceMappingPolicies(t *testing.T) {
	replacement := &Source{Original: "a`n n`z", Text: "aexpandedz", Edits: []SourceEdit{{Original: Span{1, 6}, Accepted: Span{1, 9}}}}
	deletion := &Source{Original: "abREMOVEDcd", Text: "abcd", Edits: []SourceEdit{{Original: Span{2, 9}, Accepted: Span{2, 2}}}}
	insertion := &Source{Original: "ab", Text: "aINSERTb", Edits: []SourceEdit{{Original: Span{1, 1}, Accepted: Span{1, 7}}}}
	for _, tc := range []struct {
		name   string
		source *Source
		span   Span
		policy SourceMappingPolicy
		want   Span
		valid  bool
	}{
		{name: "whole replacement exact", source: replacement, span: Span{1, 9}, policy: SourceExactBoundaries, want: Span{1, 6}, valid: true},
		{name: "replacement interior exact rejected", source: replacement, span: Span{2, 4}, policy: SourceExactBoundaries},
		{name: "replacement interior extent", source: replacement, span: Span{2, 4}, policy: SourceExtent, want: Span{1, 6}, valid: true},
		{name: "before replacement", source: replacement, span: Span{0, 1}, policy: SourceExactBoundaries, want: Span{0, 1}, valid: true},
		{name: "after replacement", source: replacement, span: Span{9, 10}, policy: SourceExactBoundaries, want: Span{6, 7}, valid: true},
		{name: "empty interior extent stays empty", source: replacement, span: Span{3, 3}, policy: SourceExtent, want: Span{1, 1}, valid: true},
		{name: "empty interior exact rejected", source: replacement, span: Span{3, 3}, policy: SourceExactBoundaries},
		{name: "empty replacement left edge", source: replacement, span: Span{1, 1}, policy: SourceExactBoundaries, want: Span{1, 1}, valid: true},
		{name: "deletion starts right biased", source: deletion, span: Span{2, 4}, policy: SourceExtent, want: Span{9, 11}, valid: true},
		{name: "deletion ends left biased", source: deletion, span: Span{0, 2}, policy: SourceExtent, want: Span{0, 2}, valid: true},
		{name: "empty deletion extent stays empty", source: deletion, span: Span{2, 2}, policy: SourceExtent, want: Span{9, 9}, valid: true},
		{name: "exact collapsed deletion preserves original boundaries", source: deletion, span: Span{2, 2}, policy: SourceExactBoundaries, want: Span{2, 9}, valid: true},
		{name: "insertion edges", source: insertion, span: Span{1, 7}, policy: SourceExactBoundaries, want: Span{1, 1}, valid: true},
		{name: "insertion interior rejected", source: insertion, span: Span{2, 5}, policy: SourceExactBoundaries},
		{name: "insertion interior extent", source: insertion, span: Span{2, 5}, policy: SourceExtent, want: Span{1, 1}, valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, valid := tc.source.OriginalSpan(tc.span, tc.policy)
			require.Equal(t, tc.valid, valid)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestSourceMappingComposesWindowAndEarlierExtent(t *testing.T) {
	parent := &Source{Original: "aSECRETz", Text: "aXXz", Edits: []SourceEdit{{Original: Span{1, 7}, Accepted: Span{1, 3}}}}
	window := parent.Window(1, 3)
	// The window has exact boundaries, although its end is inside an earlier
	// replacement. Earlier source stages retain their established extent policy.
	span, valid := window.OriginalSpan(Span{0, 1}, SourceExactBoundaries)
	require.True(t, valid)
	require.Equal(t, Span{1, 7}, span)

	set := &CredentialSet{}
	set.RecordSource(window, 0, 1, "decoded credential", true)
	require.Equal(t, []CredentialSpan{{Start: 1, End: 7, Raw: "SECRET", Value: "decoded credential", Password: true}}, set.Spans)

	root := NormalizeSource("\ufeffRETURN `n n`")
	start := len("RETURN ")
	accepted := "RETURN internal"
	composed := root.Rewrite(accepted, []SourceEdit{{Original: Span{start, start + len("`n n`")}, Accepted: Span{start, len(accepted)}}})
	span, valid = composed.OriginalSpan(Span{start, len(accepted)}, SourceExactBoundaries)
	require.True(t, valid)
	require.Equal(t, "`n n`", composed.Original[span.Start:span.End])
	_, valid = composed.OriginalSpan(Span{start + 1, len(accepted)}, SourceExactBoundaries)
	require.False(t, valid)
	span, valid = composed.OriginalSpan(Span{start + 1, len(accepted)}, SourceExtent)
	require.True(t, valid)
	require.Equal(t, "`n n`", composed.Original[span.Start:span.End])
}
