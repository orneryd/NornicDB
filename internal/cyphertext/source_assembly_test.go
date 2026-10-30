package cyphertext

import (
	"reflect"
	"testing"
)

// Author: #1663 implementation lane. Piece assembly is composition of the
// existing Window/RewriteWithCopies ancestry, including repeated copies.
func TestSourceAssemblyUsesActualAncestry(t *testing.T) {
	root := NewSource(t.Context(), "RETURN {x: 1}", AcceptedSource)
	value := root.Window(7, len(root.Text))
	first, err := root.Assemble([]SourcePiece{{Generated: "RETURN id("}, {View: value}, {Generated: ")"}})
	if err != nil {
		t.Fatal(err)
	}
	output, err := root.Assemble([]SourcePiece{{View: first.Window(0, len(first.Text))}, {Generated: ", "}, {View: value}})
	if err != nil {
		t.Fatal(err)
	}
	wantText := "RETURN id({x: 1}), {x: 1}"
	if output.Text != wantText {
		t.Fatal(output.Text)
	}
	equivalent := root.RewriteWithCopies(wantText, []SourceEdit{{Original: Span{End: len(root.Text)}, Accepted: Span{End: len(wantText)}}}, []SourceCopy{{Original: Span{Start: 7, End: 13}, Accepted: Span{Start: 10, End: 16}}, {Original: Span{Start: 7, End: 13}, Accepted: Span{Start: 19, End: 25}}})
	got, err := output.CopyRelationsFrom(root)
	if err != nil {
		t.Fatal(err)
	}
	want, err := equivalent.CopyRelationsFrom(root)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("relations: %+v, %+v, %v", got, want, err)
	}
	if !output.Origin(Span{Start: 7, End: 9}).Generated {
		t.Fatal("generated function name acquired original identity")
	}
	if _, err := root.Assemble([]SourcePiece{{View: NewSource(t.Context(), value.Text, AcceptedSource)}}); err == nil {
		t.Fatal("equal-text non-ancestor accepted")
	}
}
