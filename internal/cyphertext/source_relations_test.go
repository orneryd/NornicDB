package cyphertext

import (
	"reflect"
	"strings"
	"testing"
)

// Author: #1663 implementation lane. Source composition contracts allocated
// by orchestrator Nornic2; original endpoint pairing remains in BraceFacts.
func TestSourceCopyRelationsCompose(t *testing.T) {
	root := NormalizeSource("RETURN {x: 1}")
	window := root.Window(7, 13)
	chain := window.RewriteWithCopies("id({x: 1})", []SourceEdit{{Original: Span{0, 6}, Accepted: Span{0, 10}}}, []SourceCopy{{Original: Span{0, 6}, Accepted: Span{3, 9}}})
	copies, err := chain.CopyRelationsFrom(root)
	if err != nil {
		t.Fatal(err)
	}
	want := []SourceCopy{{Original: Span{7, 13}, Accepted: Span{3, 9}}}
	if !reflect.DeepEqual(copies, want) {
		t.Fatalf("relations: %+v", copies)
	}
	single := root.RewriteWithCopies(chain.Text, []SourceEdit{{Original: Span{0, 13}, Accepted: Span{0, 10}}}, copies)
	for index := range chain.Text {
		span := Span{index, index + 1}
		left, right := chain.Origin(span), single.Origin(span)
		if left.Exact != right.Exact || left.Generated != right.Generated || left.Exact && left.Original != right.Original {
			t.Fatalf("byte%d: chain=%+v single=%+v", index, left, right)
		}
	}
	if _, err := chain.CopyRelationsFrom(NormalizeSource(root.Text)); err == nil {
		t.Fatal("equal text established false ancestry")
	}
	legacy := root.Rewrite("RETURN id({x: 1})", []SourceEdit{{Original: Span{7, 13}, Accepted: Span{7, 17}}})
	relations, err := legacy.CopyRelationsFrom(root)
	if err != nil || !reflect.DeepEqual(relations, []SourceCopy{{Original: Span{0, 7}, Accepted: Span{0, 7}}}) {
		t.Fatalf("legacy replacement became a copy: %+v %v", relations, err)
	}
}

func TestSourceProjectsOriginalBraceEndpoints(t *testing.T) {
	query := "CALL { USE db RETURN {x: 1} }"
	root := NormalizeSource(query)
	body, ok := root.BraceFacts().At(strings.Index(query, "{"))
	if !ok {
		t.Fatal("body record missing")
	}
	start := strings.Index(query, "USE")
	end := strings.Index(query, "RETURN")
	text := query[:start] + query[end:]
	child := root.RewriteWithCopies(text, []SourceEdit{{Original: Span{start, end}, Accepted: Span{start, start}}}, nil)
	copies, err := child.CopyRelationsFrom(root)
	if err != nil {
		t.Fatal(err)
	}
	flat := root.RewriteWithCopies(text, []SourceEdit{{Original: Span{0, len(query)}, Accepted: Span{0, len(text)}}}, copies)
	projected, err := flat.ProjectBrace(body)
	if err != nil || !reflect.DeepEqual(projected, []Span{{strings.Index(text, "{"), len(text)}}) {
		t.Fatalf("brace extent: %+v %v", projected, err)
	}
	// Same byte spelling is insufficient: this close was generated rather
	// than copied, and must never substitute for the original endpoint.
	generated := root.RewriteWithCopies(query, []SourceEdit{{Original: Span{len(query) - 1, len(query)}, Accepted: Span{len(query) - 1, len(query)}}}, nil)
	if _, err := generated.ProjectBrace(body); err == nil {
		t.Fatal("generated close acquired original endpoint identity")
	}
}
