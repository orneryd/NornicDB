package cyphertext

import (
	"strings"
	"testing"
)

// Author: #1663 implementation lane. Entry endpoint/declaration contract:
// orchestrator Nornic2. Generated trivia is not an exact byte projection.
func TestSourceEntryProjectionThroughTriviaRewrite(t *testing.T) {
	root := NormalizeSource("RETURN {  x:  coalesce(1, 2)  }")
	record, _ := root.BraceFacts().At(7)
	entry := record.Entries()[0]
	view := root.Rewrite("RETURN { x: coalesce(1, 2) }", []SourceEdit{{Original: Span{8, 10}, Accepted: Span{8, 9}}, {Original: Span{12, 14}, Accepted: Span{11, 12}}, {Original: Span{28, 30}, Accepted: Span{26, 27}}})
	if spans := view.ProjectFrom(root, entry.Span); len(spans) != 0 {
		t.Fatalf("changed trivia became an exact copy: %+v", spans)
	}
	projected, err := view.ProjectEntry(entry)
	if err != nil || len(projected) != 1 {
		t.Fatalf("entry: %+v %v", projected, err)
	}
	got := projected[0]
	if view.Text[got.Name.Start:got.Name.End] != "x" || strings.TrimSpace(view.Text[got.Value.Start:got.Value.End]) != "coalesce(1, 2)" {
		t.Fatalf("wrong boundaries: %+v", got)
	}
}

func TestSourceEntryProjectionRequiresDeclaredSeparators(t *testing.T) {
	root := NormalizeSource("RETURN {x: 1}")
	record, _ := root.BraceFacts().At(7)
	entry := record.Entries()[0]
	view := root.RewriteWithCopies(root.Text, []SourceEdit{{Original: Span{9, 10}, Accepted: Span{9, 10}}}, nil)
	if found, err := root.EntryColonAt(9); !found || err != nil {
		t.Fatalf("original separator: %v %v", found, err)
	}
	if _, err := view.ProjectEntry(entry); err == nil {
		t.Fatal("generated colon substituted for original separator")
	}
	if _, err := view.EntryColonAt(9); err == nil {
		t.Fatal("lexical consumer accepted an undeclared generated separator")
	}
	declaration := EntryDeclaration{Original: entry, Entry: entry, First: entry.Name, Separators: EntrySeparators{Before: Span{7, 8}, Colon: Span{9, 10}, After: Span{12, 13}}}
	declared, err := view.WithEntryDeclarations([]EntryDeclaration{declaration})
	if err != nil {
		t.Fatal(err)
	}
	if found, err := declared.EntryColonAt(9); !found || err != nil {
		t.Fatalf("declared separator: %v %v", found, err)
	}
	projected, err := declared.ProjectEntry(entry)
	if err != nil || len(projected) != 1 || projected[0].Name != (Span{8, 9}) || projected[0].Value != (Span{10, 12}) {
		t.Fatalf("declaration: %+v %v", projected, err)
	}
	declaration.Original = BraceEntry{}
	if _, err := view.WithEntryDeclarations([]EntryDeclaration{declaration}); err == nil {
		t.Fatal("unbound declaration admitted")
	}
}

func TestSourceEntryProjectionRequiresDeclaredName(t *testing.T) {
	root := NormalizeSource("RETURN {x: 1}")
	record, _ := root.BraceFacts().At(7)
	entry := record.Entries()[0]
	view := root.RewriteWithCopies("RETURN {y: 1}", []SourceEdit{{Original: Span{8, 9}, Accepted: Span{8, 9}}}, nil)
	if _, err := view.ProjectEntry(entry); err == nil {
		t.Fatal("rewritten name silently removed the retained entry")
	}
	declaration := EntryDeclaration{Original: entry, Entry: entry, First: entry.Name, Separators: entry.SeparatorSpans()}
	declared, err := view.WithEntryDeclarations([]EntryDeclaration{declaration})
	if err != nil {
		t.Fatal(err)
	}
	projected, err := declared.ProjectEntry(entry)
	if err != nil || len(projected) != 1 || declared.Text[projected[0].Name.Start:projected[0].Name.End] != "y" {
		t.Fatalf("declared name: %+v %v", projected, err)
	}
}

// Author: #1663 lane. A generated quoted name is bound by the original record,
// rather than by attempting to reverse-map generated output bytes.
func TestSourceRebaseEntryDeclarationKeepsOriginalRecord(t *testing.T) {
	root := NormalizeSource("RETURN {x:1}")
	record, _ := root.BraceFacts().At(7)
	entry := record.Entries()[0]
	view := root.RewriteWithCopies("RETURN {`x`:1}", []SourceEdit{{Original: Span{8, 9}, Accepted: Span{8, 11}}}, nil)
	emitted := BraceEntry{Kind: KeyedBraceEntry, Span: Span{8, 13}, Name: Span{8, 11}, Value: Span{12, 13}}
	declared, err := view.WithEntryDeclarations([]EntryDeclaration{{Original: entry, Entry: emitted, First: emitted.Name,
		Separators: EntrySeparators{Before: Span{7, 8}, Colon: Span{11, 12}, After: Span{13, 14}}}})
	if err != nil {
		t.Fatal(err)
	}
	replacement := NormalizeSource(root.Text)
	rebased := declared.Rebase(root, replacement)
	other, _ := replacement.BraceFacts().At(7)
	projected, err := rebased.ProjectEntry(other.Entries()[0])
	if err != nil || len(projected) != 1 || rebased.Text[projected[0].Name.Start:projected[0].Name.End] != "`x`" {
		t.Fatalf("original record was not rebound: %+v %v", projected, err)
	}
	if projected[0].original.source != replacement {
		t.Fatal("cached declaration retained the earlier statement")
	}
}
