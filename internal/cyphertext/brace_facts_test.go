package cyphertext

import (
	"strings"
	"testing"
)

// Author: #1663 implementation lane. These controls distinguish retained
// source identity and embedded-expression extents from fragment rescanning.
func TestBraceFactsRetainedOriginalRecord(t *testing.T) {
	source := NormalizeSource("RETURN {x: 1}")
	facts := source.BraceFacts()
	view := source.RewriteWithCopies("RETURN id({x: 1})", []SourceEdit{{Original: Span{7, 13}, Accepted: Span{7, 17}}}, []SourceCopy{{Original: Span{7, 13}, Accepted: Span{10, 16}}})
	if view.BraceFacts() != facts || view.Window(10, 16).BraceFacts() != facts {
		t.Fatal("view rebuilt the original record")
	}
	record, ok := facts.AtView(view, 10)
	if !ok || record.Open != 7 || record.Close != 12 || record.Body {
		t.Fatalf("record: %+v %v", record, ok)
	}
	entries := record.Entries()
	if len(entries) != 1 || entries[0].Name != (Span{8, 9}) || entries[0].Value != (Span{10, 12}) {
		t.Fatalf("entries: %+v", entries)
	}
	entries[0].Name = Span{}
	if record.Entries()[0].Name != (Span{8, 9}) {
		t.Fatal("caller mutated retained entries")
	}
}

// Author: #1663 lane. Physical slots do not change map-key admission. The same
// original separator identities remain available to declaration readers.
func TestInvalidMapKeyRetainsPhysicalEntrySlots(t *testing.T) {
	const query = "RETURN {'vector.dimensions': 768}"
	source := NewSource(t.Context(), query, AcceptedSource)
	record, found := source.BraceFacts().At(7)
	if !found || len(record.Entries()) != 1 {
		t.Fatal("missing original entry")
	}
	entry := record.Entries()[0]
	if entry.Kind != InvalidBraceEntry || entry.IssueKind != MapKeyInvalid {
		t.Fatalf("physical slots admitted an invalid map key: %+v", entry)
	}
	if query[entry.Name.Start:entry.Name.End] != "'vector.dimensions'" || strings.TrimSpace(query[entry.Value.Start:entry.Value.End]) != "768" {
		t.Fatalf("lost physical slots: %+v", entry)
	}
	colon := entry.SeparatorSpans().Colon
	if _, found := source.OriginalEntryAtColon(colon); !found {
		t.Fatal("missing original colon identity")
	}
	const output = "RETURN id({'vector.dimensions': 768})"
	view := source.RewriteWithCopies(output, []SourceEdit{{Original: Span{End: len(query)}, Accepted: Span{End: len(output)}}}, []SourceCopy{{Original: Span{Start: 7, End: len(query)}, Accepted: Span{Start: 10, End: len(output) - 1}}})
	projected, err := view.ProjectEntry(entry)
	if err != nil || len(projected) != 1 {
		t.Fatalf("projection: %+v, %v", projected, err)
	}
	if projected[0].Kind != InvalidBraceEntry || output[projected[0].Name.Start:projected[0].Name.End] != "'vector.dimensions'" || strings.TrimSpace(output[projected[0].Value.Start:projected[0].Value.End]) != "768" {
		t.Fatalf("projection changed physical slots or admission: %+v", projected[0])
	}
}

func TestBraceFactsEmbeddedExpressions(t *testing.T) {
	query := `RETURN s"value {coalesce({x: 1}, s'inner {n{.x}}')}"`
	source := NormalizeSource(query)
	facts := source.BraceFacts()
	if len(facts.Unmatched()) != 0 {
		t.Fatalf("unmatched: %+v", facts.Unmatched())
	}
	if len(facts.Scopes()) != 3 {
		t.Fatalf("scopes: %+v", facts.Scopes())
	}
	for _, needle := range []string{"{x: 1}", "{.x}"} {
		open := strings.Index(query, needle)
		record, ok := facts.At(open)
		if !ok || record.Close != open+len(needle)-1 || len(record.Entries()) != 1 {
			t.Fatalf("%s: %+v %v", needle, record, ok)
		}
	}
	// The interpolation delimiters belong to the template grammar, rather than
	// becoming extra map literals in its containing expression stream.
	if len(facts.Braces()) != 2 {
		t.Fatalf("braces: %+v", facts.Braces())
	}
}
