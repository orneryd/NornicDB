package cyphertext

import (
	"strings"
	"testing"
)

func TestRelationshipTypesRetainWrittenClassification(t *testing.T) {
	for _, example := range []struct {
		text, name      string
		quoted, dynamic bool
	}{
		{"R", "R", false, false}, {"`A|B`", "A|B", true, false},
		{"`A``B`", "A`B", true, false}, {"关系", "关系", false, false},
		{"`$(R)`", "$(R)", true, false}, {"$(R)", "$(R)", false, true},
	} {
		types, invalid, empty, alternatives := ReadRelationshipTypes(example.text)
		if invalid != "" || empty || alternatives || types.Len() != 1 {
			t.Fatalf("unexpected type list: %s", example.text)
		}
		fact := types.At(0)
		if fact.StorageName() != example.name || fact.IsQuoted() != example.quoted || fact.IsDynamic() != example.dynamic || fact.MatchesStorageName(example.name) == example.dynamic {
			t.Errorf("classification changed: %s", example.text)
		}
	}
}

func TestRelationshipConnectionPrefixCompletesAtVisitedDetail(t *testing.T) {
	for _, example := range []struct {
		text, detail       string
		incoming, outgoing bool
	}{
		{"-[:`A]B`]->()", ":`A]B`", false, true},
		{"<-[:`A``]B` {xs:[1,2]}]-()", ":`A``]B` {xs:[1,2]}", true, false},
		{"- /* [ */ [r:R {k:'x]y'}] /* ] */ ->()", "r:R {k:'x]y'}", false, true},
	} {
		prefix, ok := ReadConnectionPrefix(example.text, 0)
		if !ok || prefix.DetailOpen() < 0 {
			t.Fatalf("missing connection prefix: %s", example.text)
		}
		close := prefix.DetailOpen() + 1 + len(example.detail)
		frame, ok := prefix.Complete(close)
		if !ok || !frame.Complete() || !frame.Bracketed() || frame.Incoming() != example.incoming || frame.Outgoing() != example.outgoing {
			t.Fatalf("incorrect completed connection: %s", example.text)
		}
		open, end := frame.DetailBounds()
		if end != close || example.text[open+1:end] != example.detail || !strings.HasPrefix(example.text[frame.End():], "(") {
			t.Fatalf("lost visited detail or following node: %s", example.text)
		}
	}
}
