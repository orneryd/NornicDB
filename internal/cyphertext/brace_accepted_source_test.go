package cyphertext

import (
	"context"
	"testing"
)

// Author: #1663 implementation lane. Normalization ancestry supplies mapping;
// the final accepted input supplies the statement's immutable record identity.
func TestBraceFactsRetainAcceptedStatementIdentity(t *testing.T) {
	source := NewSource(context.Background(), `RETURN \u007bｘ: 1｝`, RawSource)
	if source.Err != nil {
		t.Fatal(source.Err)
	}
	if source.Text != "RETURN {ｘ: 1}" {
		t.Fatalf("accepted input = %q", source.Text)
	}
	if source.parent == nil {
		t.Fatal("control requires composed normalization stages")
	}
	if source.OriginalView() != source {
		t.Fatal("mapping ancestor replaced accepted statement identity")
	}
	facts := source.BraceFacts()
	if _, found := facts.At(7); !found {
		t.Fatal("accepted opening brace has no retained record")
	}
	window := source.Window(7, len(source.Text))
	if window.BraceFacts() != facts || window.OriginalView() != source {
		t.Fatal("window rebuilt the original record")
	}
}
