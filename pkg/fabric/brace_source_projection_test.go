package fabric

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// Author: #1663 implementation lane. Exercises the public single-graph
// producer and the same producer's retained Source result on nested bodies.
func TestSingleGraphStatementRetainsOriginalBraceEndpoints(t *testing.T) {
	query := "CALL { USE cmp.a CALL { USE cmp.b RETURN {x: 1} AS v } RETURN v UNION ALL USE cmp.c RETURN {x: 2} AS v } RETURN v"
	root := cyphertext.NormalizeSource(query)
	planner := NewFabricPlanner(nil)
	text, err := planner.SingleGraphStatement(t.Context(), query, RawSource)
	if err != nil {
		t.Fatal(err)
	}
	view, err := planner.SingleGraphSource(t.Context(), root)
	if err != nil || view.Text != text || view.Original != query || view.BraceFacts() != root.BraceFacts() {
		t.Fatalf("view lost source: %+v %v", view, err)
	}
	if strings.Contains(text, "USE ") {
		t.Fatalf("USE not removed: %s", text)
	}
	for _, record := range root.BraceFacts().Braces() {
		spans, err := view.ProjectBrace(record)
		if err != nil || len(spans) != 1 {
			t.Fatalf("record %+v: %+v %v", record, spans, err)
		}
		span := spans[0]
		want, wantValid := root.OriginalSpan(cyphertext.Span{Start: record.Close, End: record.Close + 1}, cyphertext.SourceExtent)
		if got, valid := view.OriginalSpan(cyphertext.Span{Start: span.End - 1, End: span.End}, cyphertext.SourceExtent); !valid || !wantValid || got != want {
			t.Fatalf("close lost original identity: %+v", got)
		}
	}
}
