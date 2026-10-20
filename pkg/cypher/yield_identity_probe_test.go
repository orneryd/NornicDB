package cypher

import (
	"context"
	"reflect"
	"testing"
)

func TestReviewR73YieldDeclarationIdentity(t *testing.T) {
	for _, sample := range []struct{ syntax, name string }{
		{"plain", "plain"},
		{"`quoted`", "quoted"},
		{"`name,part`", "name,part"},
		{"`name``part`", "name`part"},
	} {
		t.Run(sample.syntax, func(t *testing.T) {
			exec := newFabricCallRowsExecutor(t)
			query := "WITH 7 AS value CALL db.labels() YIELD label AS " + sample.syntax + " RETURN *"
			result, err := exec.Execute(context.Background(), query, nil)
			if err != nil {
				t.Fatalf("execute: %v", err)
			}
			explain, err := exec.Execute(context.Background(), "EXPLAIN "+query, nil)
			if err != nil {
				t.Fatalf("explain: %v", err)
			}
			columns := exec.StatementColumns(query)
			t.Logf("query=%s execute=%q explain=%q declarations=%q", query, result.Columns, explain.Columns, columns)
			// This is the existing declaration-only YIELD contract: it need not
			// resolve the unrelated outer value, but the declared alias has one name.
			if !reflect.DeepEqual([]string{sample.name}, columns) {
				t.Errorf("YIELD declaration identity: want %q, got %q", []string{sample.name}, columns)
			}
		})
	}
}
