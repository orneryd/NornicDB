package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTemporalValuesRetainTheirTypesAcrossMutationPipelines(t *testing.T) {
	temporals := []struct {
		name       string
		expression string
		assertType func(*testing.T, interface{})
	}{
		{name: "date", expression: "date('2026-09-14')", assertType: func(t *testing.T, value interface{}) { require.IsType(t, CypherDate{}, value) }},
		{name: "local time", expression: "localtime('12:00:00')", assertType: func(t *testing.T, value interface{}) { require.IsType(t, CypherLocalTime{}, value) }},
		{name: "time", expression: "time('12:00:00Z')", assertType: func(t *testing.T, value interface{}) { require.IsType(t, CypherTime{}, value) }},
		{name: "local datetime", expression: "localdatetime('2026-09-14T12:00:00')", assertType: func(t *testing.T, value interface{}) { require.IsType(t, CypherLocalDateTime{}, value) }},
		{name: "datetime", expression: "datetime('2026-09-14T12:00:00Z')", assertType: func(t *testing.T, value interface{}) { require.IsType(t, CypherDateTime{}, value) }},
		{name: "duration", expression: "duration('P1D')", assertType: func(t *testing.T, value interface{}) { require.IsType(t, &CypherDuration{}, value) }},
	}
	routes := []struct {
		name  string
		query func(id, expression string) string
	}{
		{name: "unwind create", query: func(id, expression string) string {
			return fmt.Sprintf("UNWIND [1] AS i CREATE (:TemporalPipeline {id: '%s', v: %s})", id, expression)
		}},
		{name: "with create", query: func(id, expression string) string {
			return fmt.Sprintf("WITH 1 AS i CREATE (:TemporalPipeline {id: '%s', v: %s})", id, expression)
		}},
		{name: "unwind merge", query: func(id, expression string) string {
			return fmt.Sprintf("UNWIND [1] AS i MERGE (:TemporalPipeline {id: '%s', v: %s})", id, expression)
		}},
	}

	for _, mode := range []struct {
		name     string
		explicit bool
	}{{name: "autocommit"}, {name: "explicit transaction", explicit: true}} {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal_pipeline"))
			ctx := context.Background()
			if mode.explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				defer func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) }()
			}

			for _, route := range routes {
				for _, temporal := range temporals {
					name := route.name + "/" + temporal.name
					t.Run(name, func(t *testing.T) {
						id := route.name + "-" + temporal.name
						_, err := exec.Execute(ctx, route.query(id, temporal.expression), nil)
						require.NoError(t, err)

						result, err := exec.Execute(ctx, fmt.Sprintf("MATCH (n:TemporalPipeline {id: '%s'}) RETURN n.v AS v, n.v = %s AS equal", id, temporal.expression), nil)
						require.NoError(t, err)
						require.Len(t, result.Rows, 1)
						temporal.assertType(t, result.Rows[0][0])
						require.Equal(t, true, result.Rows[0][1])
					})
				}
			}
		})
	}
}
