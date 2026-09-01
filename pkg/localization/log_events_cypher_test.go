package localization

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestCypherLogEventsRenderAcrossLocales(t *testing.T) {
	tests := []struct {
		name    string
		event   LogEvent
		english string
		spanish string
	}{
		{"slow query", CypherSlowQueryEvent("0123456789abcdef", 42, "MATCH (n) RETURN n"), "slow query", "consulta lenta"},
		{"create database invoked", CypherCreateDatabaseInvokedEvent(21), "executeCreateDatabase invoked", "se invocó executeCreateDatabase"},
		{"create database failed", CypherCreateDatabaseFailedEvent(), "CreateDatabase failed", "CreateDatabase falló"},
		{"create database succeeded", CypherCreateDatabaseSucceededEvent(), "CreateDatabase succeeded", "CreateDatabase se completó correctamente"},
		{"vector search disabled", CypherVectorSearchDisabledEvent("people"), "db.index.vector.queryNodes called against vector-disabled database — returning empty result", "se llamó a db.index.vector.queryNodes en una base de datos con vectores deshabilitados; se devuelve un resultado vacío"},
		{"orphan detected", CypherOrphanedEmbeddingDetectedEvent("node-7"), "orphaned embedding detected, removing from indexes", "se detectó una incrustación huérfana; se elimina de los índices"},
		{"orphan removal failed", CypherOrphanedEmbeddingRemovalFailedEvent("node-7", errors.New("remove failed")), "failed to remove orphaned embedding", "no se pudo eliminar la incrustación huérfana"},
		{"decay disabled", CypherDecaySubsystemDisabledEvent(), "decay function called but decay subsystem is disabled; returning neutral scores", "se llamó a la función de decaimiento, pero el subsistema de decaimiento está deshabilitado; se devuelven puntuaciones neutras"},
	}

	manager, err := NewManager(nil, slog.Default())
	require.NoError(t, err)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NotEmpty(t, test.event.ID)
			for _, locale := range []struct {
				tag  language.Tag
				want string
			}{
				{language.AmericanEnglish, test.english},
				{language.EuropeanSpanish, test.spanish},
				{language.MustParse("en-XA"), "[!! " + test.english + " !!]"},
			} {
				got, _, renderErr := manager.Render(WithPreferences(context.Background(), locale.tag), test.event.Message)
				require.NoError(t, renderErr)
				require.Equal(t, locale.want, got)
			}
		})
	}
}

func TestCypherLogEventsPreserveStructuredFields(t *testing.T) {
	testErr := errors.New("remove failed")
	tests := []struct {
		name   string
		event  LogEvent
		keys   []string
		values []any
	}{
		{"slow query", CypherSlowQueryEvent("0123456789abcdef", 42, "MATCH (n) RETURN n"), []string{"event", "plan_hash", "cypher.duration_ms", "query"}, []any{"slow_query", "0123456789abcdef", int64(42), "MATCH (n) RETURN n"}},
		{"create database invoked", CypherCreateDatabaseInvokedEvent(21), []string{"subsystem", "query_len"}, []any{"create_database", int64(21)}},
		{"create database failed", CypherCreateDatabaseFailedEvent(), []string{"subsystem"}, []any{"create_database"}},
		{"create database succeeded", CypherCreateDatabaseSucceededEvent(), []string{"subsystem"}, []any{"create_database"}},
		{"vector search disabled", CypherVectorSearchDisabledEvent("people"), []string{"subsystem", "index_name"}, []any{"vector_search", "people"}},
		{"orphan detected", CypherOrphanedEmbeddingDetectedEvent("node-7"), []string{"subsystem", "node_id"}, []any{"vector_search", "node-7"}},
		{"orphan removal failed", CypherOrphanedEmbeddingRemovalFailedEvent("node-7", testErr), []string{"subsystem", "node_id", "error"}, []any{"vector_search", "node-7", testErr}},
		{"decay disabled", CypherDecaySubsystemDisabledEvent(), []string{"component"}, []any{"knowledgepolicy"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Len(t, test.event.Attrs, len(test.keys))
			for index, attr := range test.event.Attrs {
				require.Equal(t, test.keys[index], attr.Key)
				require.Equal(t, test.values[index], attr.Value.Resolve().Any())
			}
		})
	}
}
