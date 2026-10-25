// Package emittercheck supplies public-executor observations for emitter tests
// outside package cypher. The property itself lives in cyphernames so cypher's
// internal tests can use the identical assertions without an import cycle.
package emittercheck

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/cyphernames"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func Executor(t *testing.T) *cypher.StorageExecutor {
	t.Helper()
	store := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "emitter"))
}

func Observe(executor *cypher.StorageExecutor) cyphernames.Execute {
	return func(query string, params map[string]any) ([]cyphernames.StoredRelationship, error) {
		result, err := executor.Execute(context.Background(), query, params)
		if err != nil {
			return nil, err
		}
		var rows []cyphernames.StoredRelationship
		for _, row := range result.Rows {
			edge, ok := row[0].(*storage.Edge)
			if !ok {
				return nil, fmt.Errorf("expected stored relationship, got %T", row[0])
			}
			rows = append(rows, cyphernames.StoredRelationship{Type: edge.Type, Properties: edge.Properties})
		}
		return rows, nil
	}
}

// Check re-imports the original emitted type/property syntax in every spelling.
func Check(t *testing.T, text, name string, params, properties map[string]any) {
	t.Helper()
	bracket, err := cyphernames.RelationshipBracket(text)
	require.NoError(t, err)
	for i, form := range cyphernames.DeclarationForms {
		t.Run(fmt.Sprintf("form-%d", i), func(t *testing.T) {
			variant, err := cyphernames.WithDeclarationForm(bracket, form)
			require.NoError(t, err)
			require.NoError(t, cyphernames.RoundTrip(variant, name, params, properties, Observe(Executor(t))))
		})
	}
}

func CheckSchema(t *testing.T, text, name string, keys []string) {
	t.Helper()
	bracket, err := cyphernames.RelationshipBracket(text)
	require.NoError(t, err)
	for i, form := range cyphernames.DeclarationForms {
		t.Run(fmt.Sprintf("schema-form-%d", i), func(t *testing.T) {
			variant, err := cyphernames.WithDeclarationForm(bracket, form)
			require.NoError(t, err)
			executor := Executor(t)
			require.NoError(t, cyphernames.SchemaRoundTrip(strings.Replace(text, bracket, variant, 1), name, keys, func(query string) ([][]any, error) {
				result, err := executor.Execute(context.Background(), query, nil)
				if err != nil {
					return nil, err
				}
				return result.Rows, nil
			}))
		})
	}
}
