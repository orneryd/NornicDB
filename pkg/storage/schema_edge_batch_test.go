package storage

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegisterEdgeTokensPreservesOrderAndAtomicValidation(t *testing.T) {
	for _, distinct := range []int{1, 8, 9, 100} {
		t.Run(fmt.Sprint(distinct), func(t *testing.T) {
			schema := NewSchemaManager()
			var edges []*Edge
			want := make([]string, distinct)
			for i := range want {
				want[i] = fmt.Sprintf("type-%03d", i)
				edges = append(edges, &Edge{Type: want[i]}, &Edge{Type: want[i]})
			}
			edges = append(edges, &Edge{Type: want[0]})
			persists := 0
			schema.SetPersister(func(*SchemaDefinition) error { persists++; return nil })
			invalid := append(append([]*Edge(nil), edges...), nil)
			require.ErrorIs(t, schema.RegisterEdgeTokens(invalid), ErrInvalidData)
			require.Zero(t, persists)
			require.Empty(t, schema.ExportDefinition().RelationshipTypeTokens)

			require.NoError(t, schema.RegisterEdgeTokens(edges))
			require.Equal(t, 1, persists)
			require.Equal(t, want, schema.ExportDefinition().RelationshipTypeTokens)
			require.NoError(t, schema.RegisterEdgeTokens(edges))
			require.Equal(t, 1, persists)
		})
	}

	schema := NewSchemaManager()
	require.NoError(t, schema.RegisterEdgeTokens(nil))
	require.ErrorIs(t, schema.RegisterEdgeTokens([]*Edge{nil}), ErrInvalidData)
	require.NoError(t, schema.RegisterEdgeTokens([]*Edge{{Type: "single"}}))
	require.Equal(t, []string{"single"}, schema.ExportDefinition().RelationshipTypeTokens)
	schema = NewSchemaManager()
	sentinel := errors.New("persist failed")
	schema.SetPersister(func(*SchemaDefinition) error { return sentinel })
	require.ErrorIs(t, schema.RegisterEdgeTokens([]*Edge{{Type: "X"}, {Type: "X"}}), sentinel)
	require.Empty(t, schema.ExportDefinition().RelationshipTypeTokens)
}
