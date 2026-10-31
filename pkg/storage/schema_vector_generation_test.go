package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVectorPropertyGenerationPublishesOnlyCommittedRetirements(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(map[bool]string{false: "rollback", true: "drop_recreate_commit"}[commit], func(t *testing.T) {
			engine, err := NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			schema := engine.GetSchemaForNamespace("generation")
			require.NoError(t, schema.AddVectorIndex("same", "Doc", "embedding", 3, "cosine", ConstraintEntityNode))
			before := schema.VectorIndexGeneration()
			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetNamespace("generation"))
			view, err := tx.Schema(OrdinarySchemaChanges)
			require.NoError(t, err)
			require.NoError(t, view.DropIndex("same"))
			require.NoError(t, view.AddVectorIndex("same", "Doc", "embedding", 3, "cosine", ConstraintEntityNode))
			require.NoError(t, tx.StageSchemaChanges())
			if commit {
				require.NoError(t, tx.Commit())
			} else {
				require.NoError(t, tx.Rollback())
			}
			require.Equal(t, !commit, schema.VectorEntryGenerationCurrent("NODE", []string{"Doc"}, "embedding", before))
			require.True(t, schema.VectorEntryGenerationCurrent("NODE", []string{"Doc"}, "unrelated", before))
			require.True(t, schema.VectorEntryGenerationCurrent("NODE", []string{"Doc"}, "embedding", schema.VectorIndexGeneration()))
		})
	}
	// A failed direct persistence must leave the descriptor and epoch intact.
	schema := NewSchemaManager()
	require.NoError(t, schema.AddVectorIndex("same", "Doc", "embedding", 3, "cosine", ConstraintEntityNode))
	before := schema.VectorIndexGeneration()
	schema.SetPersister(func(*SchemaDefinition) error { return errors.New("injected schema persistence failure") })
	require.Error(t, schema.DropIndex("same"))
	_, present := schema.GetVectorIndex("same")
	require.True(t, present)
	require.Equal(t, before, schema.VectorIndexGeneration())
}
