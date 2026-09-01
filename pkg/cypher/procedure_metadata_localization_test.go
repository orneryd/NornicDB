package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestShowProceduresLocalizesBuiltInMetadata(t *testing.T) {
	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "procedure_metadata_localization"))
	exec.SetLocalizationRenderer(manager)

	tests := []struct {
		name        string
		tag         language.Tag
		description string
	}{
		{name: "en-US unchanged", tag: language.AmericanEnglish, description: "Lists all labels in the database"},
		{name: "es-ES", tag: language.EuropeanSpanish, description: "Enumera todas las etiquetas de la base de datos"},
		{name: "en-XA", tag: language.MustParse("en-XA"), description: "[!! Lists all labels in the database !!]"},
	}

	var englishRows map[string][]interface{}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := localization.WithPreferences(context.Background(), test.tag)
			result, executeErr := exec.Execute(ctx, "SHOW PROCEDURES", nil)
			require.NoError(t, executeErr)
			require.Equal(t, []string{"name", "signature", "description", "mode", "worksOnSystem"}, result.Columns)

			row := requireProcedureMetadataRow(t, result, "db.labels")
			require.Equal(t, []interface{}{
				"db.labels",
				"db.labels() :: (label :: STRING)",
				test.description,
				"READ",
				false,
			}, row)

			rows := procedureMetadataRowsByName(result)
			if test.tag == language.AmericanEnglish {
				englishRows = rows
				return
			}
			require.Len(t, rows, len(englishRows))
			for name, englishRow := range englishRows {
				localizedRow, ok := rows[name]
				require.True(t, ok, "procedure %s missing for locale %s", name, test.tag)
				require.Equal(t, englishRow[0], localizedRow[0], "name changed for %s", name)
				require.Equal(t, englishRow[1], localizedRow[1], "signature changed for %s", name)
				require.Equal(t, englishRow[3:], localizedRow[3:], "mode metadata changed for %s", name)
			}
		})
	}
}

func TestBuiltInProcedureMetadataDescriptorCoverage(t *testing.T) {
	ensureBuiltInProceduresRegistered()
	coreCount := 0
	apocCount := 0
	for _, spec := range globalProcedureRegistry.ListBuiltIns() {
		if len(spec.Name) >= len("apoc.") && spec.Name[:len("apoc.")] == "apoc." {
			apocCount++
			require.Empty(t, spec.DescriptionMessage.ID, "APOC metadata must remain literal: %s", spec.Name)
			continue
		}
		coreCount++
		require.NotEmpty(t, spec.DescriptionMessage.ID, "core metadata requires a descriptor: %s", spec.Name)
		require.Equal(t, spec.Description, spec.DescriptionMessage.Fallback, "English fallback changed: %s", spec.Name)
	}
	require.Equal(t, 71, coreCount)
	require.Equal(t, 28, apocCount)
}

func TestShowProceduresPreservesUserDefinedLiteralMetadata(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)

	const description = "Literal user-defined description"
	err := RegisterUserProcedure(ProcedureSpec{
		Name:        "custom.localized_literal",
		Signature:   "custom.localized_literal(value :: ANY) :: (value :: ANY)",
		Description: description,
		Mode:        ProcedureModeRead,
		MinArgs:     1,
		MaxArgs:     1,
	}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{}, nil
	})
	require.NoError(t, err)

	manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
	require.NoError(t, err)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "procedure_metadata_user_literal"))
	exec.SetLocalizationRenderer(manager)

	ctx := localization.WithPreferences(context.Background(), language.EuropeanSpanish)
	result, err := exec.Execute(ctx, "SHOW PROCEDURES", nil)
	require.NoError(t, err)
	require.Equal(t, []interface{}{
		"custom.localized_literal",
		"custom.localized_literal(value :: ANY) :: (value :: ANY)",
		description,
		"READ",
		false,
	}, requireProcedureMetadataRow(t, result, "custom.localized_literal"))
}

func requireProcedureMetadataRow(t *testing.T, result *ExecuteResult, name string) []interface{} {
	t.Helper()
	for _, row := range result.Rows {
		if len(row) > 0 && row[0] == name {
			return row
		}
	}
	require.FailNow(t, "procedure metadata row not found", name)
	return nil
}

func procedureMetadataRowsByName(result *ExecuteResult) map[string][]interface{} {
	rows := make(map[string][]interface{}, len(result.Rows))
	for _, row := range result.Rows {
		if len(row) > 0 {
			rows[row[0].(string)] = row
		}
	}
	return rows
}
