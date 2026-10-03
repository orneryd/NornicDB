package cypher

import (
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// Every procedure in pkg/localization/procedure_metadata.yaml is a registered
// built-in that describes itself with its localized metadata, however it is
// registered (registerBuiltInProcedure or a ProcedureSpec helper).
func TestEveryProcedureMetadataEntryIsRegistered(t *testing.T) {
	data, err := os.ReadFile("../localization/procedure_metadata.yaml")
	require.NoError(t, err)
	var entries []struct {
		Name string `yaml:"name"`
	}
	require.NoError(t, yaml.Unmarshal(data, &entries))
	require.NotEmpty(t, entries)

	ensureBuiltInProceduresRegistered()
	for _, entry := range entries {
		procedure, found := globalProcedureRegistry.Get(entry.Name)
		if !found || procedure.User {
			t.Errorf("metadata procedure %s has no built-in registration", entry.Name)
			continue
		}
		require.Equal(t, localization.CypherProcedureMetadata(entry.Name).ID, procedure.Spec.DescriptionMessage.ID, "procedure %s must describe itself with its metadata", entry.Name)
	}
}
