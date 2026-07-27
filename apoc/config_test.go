package apoc

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadFromEnv_FileAccessRootLoadsIndependently(t *testing.T) {
	t.Setenv("NORNICDB_APOC_SECURITY_FILE_ACCESS_ROOT", "/var/lib/nornicdb/import")

	cfg := LoadFromEnv()
	require.Equal(t, "/var/lib/nornicdb/import", cfg.Security.FileAccessRoot)
	require.False(t, cfg.Security.AllowImportFileAccess)
	require.False(t, cfg.Security.AllowExportFileAccess)
}

func TestLoadFromEnv_SplitFileAccessOverridesLegacyShorthand(t *testing.T) {
	t.Setenv("NORNICDB_APOC_SECURITY_ALLOW_FILE_ACCESS", "true")
	t.Setenv("NORNICDB_APOC_SECURITY_ALLOW_IMPORT_FILE_ACCESS", "false")
	t.Setenv("NORNICDB_APOC_SECURITY_ALLOW_EXPORT_FILE_ACCESS", "true")

	cfg := LoadFromEnv()
	require.True(t, cfg.Security.AllowFileAccess)
	require.False(t, cfg.Security.AllowImportFileAccess)
	require.True(t, cfg.Security.AllowExportFileAccess)
}

func TestLoadFromEnvOrFile_LegacyFileAccessYamlEnablesImportAndExport(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "apoc.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(`security:
  allow_file_access: true
`), 0o644))

	cfg := LoadFromEnvOrFile(configPath)
	require.True(t, cfg.Security.AllowFileAccess)
	require.True(t, cfg.Security.AllowImportFileAccess)
	require.True(t, cfg.Security.AllowExportFileAccess)
}
