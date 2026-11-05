package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSearchCursorMaxPageSizeConfig(t *testing.T) {
	for _, test := range []struct {
		name string
		yaml string
		env  string
		want int
	}{
		{"default", "memory: {}", "", 500},
		{"yaml", "memory:\n  search_cursor_max_page_size: 1000", "", 1000},
		{"environment", "memory: {}", "750", 750},
		{"environment overrides yaml", "memory:\n  search_cursor_max_page_size: 1000", "700", 700},
		{"zero keeps default", "memory:\n  search_cursor_max_page_size: 0", "0", 500},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("NORNICDB_SEARCH_CURSOR_MAX_PAGE_SIZE", test.env)
			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(test.yaml), 0o600))
			cfg, err := LoadFromFile(path)
			require.NoError(t, err)
			require.Equal(t, test.want, cfg.Memory.SearchCursorMaxPageSize)
		})
	}
}
