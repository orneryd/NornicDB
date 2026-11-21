// Verification test for hunt lead L-H9-006 (v1.5.2 verification campaign, H9
// verifier lane, 2026-11-21). Test-only, not for merge.
package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func verifyH9Load(t *testing.T, yamlText string) *Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "nornicdb.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yamlText), 0o600))
	cfg, err := LoadFromFile(path)
	require.NoError(t, err)
	t.Logf("VERIFY_H9 bm25 enabled=%v warming=%q vector enabled=%v warming=%q",
		cfg.Memory.SearchBM25Enabled, cfg.Memory.SearchBM25Warming, cfg.Memory.SearchVectorEnabled, cfg.Memory.SearchVectorWarming)
	return cfg
}

func verifyH9AssertSearchOff(t *testing.T, cfg *Config) {
	t.Helper()
	assert.False(t, cfg.Memory.SearchBM25Enabled, "bm25 enabled")
	assert.Equal(t, "lazy", cfg.Memory.SearchBM25Warming, "bm25 warming")
	assert.False(t, cfg.Memory.SearchVectorEnabled, "vector enabled")
	assert.Equal(t, "lazy", cfg.Memory.SearchVectorWarming, "vector warming")
}

// The documented memory.search_* block (multi-database.md, low-memory-mode.md).
func TestVerifyH9_006_MemorySearchBlockIsApplied(t *testing.T) {
	verifyH9AssertSearchOff(t, verifyH9Load(t, "memory:\n  search_bm25_enabled: false\n  search_bm25_warming: lazy\n  search_vector_enabled: false\n  search_vector_warming: lazy\n"))
}

// Control: the same settings under the search: block the loader reads.
func TestVerifyH9_006_Control_SearchBlock(t *testing.T) {
	verifyH9AssertSearchOff(t, verifyH9Load(t, "search:\n  bm25_enabled: false\n  bm25_warming: lazy\n  vector_enabled: false\n  vector_warming: lazy\n"))
}
