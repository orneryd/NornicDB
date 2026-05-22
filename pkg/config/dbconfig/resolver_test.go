package dbconfig

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolve_GlobalOnly(t *testing.T) {
	t.Setenv("NORNICDB_SEARCH_BM25_ENGINE", "v2")
	global := config.LoadDefaults()
	global.Memory.EmbeddingDimensions = 1536
	global.Memory.SearchMinSimilarity = 0.6
	r := Resolve(global, nil)
	require.NotNil(t, r)
	assert.Equal(t, 1536, r.EmbeddingDimensions)
	assert.Equal(t, 0.6, r.SearchMinSimilarity)
	assert.Equal(t, "v2", r.BM25Engine)
	assert.NotEmpty(t, r.Effective["NORNICDB_EMBEDDING_DIMENSIONS"])
}

func TestResolve_Overrides(t *testing.T) {
	t.Setenv("NORNICDB_SEARCH_BM25_ENGINE", "v2")
	global := config.LoadDefaults()
	global.Memory.EmbeddingDimensions = 1024
	global.Memory.SearchMinSimilarity = 0.5
	overrides := map[string]string{
		"NORNICDB_EMBEDDING_DIMENSIONS":  "768",
		"NORNICDB_SEARCH_MIN_SIMILARITY": "0.8",
		"NORNICDB_SEARCH_BM25_ENGINE":    "v2",
	}
	r := Resolve(global, overrides)
	require.NotNil(t, r)
	assert.Equal(t, 768, r.EmbeddingDimensions)
	assert.Equal(t, 0.8, r.SearchMinSimilarity)
	assert.Equal(t, "v2", r.BM25Engine)
	assert.Equal(t, "768", r.Effective["NORNICDB_EMBEDDING_DIMENSIONS"])
	assert.Equal(t, "0.8", r.Effective["NORNICDB_SEARCH_MIN_SIMILARITY"])
	assert.Equal(t, "v2", r.Effective["NORNICDB_SEARCH_BM25_ENGINE"])
}

func TestResolve_DefaultDimensionsAndIgnoredOverrides(t *testing.T) {
	t.Setenv("NORNICDB_SEARCH_BM25_ENGINE", "unexpected")
	global := config.LoadDefaults()
	global.Memory.EmbeddingDimensions = 0
	global.Memory.SearchMinSimilarity = 0.55

	r := Resolve(global, map[string]string{
		"NOT_ALLOWED":                    "ignored",
		"NORNICDB_EMBEDDING_DIMENSIONS":  "-5",
		"NORNICDB_SEARCH_BM25_ENGINE":    "v1",
		"NORNICDB_SEARCH_MIN_SIMILARITY": "bad",
	})
	require.NotNil(t, r)
	assert.Equal(t, 1024, r.EmbeddingDimensions)
	assert.Equal(t, 0.55, r.SearchMinSimilarity)
	assert.Equal(t, "v1", r.BM25Engine)
	_, ok := r.Effective["NOT_ALLOWED"]
	assert.False(t, ok)
}

func TestApplyOverride(t *testing.T) {
	r := &ResolvedDbConfig{
		EmbeddingDimensions: 1024,
		SearchMinSimilarity: 0.5,
		BM25Engine:          "v2",
		Effective:           map[string]string{},
	}

	applyOverride(r, "NORNICDB_EMBEDDING_DIMENSIONS", " 2048 ")
	assert.Equal(t, 2048, r.EmbeddingDimensions)

	applyOverride(r, "NORNICDB_EMBEDDING_DIMENSIONS", "0")
	assert.Equal(t, 1024, r.EmbeddingDimensions)

	applyOverride(r, "NORNICDB_SEARCH_MIN_SIMILARITY", "0.75")
	assert.Equal(t, 0.75, r.SearchMinSimilarity)

	applyOverride(r, "NORNICDB_SEARCH_MIN_SIMILARITY", "0")
	assert.Equal(t, 0.0, r.SearchMinSimilarity)

	applyOverride(r, "NORNICDB_SEARCH_BM25_ENGINE", "V1")
	assert.Equal(t, "v1", r.BM25Engine)

	applyOverride(r, "NORNICDB_EMBEDDING_ENABLED", "1")
	assert.Equal(t, "v1", r.BM25Engine)

	applyOverride(r, "UNKNOWN_KEY", "value")
	assert.Equal(t, 1024, r.EmbeddingDimensions)
}

func TestIsAllowedKey(t *testing.T) {
	assert.True(t, IsAllowedKey("NORNICDB_EMBEDDING_MODEL"))
	assert.True(t, IsAllowedKey("NORNICDB_SEARCH_MIN_SIMILARITY"))
	assert.True(t, IsAllowedKey("NORNICDB_EMBEDDING_API_KEY"))
	assert.True(t, IsAllowedKey("NORNICDB_SEARCH_BM25_ENABLED"))
	assert.True(t, IsAllowedKey("NORNICDB_SEARCH_BM25_WARMING"))
	assert.True(t, IsAllowedKey("NORNICDB_SEARCH_VECTOR_ENABLED"))
	assert.True(t, IsAllowedKey("NORNICDB_SEARCH_VECTOR_WARMING"))
	assert.False(t, IsAllowedKey("UNKNOWN_KEY"))
}

// TestResolveSearchFlags_Defaults — when no overrides are present, the
// resolved config mirrors the global default (true / startup).
func TestResolveSearchFlags_Defaults(t *testing.T) {
	global := config.LoadDefaults()
	r := Resolve(global, nil)
	require.NotNil(t, r)
	assert.True(t, r.BM25Enabled)
	assert.True(t, r.VectorEnabled)
	assert.Equal(t, "startup", r.BM25Warming)
	assert.Equal(t, "startup", r.VectorWarming)
	// Effective map mirrors all four keys.
	assert.Equal(t, "true", r.Effective["NORNICDB_SEARCH_BM25_ENABLED"])
	assert.Equal(t, "startup", r.Effective["NORNICDB_SEARCH_BM25_WARMING"])
	assert.Equal(t, "true", r.Effective["NORNICDB_SEARCH_VECTOR_ENABLED"])
	assert.Equal(t, "startup", r.Effective["NORNICDB_SEARCH_VECTOR_WARMING"])
}

// TestResolveSearchFlags_OverrideMatrix — the load-bearing guarantee.
// Per-DB overrides win over the global default in BOTH directions: an override
// of true turns on a globally-disabled index; an override of false turns off
// a globally-enabled one. Same for warming.
func TestResolveSearchFlags_OverrideMatrix(t *testing.T) {
	type row struct {
		name        string
		globalBM25  bool
		globalVec   bool
		globalBM25W string
		globalVecW  string
		overrides   map[string]string
		wantBM25    bool
		wantVec     bool
		wantBM25W   string
		wantVecW    string
	}
	cases := []row{
		{
			name:       "both global true, no override → both enabled startup",
			globalBM25: true, globalVec: true, globalBM25W: "startup", globalVecW: "startup",
			overrides: nil,
			wantBM25:  true, wantVec: true, wantBM25W: "startup", wantVecW: "startup",
		},
		{
			name:       "both global false, no override → both disabled",
			globalBM25: false, globalVec: false, globalBM25W: "startup", globalVecW: "startup",
			overrides: nil,
			wantBM25:  false, wantVec: false,
		},
		{
			name:       "global vector false, per-DB override true → vector ON",
			globalBM25: true, globalVec: false, globalBM25W: "startup", globalVecW: "startup",
			overrides: map[string]string{"NORNICDB_SEARCH_VECTOR_ENABLED": "true"},
			wantBM25:  true, wantVec: true,
		},
		{
			name:       "global vector false, per-DB override lazy → vector ON, warming lazy",
			globalBM25: true, globalVec: false, globalBM25W: "startup", globalVecW: "startup",
			overrides: map[string]string{
				"NORNICDB_SEARCH_VECTOR_ENABLED": "true",
				"NORNICDB_SEARCH_VECTOR_WARMING": "lazy",
			},
			wantBM25: true, wantVec: true, wantBM25W: "startup", wantVecW: "lazy",
		},
		{
			name:       "global true, per-DB override false → disabled",
			globalBM25: true, globalVec: true, globalBM25W: "startup", globalVecW: "startup",
			overrides: map[string]string{"NORNICDB_SEARCH_VECTOR_ENABLED": "false"},
			wantBM25:  true, wantVec: false,
		},
		{
			name:       "global startup, per-DB lazy → lazy",
			globalBM25: true, globalVec: true, globalBM25W: "startup", globalVecW: "startup",
			overrides: map[string]string{"NORNICDB_SEARCH_VECTOR_WARMING": "lazy"},
			wantBM25:  true, wantVec: true, wantBM25W: "startup", wantVecW: "lazy",
		},
		{
			name:       "global lazy, per-DB startup → startup",
			globalBM25: true, globalVec: true, globalBM25W: "lazy", globalVecW: "lazy",
			overrides: map[string]string{
				"NORNICDB_SEARCH_BM25_WARMING":   "startup",
				"NORNICDB_SEARCH_VECTOR_WARMING": "startup",
			},
			wantBM25: true, wantVec: true, wantBM25W: "startup", wantVecW: "startup",
		},
		{
			name:       "independent per-key flips: vector true, BM25 false from globals true,true",
			globalBM25: true, globalVec: true, globalBM25W: "startup", globalVecW: "startup",
			overrides: map[string]string{
				"NORNICDB_SEARCH_BM25_ENABLED":   "false",
				"NORNICDB_SEARCH_VECTOR_ENABLED": "true",
			},
			wantBM25: false, wantVec: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			global := config.LoadDefaults()
			global.Memory.SearchBM25Enabled = c.globalBM25
			global.Memory.SearchVectorEnabled = c.globalVec
			global.Memory.SearchBM25Warming = c.globalBM25W
			global.Memory.SearchVectorWarming = c.globalVecW
			r := Resolve(global, c.overrides)
			require.NotNil(t, r)
			assert.Equal(t, c.wantBM25, r.BM25Enabled, "BM25Enabled")
			assert.Equal(t, c.wantVec, r.VectorEnabled, "VectorEnabled")
			if c.wantBM25W != "" {
				assert.Equal(t, c.wantBM25W, r.BM25Warming, "BM25Warming")
			}
			if c.wantVecW != "" {
				assert.Equal(t, c.wantVecW, r.VectorWarming, "VectorWarming")
			}
		})
	}
}

// TestResolveSearchFlags_BoolFallback — bogus boolean strings don't silently
// flip the value; the global default wins.
func TestResolveSearchFlags_BoolFallback(t *testing.T) {
	global := config.LoadDefaults()
	global.Memory.SearchVectorEnabled = true
	r := Resolve(global, map[string]string{
		"NORNICDB_SEARCH_VECTOR_ENABLED": "yes", // not a recognised bool literal
	})
	require.NotNil(t, r)
	assert.True(t, r.VectorEnabled, "bogus override should not flip the global default")
}

// TestEnumValidation — IsValidEnumValue accepts startup/lazy and rejects others.
func TestEnumValidation(t *testing.T) {
	ok, _ := IsValidEnumValue("NORNICDB_SEARCH_VECTOR_WARMING", "startup")
	assert.True(t, ok)
	ok, _ = IsValidEnumValue("NORNICDB_SEARCH_VECTOR_WARMING", "LAZY")
	assert.True(t, ok, "case-insensitive match")
	ok, allowed := IsValidEnumValue("NORNICDB_SEARCH_VECTOR_WARMING", "asap")
	assert.False(t, ok)
	assert.Equal(t, "startup,lazy", allowed)
	// Non-enum keys: no value-level validation.
	ok, _ = IsValidEnumValue("NORNICDB_SEARCH_BM25_ENABLED", "anything")
	assert.True(t, ok)
}

// TestResolve_PrecedenceLadder walks every level of the precedence
// ladder for the four search flags and asserts each level overrides
// the levels below it. Order, lowest → highest:
//
//  1. defaults     (config.LoadDefaults; bm25=true, vector=true, both startup)
//  2. global       (cfg.Memory.Search* — populated from YAML/env)
//  3. per-DB       (the `overrides` map — admin API store / YAML databases:)
//  4. CLI          (cfg.CLIOverrides — cmd.Flags().Changed at boot)
//
// Each subtest holds the levels above the one under test as no-op
// (matching the level below) so the ONE level we're exercising is
// what's actually demonstrated to win.
func TestResolve_PrecedenceLadder(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		// No overlay anywhere: the resolved values mirror LoadDefaults.
		global := config.LoadDefaults()
		r := Resolve(global, nil)
		assert.True(t, r.BM25Enabled, "default BM25 must be enabled")
		assert.True(t, r.VectorEnabled, "default vector must be enabled")
		assert.Equal(t, "startup", r.BM25Warming, "default warming")
		assert.Equal(t, "startup", r.VectorWarming, "default warming")
	})

	t.Run("global overrides defaults", func(t *testing.T) {
		// Stand-in for YAML/env loading: cfg.Memory.Search* differs
		// from defaults; no per-DB or CLI overrides.
		global := config.LoadDefaults()
		global.Memory.SearchBM25Enabled = false
		global.Memory.SearchVectorEnabled = false
		global.Memory.SearchBM25Warming = "lazy"
		global.Memory.SearchVectorWarming = "lazy"
		r := Resolve(global, nil)
		assert.False(t, r.BM25Enabled, "global must override default")
		assert.False(t, r.VectorEnabled, "global must override default")
		assert.Equal(t, "lazy", r.BM25Warming)
		assert.Equal(t, "lazy", r.VectorWarming)
	})

	t.Run("per-DB overrides global", func(t *testing.T) {
		// Global says enabled; per-DB store says disabled. Per-DB wins.
		global := config.LoadDefaults()
		global.Memory.SearchBM25Enabled = true
		global.Memory.SearchVectorEnabled = true
		overrides := map[string]string{
			"NORNICDB_SEARCH_BM25_ENABLED":   "false",
			"NORNICDB_SEARCH_VECTOR_ENABLED": "false",
			"NORNICDB_SEARCH_BM25_WARMING":   "lazy",
			"NORNICDB_SEARCH_VECTOR_WARMING": "lazy",
		}
		r := Resolve(global, overrides)
		assert.False(t, r.BM25Enabled, "per-DB must override global")
		assert.False(t, r.VectorEnabled, "per-DB must override global")
		assert.Equal(t, "lazy", r.BM25Warming)
		assert.Equal(t, "lazy", r.VectorWarming)
	})

	t.Run("per-DB overrides global the other direction", func(t *testing.T) {
		// Belt-and-suspenders: ensure per-DB can also turn ON something
		// global says is off, not just off-something-on.
		global := config.LoadDefaults()
		global.Memory.SearchBM25Enabled = false
		global.Memory.SearchVectorEnabled = false
		overrides := map[string]string{
			"NORNICDB_SEARCH_BM25_ENABLED":   "true",
			"NORNICDB_SEARCH_VECTOR_ENABLED": "true",
		}
		r := Resolve(global, overrides)
		assert.True(t, r.BM25Enabled, "per-DB true must override global false")
		assert.True(t, r.VectorEnabled, "per-DB true must override global false")
	})

	t.Run("CLI trumps per-DB and global (kill switch)", func(t *testing.T) {
		// The lab-incident shape: tenant has search ON via per-DB store,
		// operator boots with --search-*-enabled=false to mitigate OOM.
		// CLI must win.
		global := config.LoadDefaults()
		global.Memory.SearchBM25Enabled = true
		global.Memory.SearchVectorEnabled = true
		global.CLIOverrides = map[string]string{
			"NORNICDB_SEARCH_BM25_ENABLED":   "false",
			"NORNICDB_SEARCH_VECTOR_ENABLED": "false",
		}
		// Per-DB explicitly says enabled — CLI must still win.
		overrides := map[string]string{
			"NORNICDB_SEARCH_BM25_ENABLED":   "true",
			"NORNICDB_SEARCH_VECTOR_ENABLED": "true",
		}
		r := Resolve(global, overrides)
		assert.False(t, r.BM25Enabled,
			"CLI kill switch must override per-DB enable; got=%+v", r)
		assert.False(t, r.VectorEnabled,
			"CLI kill switch must override per-DB enable; got=%+v", r)
		// Effective map should reflect CLI's value as the last writer.
		assert.Equal(t, "false", r.Effective["NORNICDB_SEARCH_BM25_ENABLED"])
		assert.Equal(t, "false", r.Effective["NORNICDB_SEARCH_VECTOR_ENABLED"])
	})

	t.Run("CLI trumps per-DB the other direction", func(t *testing.T) {
		// Operator wants search ON globally even though a per-DB entry
		// says off. Less common but symmetric — a debug or recovery
		// session could need it.
		global := config.LoadDefaults()
		global.Memory.SearchBM25Enabled = false
		global.Memory.SearchVectorEnabled = false
		global.CLIOverrides = map[string]string{
			"NORNICDB_SEARCH_BM25_ENABLED":   "true",
			"NORNICDB_SEARCH_VECTOR_ENABLED": "true",
		}
		overrides := map[string]string{
			"NORNICDB_SEARCH_BM25_ENABLED":   "false",
			"NORNICDB_SEARCH_VECTOR_ENABLED": "false",
		}
		r := Resolve(global, overrides)
		assert.True(t, r.BM25Enabled, "CLI true must trump per-DB false")
		assert.True(t, r.VectorEnabled, "CLI true must trump per-DB false")
	})

	t.Run("CLI applies even without per-DB override", func(t *testing.T) {
		// CLI must work whether or not the dbconfig store has any
		// row for this DB. Single-source environments (no admin API
		// usage, no YAML databases: block) are common.
		global := config.LoadDefaults()
		global.Memory.SearchBM25Enabled = true
		global.CLIOverrides = map[string]string{
			"NORNICDB_SEARCH_BM25_ENABLED": "false",
		}
		r := Resolve(global, nil)
		assert.False(t, r.BM25Enabled, "CLI must apply even with no per-DB overrides")
	})

	t.Run("CLI scoped per-key — partial overrides don't bleed", func(t *testing.T) {
		// Operator only flips BM25 via CLI. Vector flag must follow
		// the next-highest level (per-DB here), not get coerced by
		// CLI's BM25 entry.
		global := config.LoadDefaults()
		global.Memory.SearchBM25Enabled = true
		global.Memory.SearchVectorEnabled = true
		global.CLIOverrides = map[string]string{
			"NORNICDB_SEARCH_BM25_ENABLED": "false",
		}
		overrides := map[string]string{
			"NORNICDB_SEARCH_VECTOR_ENABLED": "false",
		}
		r := Resolve(global, overrides)
		assert.False(t, r.BM25Enabled, "CLI overrides BM25 only")
		assert.False(t, r.VectorEnabled, "per-DB overrides vector — CLI didn't touch it")
	})

	t.Run("CLI warming override beats per-DB warming", func(t *testing.T) {
		// Warming is enum-typed (startup/lazy). Make sure the CLI path
		// applies it the same way as the boolean path.
		global := config.LoadDefaults()
		global.Memory.SearchBM25Warming = "startup"
		global.CLIOverrides = map[string]string{
			"NORNICDB_SEARCH_BM25_WARMING": "lazy",
		}
		overrides := map[string]string{
			"NORNICDB_SEARCH_BM25_WARMING": "startup",
		}
		r := Resolve(global, overrides)
		assert.Equal(t, "lazy", r.BM25Warming, "CLI warming override wins")
	})
}
