package config

import (
	"os"
	"testing"
	"time"
)

// =============================================================================
// parseMemorySize Tests
// =============================================================================

func TestParseMemorySize(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  int64
	}{
		// Bytes
		{"bytes numeric", "1024", 1024},
		{"bytes with B suffix", "1024B", 1024},
		{"bytes lowercase", "1024b", 1024},

		// Kilobytes
		{"kilobytes K", "1K", 1024},
		{"kilobytes KB", "1KB", 1024},
		{"kilobytes lowercase", "1kb", 1024},
		{"kilobytes large", "512K", 512 * 1024},

		// Megabytes
		{"megabytes M", "1M", 1024 * 1024},
		{"megabytes MB", "1MB", 1024 * 1024},
		{"megabytes lowercase", "512mb", 512 * 1024 * 1024},
		{"megabytes large", "256M", 256 * 1024 * 1024},

		// Gigabytes
		{"gigabytes G", "1G", 1024 * 1024 * 1024},
		{"gigabytes GB", "1GB", 1024 * 1024 * 1024},
		{"gigabytes lowercase", "2gb", 2 * 1024 * 1024 * 1024},
		{"gigabytes large", "4G", 4 * 1024 * 1024 * 1024},

		// Terabytes
		{"terabytes T", "1T", 1024 * 1024 * 1024 * 1024},
		{"terabytes TB", "1TB", 1024 * 1024 * 1024 * 1024},

		// Unlimited/Zero
		{"zero", "0", 0},
		{"unlimited", "unlimited", 0},
		{"unlimited caps", "UNLIMITED", 0},
		{"empty string", "", 0},

		// Whitespace handling
		{"whitespace", "  2GB  ", 2 * 1024 * 1024 * 1024},

		// Invalid returns 0
		{"invalid chars", "abc", 0},
		// Negative values parse but result in negative (caller should validate)
		{"negative", "-1GB", -1 * 1024 * 1024 * 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseMemorySize(tt.input)
			if got != tt.want {
				t.Errorf("parseMemorySize(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

// =============================================================================
// FormatMemorySize Tests
// =============================================================================

func TestFormatMemorySize(t *testing.T) {
	tests := []struct {
		name  string
		bytes int64
		want  string
	}{
		{"zero", 0, "0 B"},
		{"bytes", 512, "512 B"},
		{"kilobytes", 1024, "1.00 KB"},
		{"kilobytes fractional", 1536, "1.50 KB"},
		{"megabytes", 1024 * 1024, "1.00 MB"},
		{"megabytes fractional", 512 * 1024 * 1024, "512.00 MB"},
		{"gigabytes", 1024 * 1024 * 1024, "1.00 GB"},
		{"gigabytes large", 4 * 1024 * 1024 * 1024, "4.00 GB"},
		{"terabytes", 1024 * 1024 * 1024 * 1024, "1.00 TB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatMemorySize(tt.bytes)
			if got != tt.want {
				t.Errorf("FormatMemorySize(%d) = %q, want %q", tt.bytes, got, tt.want)
			}
		})
	}
}

// =============================================================================
// LoadFromEnv Runtime Memory Tests
// =============================================================================

func TestLoadFromEnv_RuntimeMemory(t *testing.T) {
	// Clear environment first
	envVars := []string{
		"NORNICDB_MEMORY_LIMIT",
		"NORNICDB_GC_PERCENT",
		"NORNICDB_POOL_ENABLED",
		"NORNICDB_POOL_MAX_SIZE",
		"NORNICDB_QUERY_CACHE_ENABLED",
		"NORNICDB_QUERY_CACHE_SIZE",
		"NORNICDB_QUERY_CACHE_TTL",
	}
	for _, v := range envVars {
		os.Unsetenv(v)
	}

	t.Run("defaults", func(t *testing.T) {
		cfg := LoadFromEnv()

		if cfg.Memory.RuntimeLimit != 0 {
			t.Errorf("RuntimeLimit = %d, want 0 (unlimited)", cfg.Memory.RuntimeLimit)
		}
		if cfg.Memory.GCPercent != 100 {
			t.Errorf("GCPercent = %d, want 100", cfg.Memory.GCPercent)
		}
		if !cfg.Memory.PoolEnabled {
			t.Error("PoolEnabled should be true by default")
		}
		if cfg.Memory.PoolMaxSize != 1000 {
			t.Errorf("PoolMaxSize = %d, want 1000", cfg.Memory.PoolMaxSize)
		}
		if !cfg.Memory.QueryCacheEnabled {
			t.Error("QueryCacheEnabled should be true by default")
		}
		if cfg.Memory.QueryCacheSize != 1000 {
			t.Errorf("QueryCacheSize = %d, want 1000", cfg.Memory.QueryCacheSize)
		}
		if cfg.Memory.QueryCacheTTL != 5*time.Minute {
			t.Errorf("QueryCacheTTL = %v, want 5m", cfg.Memory.QueryCacheTTL)
		}
	})

	t.Run("memory limit from env", func(t *testing.T) {
		os.Setenv("NORNICDB_MEMORY_LIMIT", "2GB")
		defer os.Unsetenv("NORNICDB_MEMORY_LIMIT")

		cfg := LoadFromEnv()
		want := int64(2 * 1024 * 1024 * 1024)
		if cfg.Memory.RuntimeLimit != want {
			t.Errorf("RuntimeLimit = %d, want %d", cfg.Memory.RuntimeLimit, want)
		}
		if cfg.Memory.RuntimeLimitStr != "2GB" {
			t.Errorf("RuntimeLimitStr = %q, want %q", cfg.Memory.RuntimeLimitStr, "2GB")
		}
	})

	t.Run("gc percent from env", func(t *testing.T) {
		os.Setenv("NORNICDB_GC_PERCENT", "50")
		defer os.Unsetenv("NORNICDB_GC_PERCENT")

		cfg := LoadFromEnv()
		if cfg.Memory.GCPercent != 50 {
			t.Errorf("GCPercent = %d, want 50", cfg.Memory.GCPercent)
		}
	})

	t.Run("pool enabled false", func(t *testing.T) {
		os.Setenv("NORNICDB_POOL_ENABLED", "false")
		defer os.Unsetenv("NORNICDB_POOL_ENABLED")

		cfg := LoadFromEnv()
		if cfg.Memory.PoolEnabled {
			t.Error("PoolEnabled should be false")
		}
	})

	t.Run("pool max size from env", func(t *testing.T) {
		os.Setenv("NORNICDB_POOL_MAX_SIZE", "500")
		defer os.Unsetenv("NORNICDB_POOL_MAX_SIZE")

		cfg := LoadFromEnv()
		if cfg.Memory.PoolMaxSize != 500 {
			t.Errorf("PoolMaxSize = %d, want 500", cfg.Memory.PoolMaxSize)
		}
	})

	t.Run("query cache size from env", func(t *testing.T) {
		os.Setenv("NORNICDB_QUERY_CACHE_SIZE", "2000")
		defer os.Unsetenv("NORNICDB_QUERY_CACHE_SIZE")

		cfg := LoadFromEnv()
		if cfg.Memory.QueryCacheSize != 2000 {
			t.Errorf("QueryCacheSize = %d, want 2000", cfg.Memory.QueryCacheSize)
		}
	})

	t.Run("query cache ttl from env", func(t *testing.T) {
		os.Setenv("NORNICDB_QUERY_CACHE_TTL", "10m")
		defer os.Unsetenv("NORNICDB_QUERY_CACHE_TTL")

		cfg := LoadFromEnv()
		if cfg.Memory.QueryCacheTTL != 10*time.Minute {
			t.Errorf("QueryCacheTTL = %v, want 10m", cfg.Memory.QueryCacheTTL)
		}
	})
}

// =============================================================================
// ApplyRuntimeMemory Tests
// =============================================================================

func TestMemoryConfig_ApplyRuntimeMemory(t *testing.T) {
	// Apply should not panic with defaults
	cfg := &MemoryConfig{
		RuntimeLimit: 0,
		GCPercent:    100,
	}
	cfg.ApplyRuntimeMemory() // Should be no-op for defaults

	cfg2 := &MemoryConfig{
		RuntimeLimit: 1024 * 1024 * 1024, // 1GB
		GCPercent:    50,
	}
	cfg2.ApplyRuntimeMemory() // Should set memory limit and GC percent

	// Reset to defaults
	cfg.GCPercent = 100
	cfg.ApplyRuntimeMemory()
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkParseMemorySize(b *testing.B) {
	inputs := []string{"2GB", "512MB", "1024", "unlimited", "1TB"}

	for _, input := range inputs {
		b.Run(input, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				parseMemorySize(input)
			}
		})
	}
}

func BenchmarkFormatMemorySize(b *testing.B) {
	sizes := []int64{1024, 1024 * 1024, 1024 * 1024 * 1024}

	for _, size := range sizes {
		b.Run(FormatMemorySize(size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				FormatMemorySize(size)
			}
		})
	}
}
