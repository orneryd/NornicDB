package config

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// deprecationWarningAllocationBudget bounds the steady-state allocation that the
// deprecated NORNICDB_MEMORY_LIMIT warning may add to one LoadFromEnv call.
//
// Configuration loads happen per executor construction (see
// cypher.NewStorageExecutor), so the warning must not rebuild the embedded
// localization catalogs on every call. After the one-time catalog load the
// warning costs one render, which is orders of magnitude below this budget.
const deprecationWarningAllocationBudget = 2 << 20

// canonicalMemoryLimitEnv aliases the legacy and canonical Go runtime memory
// limit environment variables so a test starts from a known alias state.
func canonicalMemoryLimitEnv(t *testing.T) {
	t.Helper()
	t.Setenv("NORNICDB_MEMORY_LIMIT", "")
	t.Setenv("NORNICDB_GO_MEMORY_LIMIT_BYTES", "")
}

// unsetEnv removes name for the duration of the test and restores the previous
// state afterwards, which t.Setenv cannot express.
func unsetEnv(t *testing.T, name string) {
	t.Helper()
	previous, existed := os.LookupEnv(name)
	if err := os.Unsetenv(name); err != nil {
		t.Fatalf("unset %s: %v", name, err)
	}
	t.Cleanup(func() {
		if existed {
			if err := os.Setenv(name, previous); err != nil {
				t.Errorf("restore %s: %v", name, err)
			}
			return
		}
		if err := os.Unsetenv(name); err != nil {
			t.Errorf("clear %s: %v", name, err)
		}
	})
}

// captureWarnings records slog warnings emitted while it is installed.
func captureWarnings(t *testing.T) func() string {
	t.Helper()
	var buffer bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buffer, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() {
		slog.SetDefault(previous)
	})
	return func() string {
		return buffer.String()
	}
}

// memoryLimitWarningCount reports how many capture lines rendered the
// deprecated memory-limit warning. Both the source and translated catalogs
// mention the canonical variable name, so the count is locale independent.
func memoryLimitWarningCount(captured string) int {
	count := 0
	for _, line := range strings.Split(captured, "\n") {
		if strings.Contains(line, "NORNICDB_GO_MEMORY_LIMIT_BYTES") {
			count++
		}
	}
	return count
}

// loadFromEnvAllocBytes reports the bytes LoadFromEnv allocated for one call.
func loadFromEnvAllocBytes() uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	cfg := LoadFromEnv()
	runtime.ReadMemStats(&after)
	if cfg == nil {
		panic("LoadFromEnv returned nil")
	}
	return after.TotalAlloc - before.TotalAlloc
}

// minLoadFromEnvAllocBytes reports the smallest byte count observed over runs
// LoadFromEnv calls, which rejects one-off background allocation noise.
func minLoadFromEnvAllocBytes(runs int) uint64 {
	smallest := ^uint64(0)
	for i := 0; i < runs; i++ {
		if allocated := loadFromEnvAllocBytes(); allocated < smallest {
			smallest = allocated
		}
	}
	return smallest
}

// TestLoadFromEnvDeprecatedMemoryLimitWarningAllocationCost reproduces NOR-410's
// per-transaction warning cost: a configuration load that warns about the
// deprecated NORNICDB_MEMORY_LIMIT must cost about the same as a configuration
// load using the canonical NORNICDB_GO_MEMORY_LIMIT_BYTES, instead of rebuilding
// every embedded localization catalog.
func TestLoadFromEnvDeprecatedMemoryLimitWarningAllocationCost(t *testing.T) {
	unsetEnv(t, localization.EnvLanguage)
	canonicalMemoryLimitEnv(t)

	// Warm-up pays the one-time catalog load and any other process-wide
	// initialization; the measurement below is the steady-state cost that an
	// executor construction pays per transaction.
	t.Setenv("NORNICDB_MEMORY_LIMIT", "1024")
	_ = LoadFromEnv()
	legacyBytes := minLoadFromEnvAllocBytes(3)

	canonicalMemoryLimitEnv(t)
	t.Setenv("NORNICDB_GO_MEMORY_LIMIT_BYTES", "1073741824")
	canonicalBytes := minLoadFromEnvAllocBytes(3)

	t.Logf("LoadFromEnv allocation: deprecated NORNICDB_MEMORY_LIMIT=%d bytes, canonical NORNICDB_GO_MEMORY_LIMIT_BYTES=%d bytes, budget=%d bytes",
		legacyBytes, canonicalBytes, uint64(deprecationWarningAllocationBudget))

	if legacyBytes > canonicalBytes+deprecationWarningAllocationBudget {
		t.Fatalf("deprecated memory limit warning allocates %d bytes per LoadFromEnv call versus %d bytes for the canonical setting (budget %d bytes): the warning rebuilds the localization catalogs on every configuration load",
			legacyBytes, canonicalBytes, uint64(deprecationWarningAllocationBudget))
	}
}

// TestLoadFromEnvDeprecatedMemoryLimitWarningObservable proves the configuration
// owner still reports the deprecation through the process logger on every load
// that reads the legacy variable, and stays silent for the canonical variable.
func TestLoadFromEnvDeprecatedMemoryLimitWarningObservable(t *testing.T) {
	unsetEnv(t, localization.EnvLanguage)
	canonicalMemoryLimitEnv(t)
	captured := captureWarnings(t)

	t.Setenv("NORNICDB_MEMORY_LIMIT", "1024")
	cfg := LoadFromEnv()
	if cfg.Memory.RuntimeLimit != 1024<<20 {
		t.Fatalf("legacy runtime limit = %d, want %d", cfg.Memory.RuntimeLimit, int64(1024)<<20)
	}
	_ = LoadFromEnv()
	if got := memoryLimitWarningCount(captured()); got != 2 {
		t.Fatalf("deprecation warnings after two legacy loads = %d, want 2; captured:\n%s", got, captured())
	}

	canonicalMemoryLimitEnv(t)
	t.Setenv("NORNICDB_GO_MEMORY_LIMIT_BYTES", "1073741824")
	cfg = LoadFromEnv()
	if cfg.Memory.RuntimeLimit != 1073741824 {
		t.Fatalf("canonical runtime limit = %d, want %d", cfg.Memory.RuntimeLimit, int64(1073741824))
	}
	if got := memoryLimitWarningCount(captured()); got != 2 {
		t.Fatalf("canonical setting emitted a deprecation warning: total %d, want 2; captured:\n%s", got, captured())
	}
}

// TestWarnDeprecatedMemorySettingRendersConfiguredLanguage proves the cached
// warning renderer still honors the configured process language.
func TestWarnDeprecatedMemorySettingRendersConfiguredLanguage(t *testing.T) {
	unsetEnv(t, localization.EnvLanguage)
	unsetEnv(t, "LANG")
	unsetEnv(t, "LC_ALL")
	captured := captureWarnings(t)

	warnDeprecatedMemorySetting("es-ES", localization.NornicDBCLIEnvMemoryLimitDeprecated())

	logged := captured()
	if strings.Count(logged, "NORNICDB_GO_MEMORY_LIMIT_BYTES") != 1 {
		t.Fatalf("expected exactly one rendered warning, captured:\n%s", logged)
	}
	if !strings.Contains(logged, "obsoleto") {
		t.Fatalf("warning was not rendered in the configured es-ES catalog, captured:\n%s", logged)
	}
}

// TestLoadFromFileDeprecatedYAMLRuntimeLimitWarning covers the YAML call site of
// the shared warning renderer: a file that still sets memory.runtime_limit keeps
// reporting the deprecation in the file's configured language.
func TestLoadFromFileDeprecatedYAMLRuntimeLimitWarning(t *testing.T) {
	unsetEnv(t, localization.EnvLanguage)
	canonicalMemoryLimitEnv(t)
	captured := captureWarnings(t)

	path := filepath.Join(t.TempDir(), "config.yaml")
	file := "localization:\n  language: es-ES\nmemory:\n  runtime_limit: 1024\n"
	if err := os.WriteFile(path, []byte(file), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}
	if cfg.Memory.RuntimeLimit != 1024<<20 {
		t.Fatalf("YAML runtime limit = %d, want %d", cfg.Memory.RuntimeLimit, int64(1024)<<20)
	}

	logged := captured()
	if strings.Count(logged, "memory.go_runtime_limit_bytes") != 1 {
		t.Fatalf("expected exactly one YAML deprecation warning, captured:\n%s", logged)
	}
	if !strings.Contains(logged, "obsoleto") {
		t.Fatalf("YAML warning was not rendered in the configured es-ES catalog, captured:\n%s", logged)
	}
}

// TestDeprecatedMemoryLimitWarningConcurrentLoads proves concurrent executor
// constructions may share the process-wide warning renderer safely.
func TestDeprecatedMemoryLimitWarningConcurrentLoads(t *testing.T) {
	unsetEnv(t, localization.EnvLanguage)
	canonicalMemoryLimitEnv(t)
	captured := captureWarnings(t)

	const loads = 8
	t.Setenv("NORNICDB_MEMORY_LIMIT", "1024")
	var wait sync.WaitGroup
	wait.Add(loads)
	for i := 0; i < loads; i++ {
		go func() {
			defer wait.Done()
			if cfg := LoadFromEnv(); cfg.Memory.RuntimeLimit != 1024<<20 {
				t.Errorf("concurrent legacy runtime limit = %d, want %d", cfg.Memory.RuntimeLimit, int64(1024)<<20)
			}
		}()
	}
	wait.Wait()

	if got := memoryLimitWarningCount(captured()); got != loads {
		t.Fatalf("deprecation warnings after %d concurrent loads = %d; captured:\n%s", loads, got, captured())
	}
}
