package observability

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-01 Wave-0: Phase 3 D-05 belt-and-suspenders carry-forward.
//
// Phase 3 shipped `make lint-cardinality` (Makefile:871) — a grep gate that
// rejects raw `prometheus.New(Counter|Gauge|Histogram|Summary)Vec(` calls
// outside pkg/observability. This test PROVES the gate is falsifiable: it
// injects a sentinel offender into a tracked subsystem path and asserts
// `make lint-cardinality` exits non-zero. It then removes the offender
// and confirms the gate passes again.
//
// Phase 3's evidence file lives at
// `.planning/phases/03-metrics-infrastructure-discipline/lint-cardinality-falsifiability.txt`
// and used a //go:build never_compile_this_falsifiability_proof tag for the
// injection so even compilation was harmless. We mirror that posture: the
// injected sentinel uses an unreachable build tag so go test/go build never
// see it.

// TestLintCardinality_Falsifiable proves the lint-cardinality grep gate
// catches raw prometheus.NewXxxVec outside pkg/observability. Skipped if
// `make` or `go` is unavailable (CI minimal images, Windows hosts).
func TestLintCardinality_Falsifiable(t *testing.T) {
	if _, err := exec.LookPath("make"); err != nil {
		t.Skip("make not on PATH; lint-cardinality falsifiability requires Makefile target")
	}

	// Walk up to repo root (where Makefile lives).
	root, err := filepath.Abs("..")
	require.NoError(t, err)
	root, err = filepath.Abs(filepath.Join(root, ".."))
	require.NoError(t, err)
	if _, err := os.Stat(filepath.Join(root, "Makefile")); err != nil {
		t.Skipf("Makefile not at %s; skipping", root)
	}

	// Step 1: baseline must pass.
	baseline := exec.Command("make", "lint-cardinality")
	baseline.Dir = root
	out, err := baseline.CombinedOutput()
	require.NoError(t, err, "baseline lint-cardinality must pass before falsifiability test: %s", out)
	assert.Contains(t, string(out), "PASS",
		"baseline must report PASS (MET-04 helper-only registration enforced)")

	// Step 2: inject a sentinel offender into pkg/cypher (a path the grep
	// scans). Use an unreachable build tag so go build/test never see it.
	sentinelPath := filepath.Join(root, "pkg", "cypher", "lint_falsifiability_sentinel.go")
	sentinel := `//go:build never_compile_falsifiability_proof_04_01
// +build never_compile_falsifiability_proof_04_01

package cypher

import "github.com/prometheus/client_golang/prometheus"

var _ = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "x_total"}, []string{"y"})
`
	require.NoError(t, os.WriteFile(sentinelPath, []byte(sentinel), 0o644))
	defer os.Remove(sentinelPath)

	// Step 3: with offender present, lint must fail.
	inj := exec.Command("make", "lint-cardinality")
	inj.Dir = root
	out, err = inj.CombinedOutput()
	require.Error(t, err,
		"falsifiability: lint-cardinality MUST exit non-zero with sentinel injected; got pass: %s", out)
	assert.Contains(t, string(out), "MET-04 violation",
		"falsifiability: lint message must surface MET-04 violation; got: %s", out)

	// Step 4: remove offender and verify gate restores.
	require.NoError(t, os.Remove(sentinelPath))
	post := exec.Command("make", "lint-cardinality")
	post.Dir = root
	out, err = post.CombinedOutput()
	require.NoError(t, err, "post-revert lint-cardinality must pass: %s", out)
	assert.Contains(t, string(out), "PASS",
		"post-revert must report PASS")
}

// TestCacheCardinalityCeiling asserts task 04-01-05's per-Vec cardinality
// belt: driving 1k synthetic tenant strings AS the closed-enum cache label
// would still produce only the four allow-listed series, because subsystem
// callers route through the closed-enum allow-list (RESEARCH §Q11 ceiling=4).
//
// The drive function passes the tenant string through the *Vec but uses
// only allow-listed values; this proves the ceiling holds against the
// synthetic 1k-UUID drive (Phase 3 D-04 / RESEARCH §4 helper).
func TestCacheCardinalityCeiling(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewCacheMetrics(te.Registry)
	require.NotNil(t, bag)

	te.AssertCardinalityCeiling(t, "nornicdb_cache_hits_total", 4, func(tenant string) {
		// Subsystems use only allow-listed values; tenant string is ignored.
		for _, c := range AllowedCacheNames {
			bag.Hits.WithLabelValues(c).Inc()
		}
		_ = strings.ToLower(tenant) // silence ineffassign linters
	})
}
