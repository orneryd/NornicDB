// Wave-0 RED tests for cypher.PlanHash (D-04b FNV-1a 16-char hex).
//
// References PlanHash, which does not yet exist; package fails to compile
// until the GREEN task ships pkg/cypher/plan_hash.go.
package cypher

import (
	"regexp"
	"sync"
	"testing"
)

// fixedPlanHashFixture builds a stable minimal *ExecutionPlan used by
// TestPlanHash_Stability. Mirrors what the executor emits for `MATCH (n) RETURN n`.
// Restricted to W4 stable arg types (string/int64/float64/bool).
func fixedPlanHashFixture() *ExecutionPlan {
	return &ExecutionPlan{
		Query: "MATCH (n) RETURN n",
		Mode:  ModeNormal,
		Root: &PlanOperator{
			OperatorType: "ProduceResults",
			Description:  "n",
			Identifiers:  []string{"n"},
			Children: []*PlanOperator{
				{
					OperatorType: "AllNodesScan",
					Description:  "n",
					Identifiers:  []string{"n"},
					Arguments: map[string]interface{}{
						"label":  "",
						"strict": false,
						"limit":  int64(100),
						"factor": float64(1.5),
					},
				},
			},
		},
	}
}

// TestPlanHash_Stability is the GOLDEN test. The expected value is computed
// once on first GREEN run and committed inline. It guards against canonical-form
// drift (Pitfall 8) and verifies cross-run stability — the contract Phase 6 (TRC-04)
// relies on for its nornicdb.cypher.plan span attribute.
//
// If the canonical form ever changes (intentional schema evolution), update this
// constant in lockstep — that change is visible to operators consuming plan_hash.
func TestPlanHash_Stability(t *testing.T) {
	const expected = "f2828f5867757221" // GOLDEN — locked 2026-05-01 on first GREEN run
	got := PlanHash(fixedPlanHashFixture())
	if got != expected {
		t.Fatalf("PlanHash drift: expected golden=%q, got %q. If this change is "+
			"intentional, update the golden constant AND announce a Sunset header "+
			"per CLAUDE.md Public API contract.", expected, got)
	}
}

// TestPlanHash_Determinism — repeated invocation in parallel must return the
// same hash. Race-clean. Defends against map-iteration non-determinism (Pitfall 8).
func TestPlanHash_Determinism(t *testing.T) {
	plan := fixedPlanHashFixture()
	first := PlanHash(plan)

	const goroutines = 100
	var wg sync.WaitGroup
	results := make([]string, goroutines)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			results[idx] = PlanHash(plan)
		}(i)
	}
	wg.Wait()
	for i, got := range results {
		if got != first {
			t.Fatalf("non-deterministic PlanHash at goroutine %d: first=%q, got=%q", i, first, got)
		}
	}
}

// TestPlanHash_NilSafe — RESEARCH Pattern 6 line 749 contract.
func TestPlanHash_NilSafe(t *testing.T) {
	if got := PlanHash(nil); got != "0000000000000000" {
		t.Fatalf("expected zero placeholder for nil plan, got %q", got)
	}
	emptyRoot := &ExecutionPlan{}
	if got := PlanHash(emptyRoot); got != "0000000000000000" {
		t.Fatalf("expected zero placeholder for nil-root plan, got %q", got)
	}
}

// TestPlanHash_DifferentPlansDiffer — semantically distinct plans MUST hash differently.
func TestPlanHash_DifferentPlansDiffer(t *testing.T) {
	a := fixedPlanHashFixture()
	b := fixedPlanHashFixture()
	b.Root.Children[0].OperatorType = "NodeByLabelScan"
	if PlanHash(a) == PlanHash(b) {
		t.Fatalf("expected distinct hashes for distinct OperatorType")
	}

	c := fixedPlanHashFixture()
	c.Root.Children[0].Identifiers = []string{"m"}
	if PlanHash(a) == PlanHash(c) {
		t.Fatalf("expected distinct hashes for distinct Identifiers")
	}
}

// TestPlanHash_HexFormat — output MUST match ^[0-9a-f]{16}$.
func TestPlanHash_HexFormat(t *testing.T) {
	re := regexp.MustCompile(`^[0-9a-f]{16}$`)
	plans := []*ExecutionPlan{
		fixedPlanHashFixture(),
		nil,
		{Root: &PlanOperator{OperatorType: "Empty"}},
	}
	for i, p := range plans {
		got := PlanHash(p)
		if !re.MatchString(got) {
			t.Errorf("plan %d: PlanHash output %q does not match ^[0-9a-f]{16}$", i, got)
		}
	}
}

// TestPlanHash_RestrictedArgTypes — W4 canonical form pin. Two plans whose
// Arguments differ only in unsupported types MUST hash equal (both fall through
// the nil-contribution path with the TODO comment).
func TestPlanHash_RestrictedArgTypes(t *testing.T) {
	type customStruct struct{ X int }
	a := fixedPlanHashFixture()
	a.Root.Children[0].Arguments = map[string]interface{}{
		"weird": make(chan int),
	}
	b := fixedPlanHashFixture()
	b.Root.Children[0].Arguments = map[string]interface{}{
		"weird": func() {},
	}
	c := fixedPlanHashFixture()
	c.Root.Children[0].Arguments = map[string]interface{}{
		"weird": customStruct{X: 42},
	}
	ha, hb, hc := PlanHash(a), PlanHash(b), PlanHash(c)
	if ha != hb || hb != hc {
		t.Fatalf("W4: unsupported arg types must contribute nil; got hashes %q/%q/%q", ha, hb, hc)
	}
}

// TestPlanHash_SupportedArgTypes — string/int64/float64/bool MUST influence the
// hash (otherwise we'd be silently throwing away data).
func TestPlanHash_SupportedArgTypes(t *testing.T) {
	build := func(v interface{}) *ExecutionPlan {
		p := fixedPlanHashFixture()
		p.Root.Children[0].Arguments = map[string]interface{}{"v": v}
		return p
	}
	hStr := PlanHash(build("hello"))
	hInt := PlanHash(build(int64(7)))
	hFlt := PlanHash(build(float64(3.14)))
	hBool := PlanHash(build(true))
	hNone := PlanHash(build(nil))

	all := []string{hStr, hInt, hFlt, hBool, hNone}
	seen := make(map[string]int)
	for i, h := range all {
		seen[h] = i
	}
	if len(seen) != len(all) {
		t.Fatalf("expected distinct hashes for distinct supported arg types; got %v", all)
	}
}
