//go:build boundplan_census_operator

// Author: #1640 lane (Codex). Analysis-only client; frozen engine supplied by the operator overlay.
package boundplancheck

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
)

func boundPlanCensusConfig() workcensus.Config {
	_, here, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(here), "../.."))
	return workcensus.Config{
		Dir:              root,
		Patterns:         []string{"./pkg/cypher/...", "./pkg/fabric", "./pkg/server", "./pkg/bolt", "./pkg/graphql/...", "./internal/callinputtrace"},
		IncludeInactive:  true,
		ExcludeGenerated: true,
		SourceRoots:      []workcensus.SourceRoot{{Path: ".", Languages: map[string]string{".go": "go"}, ExcludeDirs: []string{"vendor", "node_modules", ".tn-operator"}, ExcludeFiles: []string{"*_test.go"}}},
		// Shared source coverage is the search universe only. The direct domain
		// query, not declaration existence or reachability, determines membership.
		Selectors: []workcensus.Selector{{Kind: "bound-plan-candidate", Node: "declaration", Reachability: "all"}},
	}
}

// Author: #1640 lane (Codex). The approval pins the direct executable
// mechanism union. Source coverage is context, never a default semantic class.
func boundPlanCensusPolicy() workcensus.Policy {
	classes := []string{"command-rule", "constructor-rule", "constructor-owner", "constructor-entry", "constructor-state", "constructor-input", "diagnostic-renderer", "frozen-consumer", "materialization-consumer", "public-consumer", "retained-operand-rule", "runtime-value", "syntax-owner", "upper-consumer", "upper-migration", "protocol-adapter", "immutable-representation", "representation-helper", "cache-key", "participant-signature", "syntax-observer"}
	kinds := make(map[string]string, len(classes))
	for _, class := range classes {
		kinds[class] = "review"
	}
	return workcensus.Policy{
		Classes:            classes,
		ClassKinds:         kinds,
		RequireSources:     false, // Source boundaries are observed context; only direct compiled members are approved.
		RequireReviewOwner: true,
		RequireSubjectsFor: []string{"constructor-rule", "constructor-owner", "constructor-entry", "constructor-state", "constructor-input", "frozen-consumer", "public-consumer"},
	}
}

type boundPlanCensusApproval struct{ workcensus.Inventory }

func readBoundPlanApproval(t *testing.T, config workcensus.Config) boundPlanCensusApproval {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(config.Dir, "internal/boundplancheck/testdata/population.json"))
	if err != nil {
		t.Fatal(err)
	}
	var approved boundPlanCensusApproval
	if err := json.Unmarshal(raw, &approved); err != nil {
		t.Fatal(err)
	}
	return approved
}

func TestBoundPlanPopulationInventory(t *testing.T) {
	actual, err := workcensus.Extract(boundPlanCensusConfig())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(actual)
	if err != nil {
		t.Fatal(err)
	}
	var compressed bytes.Buffer
	archive := zip.NewWriter(&compressed)
	entry, err := archive.Create("population.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("BOUND_PLAN_POPULATION sources=%d definitions=%d members=%d", len(actual.Sources), len(actual.Definitions), len(actual.Members))
	t.Logf("BOUND_PLAN_POPULATION_ZIP %s", base64.StdEncoding.EncodeToString(compressed.Bytes()))
}

func TestBoundPlanClauseKindPopulation(t *testing.T) {
	config := boundPlanCensusConfig()
	// The shared engine selects the actual Go type, including unused newly
	// declared constants. No private enum/source parser or ID generator exists.
	config.Selectors = []workcensus.Selector{{Kind: "bound-plan-candidate", Node: "const", Reachability: "all", Packages: []string{"github.com/orneryd/nornicdb/pkg/cypher"}, TypeContains: []string{"cypher.pipelineClauseKind"}}}
	raw, err := os.ReadFile(filepath.Join(config.Dir, "pkg/cypher/testdata/bound_write_transition_population.json"))
	if err != nil {
		t.Fatal(err)
	}
	var population struct {
		ConstInventory workcensus.Inventory `json:"constInventory"`
	}
	if err := json.Unmarshal(raw, &population); err != nil {
		t.Fatal(err)
	}
	policy := boundPlanCensusPolicy()
	policy.RequireSources = false // the repository-wide source gate owns this.
	actual, err := workcensus.Extract(config)
	if err != nil {
		t.Fatal(err)
	}
	if failures := workcensus.Validate(actual, population.ConstInventory, policy); len(failures) > 0 {
		t.Fatal(strings.Join(failures, "\n"))
	}
	t.Logf("shared typed selector checked all %d clause-kind declarations", len(actual.Members))
	t.Run("new-kind-without-disposition-fails", func(t *testing.T) {
		config.Overlay = map[string][]byte{filepath.Join(config.Dir, "pkg/cypher/r73_planted_kind.go"): []byte("package cypher\nconst arbitraryNewKind pipelineClauseKind = 999\n")}
		planted, err := workcensus.Extract(config)
		if err != nil {
			t.Fatal(err)
		}
		failures := strings.Join(workcensus.Validate(planted, population.ConstInventory, policy), "\n")
		if !strings.Contains(failures, "UNLISTED pkg/cypher/r73_planted_kind.go:") {
			t.Fatal("new clause kind escaped the shared typed population gate", failures)
		}
	})
}

// Semantic review is domain DATA joined to shared-engine subjects. This test
// neither enumerates source nor creates IDs, digests or a second call graph.
// The compiled Extract/Validate client remains the current-source census gate.
func TestBoundPlanSemanticSubjects(t *testing.T) {
	config := boundPlanCensusConfig()
	approved := readBoundPlanApproval(t, config)
	raw, err := os.ReadFile(filepath.Join(config.Dir, "pkg/cypher/testdata/bound_current_population.json"))
	if err != nil {
		t.Fatal(err)
	}
	var population struct {
		ProvenanceMethods []string `json:"provenanceMethods"`
		Members           []struct {
			ID, Classification, Reason string
			Cases                      []string
			Methods                    []string
		}
	}
	if err := json.Unmarshal(raw, &population); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(filepath.Join(config.Dir, "pkg/cypher/testdata/bound_validation_rules.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rules struct{ Cases []struct{ ID string } }
	if err := json.Unmarshal(raw, &rules); err != nil {
		t.Fatal(err)
	}
	cases := map[string]bool{}
	for _, sample := range rules.Cases {
		cases[sample.ID] = true
	}
	check := func(rows []struct {
		ID, Classification, Reason string
		Cases                      []string
		Methods                    []string
	}) []string {
		byID := map[string]workcensus.Member{}
		for _, member := range approved.Members {
			byID[member.ID] = member
		}
		var failures []string
		seen := map[string]bool{}
		for _, row := range rows {
			member, found := byID[row.ID]
			if !found || seen[row.ID] || row.Classification == "" || row.Reason == "" || member.Class != row.Classification || member.Reason != row.Reason {
				failures = append(failures, "missing semantic disposition: "+row.ID)
			}
			seen[row.ID] = true
			for _, id := range row.Cases {
				if !cases[id] {
					failures = append(failures, "missing public witness: "+row.ID+" "+id)
				}
			}
			if len(row.Methods) == 0 {
				failures = append(failures, "missing direct mechanism: "+row.ID)
			}
		}
		for _, member := range approved.Members {
			if !seen[member.ID] {
				failures = append(failures, "semantic subject omitted: "+member.ID)
			}
		}
		return failures
	}
	for _, method := range []string{"graphify", "all-tracked-rg", "own-delta", "compiled-direct"} {
		found := false
		for _, observed := range population.ProvenanceMethods {
			found = found || observed == method
		}
		if !found {
			t.Fatalf("missing population-union method %s", method)
		}
	}
	if len(population.Members) == 0 {
		t.Fatal("empty semantic population")
	}
	if failures := check(population.Members); len(failures) != 0 {
		t.Fatal(strings.Join(failures, "\n"))
	}
	t.Run("missing-semantic-disposition-fails", func(t *testing.T) {
		if len(check(population.Members[1:])) == 0 {
			t.Fatal("omitted semantic member escaped")
		}
	})
	t.Logf("semantic DATA checked %d shared-engine subjects and public witness references", len(population.Members))
}

// Author: #1640 lane (Codex). These exact dependency subjects are a separate
// cross-check, not additional bound-fact members or a second source enumerator.
func TestBoundPlanAuthoredSourceSubjects(t *testing.T) {
	config := boundPlanCensusConfig()
	raw, err := os.ReadFile(filepath.Join(config.Dir, "internal/boundplancheck/testdata/authored_source_subjects.json"))
	if err != nil {
		t.Fatal(err)
	}
	var approved workcensus.Inventory
	if err := json.Unmarshal(raw, &approved); err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	for _, member := range approved.Members {
		ids[member.ID] = true
	}
	config.Patterns = []string{"./pkg/cypher"}
	config.SourceRoots = nil
	live, err := workcensus.Extract(config)
	if err != nil {
		t.Fatal(err)
	}
	actual := workcensus.Inventory{}
	for _, member := range live.Members {
		if ids[member.ID] {
			actual.Members = append(actual.Members, member)
		}
	}
	observed, err := json.Marshal(actual)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("BOUND_AUTHORED_SUBJECTS %s", observed)
	policy := boundPlanCensusPolicy()
	policy.RequireSources = false // Full repository boundaries belong to the direct gate.
	if failures := workcensus.Validate(actual, approved, policy); len(failures) > 0 {
		t.Fatal(strings.Join(failures, "\n"))
	}
}

// Author: #1640 lane. This checks domain DATA against the shared approval;
// discovery and declaration identity remain exclusively in workcensus.
func TestBoundPlanAuthoredSourcePopulation(t *testing.T) {
	config := boundPlanCensusConfig()
	subjectRaw, err := os.ReadFile(filepath.Join(config.Dir, "internal/boundplancheck/testdata/authored_source_subjects.json"))
	if err != nil {
		t.Fatal(err)
	}
	var approved workcensus.Inventory
	if err := json.Unmarshal(subjectRaw, &approved); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(config.Dir, "pkg/cypher/testdata/bound_authored_source_population.json"))
	if err != nil {
		t.Fatal(err)
	}
	var population struct {
		Author  string
		Members []struct {
			Author, ID, File, Symbol, SubjectID, Status, Reason, Dependency string
		}
	}
	if err := json.Unmarshal(raw, &population); err != nil {
		t.Fatal(err)
	}
	if population.Author == "" || len(population.Members) == 0 {
		t.Fatal("authored-source population and authorship are required")
	}
	byID := map[string]workcensus.Member{}
	for _, member := range approved.Members {
		byID[member.ID] = member
	}
	seen := map[string]bool{}
	for _, row := range population.Members {
		member, known := byID[row.SubjectID]
		if !known || member.Class == "source-candidate" || seen[row.ID] || row.Author == "" || row.Reason == "" {
			t.Fatalf("source reconstruction lacks an individually reviewed shared subject: %+v", row)
		}
		seen[row.ID] = true
		switch row.Status {
		case "OPEN":
			if row.Dependency != "credentials internal/cyphertext accepted-view/original-span product; consumed at composition" {
				t.Fatalf("normalized-source dependency has no lower owner: %+v", row)
			}
		case "authored-framing", "upper-consumer":
		default:
			t.Fatalf("source reconstruction has no disposition: %+v", row)
		}
	}
	t.Logf("checked %d source reconstruction/consumer dispositions; OPEN dependencies grant no readiness", len(population.Members))
}
