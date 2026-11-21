package fgac

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

const comparisonScriptPath = "run-fgac-comparison.sh"

func TestComparisonScriptSyntax(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is not available")
	}
	command := exec.Command("bash", "-n", comparisonScriptPath)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("bash -n %s: %v\n%s", comparisonScriptPath, err, output)
	}
}

func TestComparisonScriptUsesPinnedReferencesAndOptIn(t *testing.T) {
	content, err := os.ReadFile(comparisonScriptPath)
	if err != nil {
		t.Fatalf("read %s: %v", comparisonScriptPath, err)
	}
	script := string(content)
	for _, want := range []string{
		"references.json",
		"FGAC_EE_EVALUATION",
		"FGAC_ALLOW_IMAGE_PULL",
		"NEO4J_ACCEPT_LICENSE_AGREEMENT=eval",
		"TestFGACComparison",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("script does not contain %q", want)
		}
	}
	for _, version := range []string{"5.26.30", "2026.10.2"} {
		if strings.Contains(script, version) {
			t.Errorf("script duplicates pinned version %q; references.json is the only pin owner", version)
		}
	}
	if strings.Contains(script, "docker pull neo4j:") {
		t.Error("script contains an unpinned image pull")
	}
}

func TestComparisonScriptPassesAndFailsClosed(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is not available")
	}
	command := exec.Command("bash", comparisonScriptPath)
	command.Env = append(os.Environ(),
		"FGAC_EE_EVALUATION=",
		"NORNICDB_FGAC_EE_EVALUATION=",
		"NORNICDB_NEO4J_REFERENCE_URI=",
		"NORNICDB_NEO4J_REFERENCE_PASSWORD=",
		"NORNICDB_FGAC_CANDIDATE_URI=",
		"NORNICDB_FGAC_CANDIDATE_PASSWORD=",
	)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("script succeeded without the evaluation opt-in:\n%s", output)
	}
	if !strings.Contains(string(output), "opt-in") {
		t.Fatalf("script error = %s, want the opt-in refusal", output)
	}
}
