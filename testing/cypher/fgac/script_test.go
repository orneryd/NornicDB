package fgac

import (
	"os"
	"os/exec"
	"path/filepath"
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

func TestComparisonScriptUsesReferenceABACSetting(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is not available")
	}
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 is not available")
	}
	pins, err := LoadReferencePins(referencesPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins.References {
		t.Run(pin.Version, func(t *testing.T) {
			dir := t.TempDir()
			docker := filepath.Join(dir, "docker")
			args := filepath.Join(dir, "docker-args")
			stub := `#!/usr/bin/env bash
case "$1" in
  image|rm) exit 0 ;;
  run) printf '%s\n' "$@" > "$FGAC_DOCKER_ARGS"; exit 47 ;;
  *) exit 48 ;;
esac
`
			if err := os.WriteFile(docker, []byte(stub), 0o755); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("bash", comparisonScriptPath)
			command.Env = append(os.Environ(),
				"PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"),
				"FGAC_EE_EVALUATION=1",
				"FGAC_START_REFERENCE=1",
				"FGAC_REFERENCE_VERSION="+pin.Version,
				"FGAC_REFERENCE_PASSWORD=fixture-only",
				"FGAC_DOCKER_ARGS="+args,
			)
			out, err := command.CombinedOutput()
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 47 {
				t.Fatalf("expected the Docker stub to stop the script, got %v\n%s", err, out)
			}
			data, err := os.ReadFile(args)
			if err != nil {
				t.Fatal(err)
			}
			arguments := string(data)
			if !strings.Contains(arguments, "\n"+pin.Ref()+"\n") {
				t.Errorf("docker run did not receive the immutable image reference for %s", pin.Version)
			}
			const nativeABAC = "\nNEO4J_dbms_security_abac_authorization__providers=native\n"
			if got := strings.Contains(arguments, nativeABAC); got != pin.NativeABAC {
				t.Errorf("native ABAC argument present = %t, want %t", got, pin.NativeABAC)
			}
		})
	}
}
