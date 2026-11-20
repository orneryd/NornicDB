package zzv152repro

// Lab runner for the upgrade-store reproduction scripts (research harness, not for merge).
// The lab's script route runs only `make verify-selftest`, so a lab job runs a script through this test:
//
//	REPRO_ITEM=<item> [REPRO_ARGS='<args>'] go test ./internal/zzv152repro -run '^TestV152Repro$' -count=1 -v
//
// REPRO_ARGS: script arguments separated by spaces or commas, each of the form --name or --name=value (lowercase letters,
// digits and dashes), e.g. REPRO_ARGS=--forced-overlap,--write-behind=off.
// It runs scripts/v152-repro/<item>.sh unchanged (bash, from the repository root), streams its output, and
// fails unless the script exits 0 (PASS). Exit 1 is the claimed defect reproduced, exit 2 a harness error.
// Outside the lab, run the script itself: bash scripts/v152-repro/<item>.sh

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var reproArg = regexp.MustCompile(`^--[a-z0-9-]+(=[a-z0-9-]+)?$`)

func TestV152Repro(t *testing.T) {
	item := strings.TrimSpace(os.Getenv("REPRO_ITEM"))
	if item == "" || strings.ContainsAny(item, "/\\.") {
		t.Fatalf("REPRO_ITEM must name a script in scripts/v152-repro (got %q)", item)
	}
	top, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Fatalf("repository root: %v", err)
	}
	root := strings.TrimSpace(string(top))
	script := filepath.Join(root, "scripts", "v152-repro", item+".sh")
	if _, err := os.Stat(script); err != nil {
		t.Fatalf("no such reproduction script: %v", err)
	}
	args := strings.FieldsFunc(os.Getenv("REPRO_ARGS"), func(r rune) bool { return r == ' ' || r == ',' })
	for _, a := range args {
		if !reproArg.MatchString(a) {
			t.Fatalf("REPRO_ARGS: argument %q is not --name or --name=value", a)
		}
	}
	cmd := exec.Command("bash", append([]string{script}, args...)...)
	cmd.Dir = root
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	err = cmd.Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exit) && exit.ExitCode() == 1:
		t.Fatalf("%s: FAIL (the claimed defect is reproduced)", item)
	case errors.As(err, &exit):
		t.Fatalf("%s: harness error (exit %d)", item, exit.ExitCode())
	default:
		t.Fatalf("%s: %v", item, err)
	}
}
