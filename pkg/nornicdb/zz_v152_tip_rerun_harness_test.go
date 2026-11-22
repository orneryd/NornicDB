package nornicdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestV152TipRerunHarness runs the existing fail-closed shell reproduction
// harnesses inside the lab's Go worker. The runner accepts focused Go tests;
// this test keeps each harness command and its result visible in that job.
func TestV152TipRerunHarness(t *testing.T) {
	type step struct {
		Script string            `json:"script"`
		Args   []string          `json:"args"`
		Env    map[string]string `json:"env"`
	}
	var steps []step
	raw := os.Getenv("TNRUN_HARNESS_STEPS")
	if raw == "" {
		t.Skip("TNRUN_HARNESS_STEPS is not set")
	}
	err := json.Unmarshal([]byte(raw), &steps)
	if err != nil {
		// tnrun transports --env values through Bash; accept JSON strings
		// with nested escaping as well as literal JSON.
		normalized := raw
		for i := 0; i < 4; i++ {
			before := normalized
			normalized = strings.ReplaceAll(normalized, `\\`, `\`)
			normalized = strings.ReplaceAll(normalized, `\"`, `"`)
			if normalized == before {
				break
			}
		}
		err = json.Unmarshal([]byte(normalized), &steps)
	}
	if err != nil || len(steps) == 0 {
		t.Fatalf("invalid TNRUN_HARNESS_STEPS: %v", err)
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	observedFailure := false
	var harnessErrors []string
	for i, s := range steps {
		if !strings.HasPrefix(s.Script, "scripts/v152-repro/") || strings.Contains(s.Script, "..") {
			t.Fatalf("step %d has an unsupported script path %q", i, s.Script)
		}
		cmd := exec.Command("bash", append([]string{s.Script}, s.Args...)...)
		cmd.Dir = root
		cmd.Env = os.Environ()
		for k, v := range s.Env {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
		var out bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &out
		err := cmd.Run()
		t.Logf("HARNESS_STEP %d script=%s args=%q exit=%v\n%s", i+1, s.Script, s.Args, err, out.String())
		if err == nil {
			continue
		}
		var ee *exec.ExitError
		if errors.As(err, &ee) && ee.ExitCode() == 1 {
			observedFailure = true
			continue
		}
		harnessErrors = append(harnessErrors, fmt.Sprintf("%s: %v", s.Script, err))
	}
	if len(harnessErrors) > 0 {
		t.Fatalf("harness error(s): %s", strings.Join(harnessErrors, "; "))
	}
	if observedFailure {
		t.Error("one or more reproduction harnesses reported RESULT=FAIL")
	}
}
