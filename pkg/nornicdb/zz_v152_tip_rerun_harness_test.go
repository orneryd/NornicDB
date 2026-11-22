package nornicdb

import (
	"bytes"
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
		Script string
		Args   []string
		Env    map[string]string
	}
	parseSteps := func(raw string) ([]step, error) {
		var steps []step
		for _, encoded := range strings.Split(raw, ";") {
			if encoded == "" {
				continue
			}
			parts := strings.SplitN(encoded, ":", 2)
			name, mode := parts[0], ""
			if len(parts) == 2 {
				mode = parts[1]
			}
			s := step{Env: map[string]string{}}
			switch name {
			case "snapshot-rebuild-b6", "wal-replay-rebuild":
				s.Script = "scripts/v152-repro/" + name + ".sh"
				if mode != "" {
					return nil, fmt.Errorf("unsupported mode %q for %s", mode, name)
				}
			case "wal-v4-upgrade":
				s.Script = "scripts/v152-repro/" + name + ".sh"
				if mode != "" {
					s.Env["REPRO_SCENARIO"] = mode
				}
			case "snapshot-wal-recovery":
				s.Script = "scripts/v152-repro/" + name + ".sh"
				switch mode {
				case "", "default":
				case "overlap":
					s.Args = []string{"--overlap-inducing"}
				case "overlap-off":
					s.Args = []string{"--overlap-inducing", "--write-behind=off"}
				default:
					return nil, fmt.Errorf("unsupported mode %q for %s", mode, name)
				}
			default:
				return nil, fmt.Errorf("unsupported harness %q", name)
			}
			steps = append(steps, s)
		}
		if len(steps) == 0 {
			return nil, fmt.Errorf("no harness steps")
		}
		return steps, nil
	}
	raw := os.Getenv("TNRUN_HARNESS_STEPS")
	if raw == "" {
		t.Skip("TNRUN_HARNESS_STEPS is not set")
	}
	steps, err := parseSteps(raw)
	if err != nil {
		t.Fatalf("invalid TNRUN_HARNESS_STEPS: %v", err)
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	observedFailure := false
	var harnessErrors []string
	for i, s := range steps {
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
