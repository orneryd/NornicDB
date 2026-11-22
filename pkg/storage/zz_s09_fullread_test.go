package storage

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestS09FullRead runs the printed v1.5.2 upgrade scenario and decodes the exact
// store it wrote. The kill scenario is expected to report the documented refusal.
func TestS09FullRead(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil { t.Fatal(err) }
	repo := filepath.Clean(filepath.Join(wd, "..", ".."))
	cmd := exec.Command("bash", "scripts/v152-repro/wal-v4-upgrade.sh")
	cmd.Dir = repo
	cmd.Env = append(os.Environ(), "REPRO_SCENARIO=asyncbolt-nosnap-kill", "REPRO_KEEP=1")
	out, runErr := cmd.CombinedOutput()
	text := string(out)
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "REPRO ") || strings.HasPrefix(line, "REPRO_ABOUT ") || strings.HasPrefix(line, "REPRO_PREFLIGHT ") || strings.HasPrefix(line, "REPRO_STEP root=") {
			t.Log(line)
		}
	}
	var root string
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "REPRO_STEP root=") { root = strings.TrimPrefix(line, "REPRO_STEP root=") }
	}
	if root == "" { t.Fatalf("script did not print its retained store root: %v", runErr) }
	if !strings.Contains(text, " RESULT=FAIL ") && !strings.Contains(text, " RESULT=PASS ") {
		t.Fatalf("script did not produce a fail-closed RESULT line (exit %v)", runErr)
	}
	decode := exec.Command("go", "test", "./pkg/storage", "-run", "^TestL6V4RecordDecode$", "-count=1", "-timeout=30m", "-v")
	decode.Dir = repo
	decode.Env = append(os.Environ(), "ZZL6_UPG_ROOT="+filepath.Join(root, "i1"))
	decoded, err := decode.CombinedOutput()
	t.Log("S09_STRUCTURED_DECODE_BEGIN")
	for _, line := range strings.Split(string(decoded), "\n") { if strings.Contains(line, "L6_V4_") { t.Log(line) } }
	t.Log("S09_STRUCTURED_DECODE_END")
	if err != nil { t.Fatalf("structured decode failed: %v\n%s", err, decoded) }
}