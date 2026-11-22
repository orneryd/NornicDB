package nornicdb

import (
    "errors"
    "fmt"
    "os"
    "os/exec"
    "path/filepath"
    "strings"
    "testing"
)

func s3UpgradeScript(t *testing.T, scriptName string, extraEnv ...string) {
    t.Helper()
    root, err := filepath.Abs(filepath.Join("..", ".."))
    if err != nil { t.Fatal(err) }
    script := filepath.Join(root, "scripts", "v152-repro", scriptName)
    if _, err := os.Stat(script); err != nil { t.Fatalf("script missing: %v", err) }
    cmd := exec.Command("bash", script)
    cmd.Dir = root
    cmd.Env = append(os.Environ(), "REPRO_STORES=1", "REPRO_COUNT=1", "REPRO_TMP="+t.TempDir())
    cmd.Env = append(cmd.Env, extraEnv...)
    out, runErr := cmd.CombinedOutput()
    output := string(out)
    code := 0
    if runErr != nil {
        var exitErr *exec.ExitError
        if !errors.As(runErr, &exitErr) { t.Fatalf("run %s: %v\n%s", scriptName, runErr, output) }
        code = exitErr.ExitCode()
    }
    t.Logf("S3_SCRIPT_RESULT script=%s exit=%d output=%s", scriptName, code, output)
    if code > 2 || !strings.Contains(output, "REPRO ") {
        t.Errorf("S3_SCRIPT_RESULT=harness-error script=%s exit=%d err=%s", scriptName, code, fmt.Sprint(runErr))
    }
}

// S05 and S07 share the original snapshot-plus-journal rebuild reproduction.
func TestS3SnapshotRebuildB6(t *testing.T) {
    s3UpgradeScript(t, "snapshot-rebuild-b6.sh")
}

// S09's report command runs the kill scenario followed by the default clean
// scenario; both are run once in the same package unit.
func TestS3WalV4UpgradeB6(t *testing.T) {
    s3UpgradeScript(t, "wal-v4-upgrade.sh", "REPRO_SCENARIO=asyncbolt-nosnap-kill")
    s3UpgradeScript(t, "wal-v4-upgrade.sh")
}
