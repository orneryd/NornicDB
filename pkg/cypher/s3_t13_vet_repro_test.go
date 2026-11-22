package cypher

import (
    "os/exec"
    "path/filepath"
    "strings"
    "testing"
)

// TestS3T13IntegrationVet reproduces the report's integration-tagged vet
// check from this checkout. The expected ChunkText diagnostic is reported
// as a reproduced defect, not as a failure of this wrapper.
func TestS3T13IntegrationVet(t *testing.T) {
    root, err := filepath.Abs(filepath.Join("..", ".."))
    if err != nil { t.Fatal(err) }
    cmd := exec.Command("go", "vet", "-tags=integration,noui,nolocalllm", "./pkg/cypher")
    cmd.Dir = root
    out, err := cmd.CombinedOutput()
    output := string(out)
    if err == nil { t.Logf("S3_T13_RESULT=not-reproduced output=%s", output); return }
    if strings.Contains(output, "ChunkText") { t.Logf("S3_T13_RESULT=reproduced output=%s", output); return }
    t.Errorf("S3_T13_RESULT=harness-error err=%v output=%s", err, output)
}
