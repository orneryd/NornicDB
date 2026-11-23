package cypher

import (
	"os"
	"os/exec"
	"testing"
)

// TestT03IsolatedReproDriver preserves the report's package-isolation run
// while allowing the package's other DEI probes to share a lab job.
func TestT03IsolatedReproDriver(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^(TestMergeNodeWritesOf|TestQueryBodyOutputPresencePolicies)$", "-test.count=1")
	output, err := cmd.CombinedOutput()
	t.Logf("T03_CHILD_EXIT err=%v\n%s", err, output)
	if err != nil {
		t.Fatalf("isolated T03 reproduction failed: %v", err)
	}
}
