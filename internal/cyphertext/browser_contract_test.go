//go:build browsercontract

package cyphertext

import (
	"os/exec"
	"testing"
)

// This opt-in contract runs the maintained browser peer against the same
// independent fixtures. It requires Node.js in the lab's verification image.
func TestBrowserStatementFixtureContract(t *testing.T) {
	command := exec.CommandContext(t.Context(), "node", "ui/scripts/test-cypher-text.mjs")
	command.Dir = "../.."
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("browser fixture contract: %v\n%s", err, output)
	}
	t.Log(string(output))
}
