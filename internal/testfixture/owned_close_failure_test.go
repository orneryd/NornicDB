package testfixture

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// Reproduction for NOR-323 (PR #1698): Own registers a nil reporter, so a
// fixture whose Close fails passes its testing owner. The failure is observed
// from a child process so this test reports the defect without failing its own
// package run through the child.
func TestOwnCloseFailureFailsTestingOwner(t *testing.T) {
	command := exec.Command(os.Args[0], "-test.run=^TestOwnCloseFailureChild$", "-test.count=1")
	command.Env = append(os.Environ(), "NORNIC_FIXTURE_CLOSE_FAILURE=1")
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("close failure passed its testing owner: %s", output)
	}
	for _, want := range []string{"--- FAIL: TestOwnCloseFailureChild/owner", "closing test fixture: planted close failure"} {
		if !strings.Contains(string(output), want) {
			t.Errorf("missing close-failure evidence %q: %s", want, output)
		}
	}
}

func TestOwnCloseFailureChild(t *testing.T) {
	if os.Getenv("NORNIC_FIXTURE_CLOSE_FAILURE") == "" {
		t.Skip("executed by the parent subprocess contract")
	}
	var closed []string
	t.Run("owner", func(t *testing.T) {
		Own(t, &failingCloser{recordingCloser: recordingCloser{name: "engine", closed: &closed}, failure: errors.New("planted close failure")})
	})
}
