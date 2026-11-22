package storage

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestS08FullReadScripts runs the entry's three commands unchanged against
// the shared lab store and emits the scripts' own RESULT/refused_starts lines.
func TestS08FullReadScripts(t *testing.T) {
	wd, err := os.Getwd(); if err != nil { t.Fatal(err) }
	repo := filepath.Clean(filepath.Join(wd, "..", ".."))
	commands := [][]string{
		{"scripts/v152-repro/snapshot-wal-recovery.sh", "--overlap-inducing"},
		{"scripts/v152-repro/snapshot-wal-recovery.sh"},
		{"scripts/v152-repro/wal-replay-rebuild.sh"},
	}
	for _, args := range commands {
		cmd := exec.Command("bash", args...); cmd.Dir = repo
		out, runErr := cmd.CombinedOutput(); text := string(out)
		found := false
		for _, line := range strings.Split(text, "\n") {
			if strings.Contains(line, "RESULT=") || strings.Contains(line, "refused_starts=") { fmt.Println(line); found = true }
		}
		if !found {
			info, statErr := os.Stat(filepath.Join(repo, args[0]))
			size := int64(-1); if info != nil { size = info.Size() }
			fmt.Printf("S08_DEBUG cwd=%q repo=%q script=%q size=%d statErr=%v exit=%v output_bytes=%d\n", wd, repo, args[0], size, statErr, runErr, len(out))
			t.Fatalf("%s emitted no RESULT/refused_starts line (exit %v):\n%s", strings.Join(args, " "), runErr, text)
		}
		if strings.Contains(text, "RESULT=HARNESS-ERROR") { t.Fatalf("%s harness error (exit %v)", strings.Join(args, " "), runErr) }
	}
}