package workcensus

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/ssa"
)

// Author: #1667 implementation lane (Codex). Generic names, no storage policy.
const frameProofFixture = `package framefixture
import "sync"
func side() {}
func Frame(pass func(func()), work func()) {
 var mutex sync.Mutex
 closed, called := false, false
 _ = closed; _ = called
 guarded := func() {
  mutex.Lock()
  defer mutex.Unlock()
  if closed || called { return }
  called = true
  work()
 }
 defer func() { mutex.Lock(); closed = true; mutex.Unlock() }()
 pass(guarded)
}
`

func TestGenericCallbackFrameProof(t *testing.T) {
	for _, tc := range []struct {
		name, source string
		proof        bool
		covered      []bool
	}{
		{"sealed", frameProofFixture, true, []bool{true}},
		{"unsealed", strings.Replace(frameProofFixture, "defer func() { mutex.Lock(); closed = true; mutex.Unlock() }()", "", 1), false, nil},
		{"not_once", strings.Replace(frameProofFixture, "closed || called", "closed", 1), false, nil},
		{"not_closed", strings.Replace(frameProofFixture, "closed || called", "called", 1), false, nil},
		{"early_unlock", strings.Replace(frameProofFixture, "defer mutex.Unlock()", "mutex.Unlock()", 1), false, nil},
		{"unlocked_seal", strings.Replace(frameProofFixture, "defer func() { mutex.Lock();", "defer func() { side();", 1), false, nil},
		{"unlocked_parent_flag", strings.Replace(frameProofFixture, "pass(guarded)", "pass(guarded); closed = true", 1), false, nil},
		{"reopened", strings.Replace(frameProofFixture, "pass(guarded)", "pass(guarded); closed = false", 1), false, nil},
		{"non_rejecting_guard", strings.Replace(frameProofFixture, "if closed || called { return }", "if closed { side() }; if called { side() }", 1), false, nil},
		{"outside", strings.Replace(frameProofFixture, "mutex.Lock()\n  defer", "work()\n  mutex.Lock()\n  defer", 1), true, []bool{false, true}},
		{"async", strings.Replace(frameProofFixture, "work()\n }", "go work()\n }", 1), true, []bool{false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module census.framefixture\n\ngo 1.26\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "frame.go"), []byte(tc.source), 0600); err != nil {
				t.Fatal(err)
			}
			program, err := LoadProgram(Config{Dir: dir, Patterns: []string{"./..."}})
			if err != nil {
				t.Fatal(err)
			}
			var callback *ssa.MakeClosure
			for _, fn := range program.Functions {
				if fn.Name() != "Frame" {
					continue
				}
				for _, block := range fn.Blocks {
					for _, ins := range block.Instrs {
						mc, ok := ins.(*ssa.MakeClosure)
						if ok {
							for _, free := range mc.Fn.(*ssa.Function).FreeVars {
								if free.Name() == "work" {
									callback = mc
								}
							}
						}
					}
				}
			}
			if callback == nil {
				t.Fatal("fixture callback missing")
			}
			proof, ok := ProveCallbackFrame(callback)
			if ok != tc.proof {
				t.Fatalf("proof=%v want %v", ok, tc.proof)
			}
			if !ok {
				return
			}
			var covered []bool
			for _, block := range callback.Fn.(*ssa.Function).Blocks {
				for _, ins := range block.Instrs {
					call, ok := ins.(ssa.CallInstruction)
					if !ok {
						continue
					}
					value := call.Common().Value
					if load, ok := value.(*ssa.UnOp); ok {
						value = load.X
					}
					if free, ok := value.(*ssa.FreeVar); ok && free.Name() == "work" {
						covered = append(covered, proof.Covers(ins))
					}
				}
			}
			if len(covered) != len(tc.covered) {
				t.Fatalf("coverage=%v want %v", covered, tc.covered)
			}
			for i := range covered {
				if covered[i] != tc.covered[i] {
					t.Fatalf("coverage=%v want %v", covered, tc.covered)
				}
			}
		})
	}
}
