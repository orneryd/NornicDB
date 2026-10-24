package storage

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
)

// Badger starts one doWrites worker per store; Close waits for that worker.
// This also counts allocations discarded before a constructor returns nil.
func recoveryOpenStores() int {
	count, _ := fixtureowner.OpenStores()
	return count
}

func requireRecoveryStoresClosed(t *testing.T) {
	t.Helper()
	before := recoveryOpenStores()
	t.Cleanup(func() {
		after := recoveryOpenStores()
		if after != before {
			t.Errorf("recovery left stores open after error/cleanup: before=%d after=%d", before, after)
		}
	})
}

func TestRecoveryErrorLifetime(t *testing.T) {
	for _, name := range []string{"existing_error_root", "with_result", "transactions", "logger", "plain"} {
		t.Run(name, func(t *testing.T) {
			command := exec.Command(os.Args[0], "-test.run=^TestRecoveryErrorLifetimeChild$", "-test.count=1", "-test.v")
			command.Env = append(os.Environ(), "NORNIC_RECOVERY_LIFETIME_CASE="+name)
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("recovery leaked an internally allocated store: %v\n%s", err, output)
			}
		})
	}
}

func TestRecoveryErrorLifetimeChild(t *testing.T) {
	name := os.Getenv("NORNIC_RECOVERY_LIFETIME_CASE")
	if name == "" {
		return
	}
	if before := recoveryOpenStores(); before != 0 {
		t.Fatalf("isolated recovery control starts with %d stores", before)
	}
	t.Run("recovery", func(t *testing.T) {
		if name == "existing_error_root" {
			TestRecoverFromWALWithResult_ErrorAndRoutingBranches(t)
			return
		}
		dir := t.TempDir()
		snapshot := filepath.Join(dir, "broken.json")
		if err := os.WriteFile(snapshot, []byte("{bad-json"), 0644); err != nil {
			t.Fatal(err)
		}
		var engine *MemoryEngine
		var err error
		switch name {
		case "with_result":
			engine, _, err = fixtureowner.OwnResult3[*MemoryEngine, ReplayResult](t, ErrStorageClosed)(RecoverFromWALWithResult(filepath.Join(dir, "wal"), snapshot))
		case "transactions":
			engine, _, err = fixtureowner.OwnResult3[*MemoryEngine, *TransactionRecoveryResult](t, ErrStorageClosed)(RecoverWithTransactions(filepath.Join(dir, "wal"), snapshot))
		case "logger":
			engine, err = fixtureowner.OwnResult[*MemoryEngine](t, ErrStorageClosed)(RecoverFromWALWithLogger(filepath.Join(dir, "wal"), snapshot, nil))
		case "plain":
			engine, err = fixtureowner.OwnResult[*MemoryEngine](t, ErrStorageClosed)(RecoverFromWAL(filepath.Join(dir, "wal"), snapshot))
		default:
			t.Fatal("unknown recovery control", name)
		}
		if err == nil || engine != nil {
			t.Fatalf("constructor result changed: engine=%v err=%v", engine, err)
		}
	})
	after := recoveryOpenStores()
	fmt.Printf("RECOVERY_LIFETIME name=%s before=0 after=%d\n", name, after)
	if after != 0 {
		t.Errorf("internally allocated stores survive error return: %d", after)
	}
}
