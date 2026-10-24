package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
)

// This fixture is copied unchanged onto the actual release base by the remote
// compatibility driver. All data operations use APIs present on that base. The new
// head's test adapter substitutes its constructor entry point for own-WAL reads.
var walDowngradeRecovery = RecoverBufferedCommits

func TestWALPublicDowngradeFixture(t *testing.T) {
	dir, mode := os.Getenv("NORNICDB_WAL_COMPAT_DIR"), os.Getenv("NORNICDB_WAL_COMPAT_MODE")
	if dir == "" || mode == "" {
		t.Skip("two-binary fixture: run with the remote compatibility driver")
	}
	t.Cleanup(config.WithWALEnabled())
	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	logDir := filepath.Join(dir, "wal")
	cfg := DefaultWALConfig()
	if os.Getenv("NORNICDB_WAL_COMPAT_ROTATE") == "1" {
		cfg.MaxEntries = 1
	}
	if mode == "write-clean-sync-restore" || mode == "write-crash-sync-restore" || mode == "read-sync-restore" {
		walDowngradeSynchronousRestore(t, dir, mode, cfg)
		return
	}
	if mode == "refuse" {
		path := filepath.Join(logDir, "wal.log")
		before, err := os.ReadFile(path)
		check(err)
		_, err = NewWAL(logDir, cfg)
		if err == nil || !strings.Contains(err.Error(), "unsupported WAL version 4 (max supported: 3)") {
			t.Fatalf("older reader refusal: %v", err)
		}
		after, err := os.ReadFile(path)
		check(err)
		if !bytes.Equal(before, after) {
			t.Fatal("older reader changed the refused WAL")
		}
		t.Log("unsupported WAL version 4 (max supported: 3); WAL unchanged")
		return
	}
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: filepath.Join(dir, "store"), WriteBehind: true, WriteBehindInterval: time.Hour})
	check(err)
	// Test-only staging control shared by base/head: an acknowledged crash
	// write must remain pending instead of racing the adaptive flush timer.
	b.writeBehind.SetDelayBounds(time.Hour, time.Hour)
	parkBuffer := func() {
		// Explicit flush can leave an empty rotation request queued. Replace
		// only that idle test buffer before starting the genuine crash phase.
		b.writeBehind.Close()
		b.writeBehind = NewWriteBehindBuffer(time.Hour, 1000000, b.replayWriteBehind)
		b.writeBehind.SetDelayBounds(time.Hour, time.Hour)
	}
	if strings.HasPrefix(mode, "write") {
		if mode == "write-clean-foreign" {
			// Destination beta=1, alpha=2; the source below writes alpha=1.
			_, err := b.CreateNode(&Node{ID: "upgrade:seed", Properties: map[string]any{"beta": int64(0)}})
			check(err)
		}
		_, err := b.CreateNode(&Node{ID: "upgrade:a", Properties: map[string]any{"alpha": int64(1)}})
		check(err)
		check(b.FlushWriteBehind())
		check(b.Sync())
		parkBuffer()
		if mode == "write-clean-foreign" {
			walDowngradeForeignSource(t, b, logDir)
		}
	}
	w, err := NewWAL(logDir, cfg)
	check(err)
	if !strings.HasPrefix(mode, "write") {
		applied, err := walDowngradeRecovery(b, w)
		check(err)
		if (mode == "read-recover" || mode == "read-three") && applied != 1 {
			t.Fatalf("replayed %d, want one genuinely pending commit", applied)
		}
		node, err := b.GetNode("upgrade:a")
		check(err)
		want := int64(2)
		if mode == "read-three" {
			want = 3
		}
		if node.Properties["alpha"] != want {
			t.Fatalf("alpha=%v, want %d", node.Properties["alpha"], want)
		}
		if mode == "old-update" || mode == "read-three" {
			seed, err := b.GetNode("upgrade:seed")
			check(err)
			if seed.Properties["beta"] != int64(0) {
				t.Fatal("foreign replay changed the destination's existing binding")
			}
		}
	}
	journal := NewWALEngine(b, w)
	if strings.HasPrefix(mode, "write") && mode != "write-clean-foreign" || mode == "old-update" {
		value := int64(2)
		if mode == "old-update" {
			value = 3
			// Warm the inherited cold-reopen sequence path using public CRUD.
			node, err := b.GetNode("upgrade:a")
			check(err)
			check(b.UpdateNode(node))
			parkBuffer()
		}
		tx, err := b.BeginTransaction()
		check(err)
		check(tx.SetNamespace("upgrade"))
		check(tx.SetImplicit(true))
		check(tx.UpdateNode(&Node{ID: "upgrade:a", Properties: map[string]any{"alpha": value}}))
		check(tx.Commit())
		if !tx.AcknowledgedBuffered() {
			t.Fatal("fixture did not exercise write-behind")
		}
		if mode == "write-crash" || mode == "old-update" {
			// The remote driver invokes the compiled test binary directly and
			// expects this exit status; no defers or clean shutdown run.
			fmt.Println("WAL_COMPAT_CRASH_AFTER_ACK")
			os.Exit(23)
		}
	}
	check(journal.Close())
}

// A synchronous native restore still invalidates the old dictionary authority.
// This case deliberately performs no snapshot compaction before clean shutdown.
// The remote driver copies this fixture unchanged onto the old reader too.
func walDowngradeSynchronousRestore(t *testing.T, dir, mode string, cfg *WALConfig) {
	t.Helper()
	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: filepath.Join(dir, "store"), WriteBehind: false})
	check(err)
	t.Cleanup(func() { _ = b.Close() })
	w, err := NewWAL(filepath.Join(dir, "wal"), cfg)
	check(err)
	t.Cleanup(func() { _ = w.Close() })
	journal := NewWALEngine(b, w)
	if strings.HasPrefix(mode, "write-") {
		source, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: filepath.Join(dir, "source"), WriteBehind: false})
		check(err)
		t.Cleanup(func() { _ = source.Close() })
		_, err = source.CreateNode(&Node{ID: "upgrade:a", Properties: map[string]any{"alpha": int64(2), "beta": "sync"}})
		check(err)
		backup := filepath.Join(dir, "native.backup")
		check(source.Backup(backup))
		check(source.Close())
		check(b.Restore(backup))
		_, err = journal.CreateNode(&Node{ID: "upgrade:sync-marker", Properties: map[string]any{"after": true}})
		check(err)
	}
	node, err := b.GetNode("upgrade:a")
	check(err)
	if node.Properties["alpha"] != int64(2) || node.Properties["beta"] != "sync" {
		t.Fatalf("synchronous restore changed properties: %v", node.Properties)
	}
	marker, err := b.GetNode("upgrade:sync-marker")
	check(err)
	if marker.Properties["after"] != true {
		t.Fatalf("synchronous post-restore write missing: %v", marker.Properties)
	}
	if mode == "write-crash-sync-restore" {
		check(b.Sync())
		fmt.Println("WAL_COMPAT_CRASH_AFTER_SYNC_RESTORE")
		os.Exit(23)
	}
	check(journal.Close())
}

func walDowngradeForeignSource(t *testing.T, destination *BadgerEngine, logDir string) {
	t.Helper()
	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	source, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: filepath.Join(filepath.Dir(logDir), "source"), WriteBehind: true, WriteBehindInterval: time.Hour})
	check(err)
	source.writeBehind.SetDelayBounds(time.Hour, time.Hour)
	_, err = source.CreateNode(&Node{ID: "upgrade:a", Properties: map[string]any{"alpha": int64(0)}})
	check(err)
	check(source.FlushWriteBehind())
	wal, err := NewWAL(logDir, DefaultWALConfig())
	check(err)
	journal := NewWALEngine(source, wal)
	tx, err := source.BeginTransaction()
	check(err)
	check(tx.SetNamespace("upgrade"))
	check(tx.SetImplicit(true))
	check(tx.UpdateNode(&Node{ID: "upgrade:a", Properties: map[string]any{"alpha": int64(2)}}))
	check(tx.Commit())
	if !tx.AcknowledgedBuffered() {
		t.Fatal("foreign fixture did not use a buffered record")
	}
	check(journal.Close())
	result, _, err := RecoverIntoEngine(destination, logDir, "")
	check(err)
	if result.Applied != 1 || result.Failed != 0 {
		t.Fatalf("foreign fixture: %s", result.Summary())
	}
}
