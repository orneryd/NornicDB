// Author: Codex (#1636 lane). Lifetime retirement composes with native generations.
package nornicdb_test

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestOwnerClassSearchRetirementReentry(t *testing.T) {
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.EmbeddingEnabled = false
	cfg.Database.AsyncWritesEnabled = false
	cfg.Database.PersistSearchIndexes = true
	dir := t.TempDir()
	db, err := nornicdb.Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	old, err := db.GetOrCreateSearchService("retiring", nil)
	if err != nil {
		t.Fatal(err)
	}
	currentPath := filepath.Join(dir, "search", "retiring", "CURRENT")
	previous, err := os.ReadFile(currentPath)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	reentered := make(chan error, 1)
	old.SetLazyWarming(true, func() {
		close(entered)
		<-release
		_, err := db.GetOrCreateSearchService("retiring", nil)
		reentered <- err
	})
	warm := make(chan error, 1)
	go func() { warm <- old.EnsureWarm(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("warm callback did not start")
	}
	retired := make(chan struct{})
	go func() { db.ResetSearchService("retiring"); close(retired) }()
	deadline := time.Now().Add(3 * time.Second)
	for !old.Closed() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !old.Closed() {
		unblock()
		t.Fatal("invalidation did not signal old lifetime")
	}
	early := false
	select {
	case <-retired:
		early = true
	case <-time.After(100 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-reentered:
		if !errors.Is(err, nornicdb.ErrClosed) {
			t.Errorf("constructor during retirement must reject retired lifetime: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("callback-to-constructor completion cycle during invalidation")
	}
	select {
	case err := <-warm:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("warm caller did not finish")
	}
	if !early {
		select {
		case <-retired:
		case <-time.After(3 * time.Second):
			t.Fatal("invalidation did not join actual callback")
		}
	}
	fresh, err := db.GetOrCreateSearchService("retiring", nil)
	if err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(currentPath)
	if err != nil {
		t.Fatal(err)
	}
	if fresh == old || fresh.Closed() || string(previous) == string(current) {
		t.Error("retirement reused the closed service or stale native CURRENT generation")
	}
	if err := old.EnsureWarm(context.Background()); !errors.Is(err, context.Canceled) {
		t.Errorf("closed old service remained usable: %v", err)
	}
	if early {
		t.Error("invalidation returned before the accepted callback actually completed")
	}
}
