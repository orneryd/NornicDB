// Author: Codex (#1636 lane). Public owner-class retirement schedule.
package search_test

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestOwnerClassSearchActualCompletion(t *testing.T) {
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	defer engine.Close()
	svc := search.NewService(engine, search.DefaultVectorDimensions, nil)
	entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	svc.SetLazyWarming(true, func() { close(entered); <-release; close(returned) })
	warmDone := make(chan error, 1)
	go func() { warmDone <- svc.EnsureWarm(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("public warming callback did not start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- svc.Close() }()
	early := false
	select {
	case err := <-closeDone:
		early = true
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
	}
	unblock()
	select {
	case <-returned:
	case <-time.After(3 * time.Second):
		t.Fatal("callback did not complete")
	}
	select {
	case err := <-warmDone:
		if err != nil {
			t.Errorf("EnsureWarm: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("warm caller did not complete")
	}
	if !early {
		select {
		case err := <-closeDone:
			if err != nil {
				t.Errorf("Close: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Close did not join after callback completion")
		}
	}
	if early {
		t.Error("Service.Close published completion before its accepted WarmFunc returned")
	}
}

func TestOwnerClassSearchNoPostCloseStart(t *testing.T) {
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	defer engine.Close()
	svc := search.NewService(engine, search.DefaultVectorDimensions, nil)
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 1)
	svc.SetLazyWarming(true, func() { entered <- struct{}{} })
	err := svc.EnsureWarm(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Errorf("EnsureWarm after Close: expected canceled lifetime, got %v", err)
	}
	select {
	case <-entered:
		t.Error("callback admitted after Service.Close")
	default:
	}
	if err := svc.BuildIndexes(context.Background()); !errors.Is(err, context.Canceled) {
		t.Errorf("BuildIndexes after Close: expected canceled lifetime, got %v", err)
	}
}

func TestOwnerClassSearchRetainsPersistenceOutcome(t *testing.T) {
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	defer engine.Close()
	svc := search.NewService(engine, search.DefaultVectorDimensions, nil)
	parent := filepath.Join(t.TempDir(), "regular-file")
	if err := os.WriteFile(parent, []byte("cannot contain an index"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.CreateNode(&storage.Node{ID: "nornic:n", Labels: []string{"Note"}, Properties: map[string]interface{}{"text": "search lifetime outcome"}}); err != nil {
		t.Fatal(err)
	}
	if err := svc.BuildIndexes(context.Background()); err != nil {
		t.Fatal(err)
	}
	svc.SetFulltextIndexPath(filepath.Join(parent, "fulltext"))
	svc.SetPersistenceEnabled(true)
	svc.PersistIndexesToDisk()
	err := svc.Close()
	if err == nil || !strings.Contains(err.Error(), "BM25 save") {
		t.Errorf("native persistence failure omitted from Service.Close: %v", err)
	}
}
