// Author: Codex (#1636 lane). Public construction boundary for the DB/service class.
package nornicdb_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/search"
)

func classSearchDB(t *testing.T) *nornicdb.DB {
	t.Helper()
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.EmbeddingEnabled = false
	cfg.Database.PersistSearchIndexes = false
	cfg.Database.AsyncWritesEnabled = false
	db, err := nornicdb.Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return db
}
func TestOwnerClassSearchConstructionReentry(t *testing.T) {
	db := classSearchDB(t)
	defer db.Close()
	child := make(chan error, 1)
	blocked := false
	db.SetDbSearchOptionsResolver(func(name string) search.ServiceOptions {
		if name == "parent" {
			go func() { _, err := db.GetOrCreateSearchService("child", nil); child <- err }()
			select {
			case err := <-child:
				child <- err
			case <-time.After(100 * time.Millisecond):
				blocked = true
			}
		}
		return search.DefaultServiceOptions(name)
	})
	if _, err := db.GetOrCreateSearchService("parent", nil); err != nil {
		t.Error(err)
	}
	select {
	case err := <-child:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reentrant child did not finish after controlled escape")
	}
	if blocked {
		t.Error("search construction holds a host lock across its options resolver")
	}
}
func TestOwnerClassSearchConstructionActualCompletion(t *testing.T) {
	db := classSearchDB(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	db.SetDbSearchOptionsResolver(func(name string) search.ServiceOptions {
		if name == "held" {
			close(entered)
			<-release
		}
		return search.DefaultServiceOptions(name)
	})
	constructed := make(chan error, 1)
	go func() { _, err := db.GetOrCreateSearchService("held", nil); constructed <- err }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("constructor did not reach resolver")
	}
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	early := false
	select {
	case err := <-closed:
		early = true
		if err != nil {
			t.Error(err)
		}
	case <-time.After(100 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-constructed:
		if !errors.Is(err, nornicdb.ErrClosed) {
			t.Errorf("construction crossing DB.Close published a service: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("constructor did not finish")
	}
	if !early {
		select {
		case err := <-closed:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Close did not join released constructor")
		}
	}
	if early {
		t.Error("DB.Close completed before its accepted service constructor returned")
	}
}
