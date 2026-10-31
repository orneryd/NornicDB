package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nativeHandleEntries are the public engine entries that reach the Badger handle
// and hold no write admission of their own: each must be joined or refused.
func nativeHandleEntries(engine *BadgerEngine) map[string]func() error {
	return map[string]func() error{
		"Sync":  engine.Sync,
		"RunGC": engine.RunGC,
		"Size": func() error {
			engine.Size()
			return nil
		},
		"EstimateSize": func() error {
			engine.EstimateSize([]byte{prefixNode})
			return nil
		},
		"DataDirFreeSpace": func() error {
			_, err := engine.DataDirFreeSpace()
			return err
		},
		"ConsumeCleanShutdownMarker": func() error {
			_, err := engine.ConsumeCleanShutdownMarker(context.Background())
			return err
		},
		"MarkCleanShutdown": func() error { return engine.MarkCleanShutdown(context.Background()) },
		"PruneMVCCVersions": func() error {
			_, err := engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{})
			return err
		},
		"CheckIDDictionary": func() error {
			_, err := engine.CheckIDDictionary()
			return err
		},
		"clearBadgerPrefix": func() error { return engine.clearBadgerPrefix(context.Background(), prefixMVCCNodeHead) },
		"persistMVCCSequence": func() error {
			engine.persistMVCCSequence("test")
			return nil
		},
		"CreateEdge": func() error {
			return engine.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "T"})
		},
	}
}

// Author: NOR-288 lane (Claude), 2026-10-31. After Close each entry answers
// "closed" or an empty result. None reaches the retired handle.
func TestNativeHandleEntriesAreRefusedAfterClose(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, engine.Close())
	valueless := map[string]bool{"Size": true, "EstimateSize": true, "persistMVCCSequence": true}
	for name, entry := range nativeHandleEntries(engine) {
		t.Run(name, func(t *testing.T) {
			require.NotPanics(t, func() {
				err := entry()
				if !valueless[name] {
					assert.ErrorIs(t, err, ErrStorageClosed)
				}
			})
		})
	}
	lsm, vlog := engine.Size()
	assert.Zero(t, lsm+vlog)
	assert.Nil(t, engine.DB())
}

// Author: NOR-288 lane (Claude), 2026-10-31. Every entry runs repeatedly while
// the engine closes. A read of the handle Close releases is a data race under
// -race and a nil dereference without it.
func TestNativeHandleEntriesRaceClose(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	failures := make(chan string, 64)
	var running sync.WaitGroup
	for name, entry := range nativeHandleEntries(engine) {
		running.Add(1)
		go func() {
			defer running.Done()
			defer func() {
				if r := recover(); r != nil {
					failures <- fmt.Sprintf("%s used the retired handle: %v", name, r)
				}
			}()
			for !engine.work.Closing() {
				_ = entry()
			}
			_ = entry()
		}()
	}
	time.Sleep(20 * time.Millisecond)
	closed := make(chan error, 1)
	go func() { closed <- engine.Close() }()
	require.NoError(t, requireWithin(t, "Close with the entries running", closed))
	running.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
}
