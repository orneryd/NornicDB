package storage

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

type pausedNamespaceTaskStore struct {
	*BadgerEngine
	entered, resume chan struct{}
	once            sync.Once
}

func (s *pausedNamespaceTaskStore) RunNamespaceTask(token any, id string, run func() error) error {
	if id == "task_a:seed" {
		s.once.Do(func() { close(s.entered); <-s.resume })
	}
	return s.BadgerEngine.RunNamespaceTask(token, id, run)
}

func TestNamespaceDropRejectsAlreadyDrainedAccessWork(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()
	for _, id := range []NodeID{"task_a:seed", "task_b:seed"} {
		_, err := engine.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	acc := knowledgepolicy.NewAccessAccumulator(true, 0)
	engine.SetAccessAccumulator(acc)
	acc.IncrementAccess("task_a:seed")
	acc.IncrementAccess("task_b:seed")
	store := &pausedNamespaceTaskStore{BadgerEngine: engine.BadgerEngine, entered: make(chan struct{}), resume: make(chan struct{})}
	var once sync.Once
	unblock := func() { once.Do(func() { close(store.resume) }) }
	defer unblock()
	flusher := knowledgepolicy.NewAccessFlusher(acc, store, time.Hour)
	done := make(chan struct{})
	go func() { flusher.Flush(); close(done) }()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("flusher did not drain the old batch")
	}
	_, _, err := engine.DeleteByPrefixContext(context.Background(), "task_a:")
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "task_a:seed"})
	require.NoError(t, err)
	unblock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("flusher did not finish")
	}
	meta, err := engine.GetAccessMeta("task_a:seed")
	require.NoError(t, err)
	require.Nil(t, meta)
	meta, err = engine.GetAccessMeta("task_b:seed")
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.Equal(t, int64(1), meta.Fixed.AccessCount)
}
