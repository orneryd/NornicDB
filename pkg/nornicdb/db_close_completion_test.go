package nornicdb

import (
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type heldCloseStorage struct {
	storage.Engine
	entered, release chan struct{}
	err              error
}

func (s *heldCloseStorage) Close() error {
	close(s.entered)
	<-s.release
	return errors.Join(s.Engine.Close(), s.err)
}

func TestDBConcurrentCloseSharesCompletionAndOutcome(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DeferSearchWarmup = true
	db, err := Open("", cfg)
	require.NoError(t, err)
	cause := errors.New("storage close failure")
	store := &heldCloseStorage{Engine: db.baseStorage, entered: make(chan struct{}), release: make(chan struct{}), err: cause}
	db.baseStorage = store
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- db.Close() }()
	<-store.entered
	go func() { second <- db.Close() }()
	var secondErr error
	returnedEarly := false
	select {
	case secondErr = <-second:
		returnedEarly = true
	case <-time.After(50 * time.Millisecond):
	}
	close(store.release)
	firstErr := <-first
	if !returnedEarly {
		secondErr = <-second
	}
	require.False(t, returnedEarly, "Close returned before the owning Close completed")
	require.ErrorIs(t, firstErr, cause)
	require.True(t, firstErr == secondErr, "every Close must return the owner's same outcome")
	require.True(t, firstErr == db.Close())
}
