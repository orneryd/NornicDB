package qdrantgrpc

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"sync"
	"testing"
	"time"
)

func TestQdrantR4RegistrationReentry(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	defer base.Close()
	store := newTestCollectionStore(t)
	s, err := NewServer(cfg, store, base, nil, nil)
	require.NoError(t, err)
	entered, read, escape := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(escape) }) }
	defer release()
	require.NoError(t, s.RegisterAdditionalServices(func(*grpc.Server) {
		close(entered)
		go func() { s.Addr(); close(read) }()
		select {
		case <-read:
		case <-escape:
		}
	}))
	started := make(chan error, 1)
	go func() { started <- s.Start() }()
	<-entered
	select {
	case <-read:
	case <-time.After(200 * time.Millisecond):
		t.Error("gRPC service registration held provider state lock across foreign callback")
	}
	release()
	require.NoError(t, <-started)
	s.Stop()
}
