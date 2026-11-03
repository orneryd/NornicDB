package heimdall

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type r4BifrostWriter struct {
	*httptest.ResponseRecorder
	write func() error
}

func (w *r4BifrostWriter) Write(p []byte) (int, error) {
	if err := w.write(); err != nil {
		return 0, err
	}
	return len(p), nil
}
func TestBifrostR4RetainsEveryWriterOutcome(t *testing.T) {
	b := NewBifrost(Config{BifrostEnabled: true})
	a, c := errors.New("writer a"), errors.New("writer c")
	for id, e := range map[string]error{"a": a, "c": c} {
		w := &r4BifrostWriter{httptest.NewRecorder(), func() error { return e }}
		b.RegisterClient(id, w, w)
	}
	err := b.SendMessage("hello")
	require.ErrorIs(t, err, a)
	require.ErrorIs(t, err, c)
}
func TestBifrostR4RegistryReentryAndRetirement(t *testing.T) {
	b := NewBifrost(Config{BifrostEnabled: true})
	entered, readNow, readDone, finish := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(finish) }) }
	defer release()
	w := &r4BifrostWriter{httptest.NewRecorder(), func() error {
		close(entered)
		<-readNow
		read := make(chan struct{})
		go func() { b.ConnectionCount(); close(read) }()
		select {
		case <-read:
			close(readDone)
		case <-finish:
		}
		<-finish
		return nil
	}}
	b.RegisterClient("a", w, w)
	sent := make(chan error, 1)
	go func() { sent <- b.SendMessage("held") }()
	<-entered
	retired := make(chan struct{})
	go func() { b.UnregisterClient("a"); close(retired) }()
	// The writer's public read must work while a disconnect is waiting for the
	// current write. Retirement must also join the actual response-writer user.
	time.Sleep(30 * time.Millisecond)
	close(readNow)
	select {
	case <-readDone:
	case <-time.After(200 * time.Millisecond):
		t.Error("registry lock held across response writer blocked reentry")
	}
	select {
	case <-retired:
		t.Error("retirement abandoned an accepted write")
	default:
	}
	release()
	require.NoError(t, <-sent)
	select {
	case <-retired:
	case <-time.After(time.Second):
		t.Fatal("disconnect did not join its writer")
	}
	_ = context.Background()
}
