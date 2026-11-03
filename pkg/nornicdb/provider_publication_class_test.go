package nornicdb

// Author: Codex. Public DB/provider publication class: metadata is extension
// work, and must remain retained while running outside publication locks.
import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type publicationClassProvider struct {
	*classReleaseEmbedder
	method  string
	check   func()
	checked atomic.Bool
}

func TestDBOwnerClassProviderPublicationActualCompletion(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingEnabled = true
	cfg.EmbeddingWorker.NumWorkers = 0
	db, err := Open(t.TempDir(), cfg)
	require.NoError(t, err)
	gate, entered := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	p := &publicationClassProvider{classReleaseEmbedder: &classReleaseEmbedder{}, method: "Model", check: func() { close(entered); <-gate }}
	installed := make(chan bool, 1)
	go func() { installed <- db.SetEmbedder(p) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("metadata did not enter")
	}
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("DB.Close passed the actual metadata completion: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.Zero(t, p.closes.Load(), "model disposed while metadata still uses it")
	release()
	select {
	case <-installed:
	case <-time.After(time.Second):
		t.Fatal("setter did not finish")
	}
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("DB.Close did not join released metadata")
	}
	require.EqualValues(t, 1, p.closes.Load())
}

func (p *publicationClassProvider) Dimensions() int {
	if p.method == "Dimensions" && p.checked.CompareAndSwap(false, true) {
		p.check()
	}
	return p.classReleaseEmbedder.Dimensions()
}
func (p *publicationClassProvider) Model() string {
	if p.method == "Model" && p.checked.CompareAndSwap(false, true) {
		p.check()
	}
	return p.classReleaseEmbedder.Model()
}

func TestDBOwnerClassProviderPublicationReentry(t *testing.T) {
	for _, method := range []string{"Model", "Dimensions"} {
		t.Run(method, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Memory.EmbeddingEnabled = true
			cfg.EmbeddingWorker.NumWorkers = 0
			db, err := Open(t.TempDir(), cfg)
			require.NoError(t, err)
			defer db.Close()
			var blocked atomic.Bool
			callbacks := make(chan struct{}, 1)
			p := &publicationClassProvider{classReleaseEmbedder: &classReleaseEmbedder{}, method: method}
			p.check = func() {
				done := make(chan struct{})
				go func() { _ = db.GetEmbedQueue(); close(done); callbacks <- struct{}{} }()
				select {
				case <-done:
				case <-time.After(200 * time.Millisecond):
					blocked.Store(true)
				}
			}
			require.True(t, db.SetEmbedder(p))
			select {
			case <-callbacks:
			case <-time.After(time.Second):
				t.Fatal("metadata callback did not unwind after its controlled escape")
			}
			require.True(t, p.checked.Load(), "public setter did not invoke provider metadata")
			require.False(t, blocked.Load(), "provider metadata could not reenter the DB publication API")
		})
	}
}
