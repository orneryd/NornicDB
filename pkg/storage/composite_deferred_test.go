package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeferredCompositeResolvesOnlyUsedConstituent(t *testing.T) {
	localCalls, remoteCalls := 0, 0
	local, remote := NewMemoryEngine(), NewMemoryEngine()
	engine := NewDeferredCompositeEngine(map[string]func() (Engine, error){
		"local":  func() (Engine, error) { localCalls++; return local, nil },
		"remote": func() (Engine, error) { remoteCalls++; return remote, nil },
	}, map[string]string{"local": "localdb", "remote": "peer"}, map[string]string{"local": "read_write", "remote": "read"})
	engine.SetCompositeName("deferred")
	require.Equal(t, "deferred", engine.CompositeName())
	require.ElementsMatch(t, []string{"local", "remote"}, engine.getConstituentsForRead())
	require.Zero(t, localCalls)
	require.Zero(t, remoteCalls)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := engine.GetConstituentByAlias("local"); require.NoError(t, err) }()
	}
	wg.Wait()
	require.Equal(t, 1, localCalls)
	require.Zero(t, remoteCalls)
	require.NoError(t, engine.Close())
	require.Zero(t, remoteCalls, "closing an unused constituent never opens it")
	require.NoError(t, remote.Close())
}
