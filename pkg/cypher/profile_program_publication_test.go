package cypher

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProfileKnownZeroOccurrenceReplacesEnclosingOrigin(t *testing.T) {
	enclosing := withProfileRuntimeSubqueryOccurrence(t.Context(), 52)
	first := withProfileRuntimeSubqueryOccurrence(enclosing, 0)
	require.Zero(t, profileRuntimeSubqueryOccurrence(first), "the first written source position is known, not inherited")
	require.Equal(t, 52, profileRuntimeSubqueryOccurrence(enclosing), "the enclosing invocation keeps its own origin")
}

func TestProfileProgramCacheDoesNotPublishCanceledCompilation(t *testing.T) {
	type program struct{ source string }
	var cache profileProgramCache[*program]
	ctx, cancel := context.WithCancel(t.Context())
	require.Nil(t, cache.program(ctx, "expression", func(context.Context, string) *program {
		cancel()
		return &program{source: "incomplete"}
	}))
	complete := &program{source: "complete"}
	require.Same(t, complete, cache.program(t.Context(), "expression", func(context.Context, string) *program {
		return complete
	}))
	unexpected := func(context.Context, string) *program {
		t.Error("an accepted cached program must not be compiled again")
		return nil
	}
	require.Same(t, complete, cache.program(t.Context(), "expression", unexpected))
	require.Nil(t, cache.program(ctx, "expression", unexpected), "a canceled request cannot resume from the cached program")
}

func TestProfileProgramCacheConcurrentReadersShareOnePublication(t *testing.T) {
	type program struct{ compiler int }
	var cache profileProgramCache[*program]
	const readers = 8
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	started := make(chan struct{}, readers)
	release := make(chan struct{})
	var tasks sync.WaitGroup
	var results [readers]*program
	for index := range readers {
		tasks.Add(1)
		go func(index int) {
			defer tasks.Done()
			results[index] = cache.program(ctx, "expression", func(context.Context, string) *program {
				started <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
				}
				return &program{compiler: index}
			})
		}(index)
	}
	defer tasks.Wait()
	for range readers {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("source compilation must not hold the publication mutex")
		}
	}
	close(release)
	tasks.Wait()
	require.NotNil(t, results[0])
	for _, result := range results {
		require.Same(t, results[0], result, "all readers receive the accepted publication")
	}
}
