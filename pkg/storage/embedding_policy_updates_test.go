package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDatabaseEmbeddingEditsPreserveIndependentPolicyFields(t *testing.T) {
	for range 32 {
		engine := &BadgerEngine{}
		engine.SetEmbeddingLabelPolicy("a", []string{"Document"}, nil)
		engine.SetEmbeddingLabelPolicy("b", []string{"Other"}, nil)
		var work sync.WaitGroup
		start := make(chan struct{})
		work.Add(2)
		go func() {
			defer work.Done()
			<-start
			engine.SetEmbeddingLabelPolicy("a", []string{"Image"}, []string{"Audit"})
		}()
		go func() {
			defer work.Done()
			<-start
			engine.SetDatabaseEmbeddingsEnabled("a", false)
		}()
		close(start)
		work.Wait()
		require.False(t, engine.IsDatabaseEmbeddingsEnabled("a"))
		require.False(t, engine.SetDatabaseEmbeddingsEnabled("a", false), "repeating the same setting is unchanged")
		require.True(t, engine.SetDatabaseEmbeddingsEnabled("a", true))
		for _, test := range []struct {
			id      NodeID
			labels  []string
			allowed bool
		}{
			{"a:image", []string{"Image"}, true},
			{"a:audit", []string{"Image", "Audit"}, false},
			{"a:document", []string{"Document"}, false},
			{"b:other", []string{"Other"}, true},
			{"b:image", []string{"Image"}, false},
		} {
			require.Equal(t, test.allowed, engine.embeddingLabelsAllowed(&Node{ID: test.id, Labels: test.labels}), string(test.id))
		}
	}
}
