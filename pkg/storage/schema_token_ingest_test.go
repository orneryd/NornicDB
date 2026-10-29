package storage

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestKnownSchemaTokenDoesNotWaitForUnrelatedTokenPersistence(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.RegisterTokens([]string{"Existing"}, nil))
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	defer finish()
	schema.SetPersister(func(*SchemaDefinition) error { close(entered); <-release; return nil })
	newDone := make(chan error, 1)
	go func() { newDone <- schema.RegisterTokens(nil, []string{"NEW"}) }()
	<-entered
	knownDone := make(chan error, 1)
	go func() { knownDone <- schema.RegisterTokens([]string{"Existing"}, nil) }()
	newStarted, sameNewDone := make(chan struct{}), make(chan error, 1)
	go func() {
		close(newStarted)
		sameNewDone <- schema.RegisterTokens(nil, []string{"NEW"})
	}()
	<-newStarted
	select {
	case err := <-knownDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		finish()
		<-newDone
		t.Fatal("an existing label waited for a different relationship token to persist")
	}
	select {
	case err := <-sameNewDone:
		t.Fatalf("uncommitted relationship token was accepted before persistence: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	finish()
	require.NoError(t, <-newDone)
	require.NoError(t, <-sameNewDone)
	require.Equal(t, []string{"Existing"}, schema.ExportDefinition().LabelTokens)
	require.Equal(t, []string{"NEW"}, schema.ExportDefinition().RelationshipTypeTokens)
}

func TestKnownSchemaTokenSnapshotFollowsReplacementAndFailure(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.RegisterTokens([]string{"Old"}, nil))
	require.NoError(t, schema.ReplaceFromDefinition(&SchemaDefinition{LabelTokens: []string{"Replacement"}}))
	sentinel := errors.New("token write rejected")
	schema.SetPersister(func(*SchemaDefinition) error { return sentinel })
	require.NoError(t, schema.RegisterTokens([]string{"Replacement"}, nil))
	require.ErrorIs(t, schema.RegisterTokens([]string{"Old"}, nil), sentinel)
	require.Equal(t, []string{"Replacement"}, schema.ExportDefinition().LabelTokens)
}

func TestKnownSchemaTokenSnapshotFollowsTransactionSchemaInstall(t *testing.T) {
	schema := NewSchemaManager()
	require.NoError(t, schema.RegisterTokens([]string{"Old"}, nil))
	transactionSchema := NewSchemaManager()
	require.NoError(t, transactionSchema.ReplaceFromDefinition(&SchemaDefinition{LabelTokens: []string{"Replacement"}}))
	schema.mu.Lock()
	schema.installTransactionSchemaLocked(transactionSchema)
	schema.mu.Unlock()

	sentinel := errors.New("token write rejected")
	schema.SetPersister(func(*SchemaDefinition) error { return sentinel })
	require.NoError(t, schema.RegisterTokens([]string{"Replacement"}, nil))
	require.ErrorIs(t, schema.RegisterTokens([]string{"Old"}, nil), sentinel)
	require.Equal(t, []string{"Replacement"}, schema.ExportDefinition().LabelTokens)
}

func BenchmarkRegisterKnownLabelParallel(b *testing.B) {
	schema := NewSchemaManager()
	if err := schema.RegisterTokens([]string{"Existing"}, nil); err != nil {
		b.Fatal(err)
	}
	labels := []string{"Existing"}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := schema.RegisterTokens(labels, nil); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
