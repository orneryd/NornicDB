package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSchemaTokenCancellationDoesNotPublish(t *testing.T) {
	for _, edges := range []bool{false, true} {
		t.Run(map[bool]string{false: "labels", true: "relationships"}[edges], func(t *testing.T) {
			schema := NewSchemaManager()
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			persisted := 0
			schema.SetPersister(func(*SchemaDefinition) error { persisted++; return nil })
			var err error
			if edges {
				err = schema.RegisterEdgeTokens(ctx, []*Edge{{Type: "Rejected"}})
			} else {
				err = schema.RegisterTokens(ctx, []string{"Rejected"}, nil)
			}
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, persisted)
			def := schemaDefinitionForTest(t, schema)
			require.Empty(t, def.LabelTokens)
			require.Empty(t, def.RelationshipTypeTokens)
			require.NoError(t, schema.RegisterTokens(t.Context(), []string{"Healthy"}, nil))
			require.Equal(t, 1, persisted)
			require.Equal(t, []string{"Healthy"}, schemaDefinitionForTest(t, schema).LabelTokens)
		})
	}
}

func TestSchemaTokenCancellationReleasesWaitingAdmission(t *testing.T) {
	schema := NewSchemaManager()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	waiting := make(chan struct{}, 1)
	restore := setAdmissionWaitObserver(func(owner any, seen context.Context) {
		if owner == &schema.publicationGate {
			select {
			case waiting <- struct{}{}:
			default:
			}
		}
	})
	defer restore()
	schema.publicationGate.Lock()
	var once sync.Once
	unlock := func() { once.Do(schema.publicationGate.Unlock) }
	defer unlock()
	done := make(chan error, 1)
	go func() { done <- schema.RegisterTokens(ctx, []string{"Rejected"}, nil) }()
	joined := false
	defer func() {
		cancel()
		unlock()
		if !joined {
			<-done
		}
	}()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("token registration did not wait at publication admission")
	}
	cancel()
	select {
	case err := <-done:
		joined = true
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled token registration kept waiting")
	}
	unlock()
	require.Empty(t, schemaDefinitionForTest(t, schema).LabelTokens)
	require.NoError(t, schema.RegisterTokens(t.Context(), []string{"Healthy"}, nil))
}

func TestSchemaTokenAcceptedPersistenceFinishesPublication(t *testing.T) {
	schema := NewSchemaManager()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var stored *SchemaDefinition
	schema.SetPersister(func(def *SchemaDefinition) error { stored = def; cancel(); return nil })
	require.NoError(t, schema.RegisterTokens(ctx, []string{"Accepted"}, []string{"ACCEPTED"}))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.NotNil(t, stored)
	require.Equal(t, stored.LabelTokens, schemaDefinitionForTest(t, schema).LabelTokens)
	require.Equal(t, stored.RelationshipTypeTokens, schemaDefinitionForTest(t, schema).RelationshipTypeTokens)
}
