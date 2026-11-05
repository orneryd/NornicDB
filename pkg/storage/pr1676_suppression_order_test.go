package storage

import (
	"context"
	"fmt"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

// Hold the suppression after its physical action while an endpoint deletion
// starts. The original PR observed DELETE followed by the earlier UPDATE.
func TestSuppressionBeforeCascadeNotificationOrder(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, producer := range []string{"manual", "decay"} {
			t.Run(fmt.Sprintf("buffered=%v/%s", buffered, producer), func(t *testing.T) {
				b, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour}))
				require.NoError(t, err)
				defer b.Close()
				ctx := context.Background()
				if producer == "decay" {
					b.SetDecayEnabled(true)
					schema := b.GetSchemaForNamespace("test")
					require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "edge_decay", Scope: knowledgepolicy.ScopeEdge, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 1, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
					require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "links", ProfileRef: "edge_decay", TargetEdgeType: "R", IsEdge: true}))
				}
				_, err = b.CreateNode(ctx, &Node{ID: "test:a"})
				require.NoError(t, err)
				_, err = b.CreateNode(ctx, &Node{ID: "test:b"})
				require.NoError(t, err)
				old := time.Now().Add(-72 * time.Hour)
				require.NoError(t, b.CreateEdge(ctx, &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R", CreatedAt: old, UpdatedAt: old}))
				var events publishedEvents
				b.OnEdgeUpdated(func(e *Edge) { events.add("update:" + string(e.ID)) })
				b.OnEdgeDeleted(func(id EdgeID) { events.add("delete:" + string(id)) })
				entered, resume := make(chan struct{}), make(chan struct{})
				paused := WithPublicationDecision(ctx, func(_ context.Context, _ PublicationIdentity, action func() error) error {
					err := action()
					if err == nil {
						close(entered)
						<-resume
					}
					return err
				})
				updated := make(chan error, 1)
				go func() {
					if producer == "manual" {
						updated <- b.SetManualSuppression(paused, StoredEntityEdgeKind, "test:r", true)
					} else {
						_, err := b.EnqueueDeindexIfSuppressed(paused, "test:r", true)
						updated <- err
					}
				}()
				select {
				case <-entered:
				case <-time.After(10 * time.Second):
					close(resume)
					t.Fatal("suppression did not reach the commit boundary")
				}
				deleted := make(chan error, 1)
				started := make(chan struct{})
				go func() {
					close(started)
					deleted <- b.DeleteNode(ctx, "test:a")
				}()
				<-started
				select {
				case err := <-deleted:
					close(resume)
					require.NoError(t, err)
					require.NoError(t, <-updated)
					t.Fatal("cascade completed before the earlier commit was published")
				case <-time.After(50 * time.Millisecond):
				}
				close(resume)
				require.NoError(t, <-updated)
				require.NoError(t, <-deleted)
				events.mu.Lock()
				defer events.mu.Unlock()
				require.Equal(t, []string{"update:test:r", "delete:test:r"}, events.events)
			})
		}
	}
}
