package search

import (
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Applying one entity's indexed image must order newer work for that entity,
// but should not stop an unrelated entity from reserving its own work.
func TestIndependentSearchMutationReservationsCanProceed(t *testing.T) {
	svc := &Service{}
	first := svc.ReserveNodeMutation(storage.NodeID("entity-a"))
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	finish := func() { releaseOnce.Do(func() { close(release) }) }
	defer finish()
	applyDone := make(chan error, 1)
	go func() {
		applyDone <- applyReservedIndexMutation(svc, first.service, first.nodeID, first.generation, &svc.pendingNodeMutations, func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	otherDone := make(chan NodeMutation, 1)
	go func() { otherDone <- svc.ReserveNodeMutation(storage.NodeID("entity-b")) }()
	sameStarted, sameDone := make(chan struct{}), make(chan NodeMutation, 1)
	go func() {
		close(sameStarted)
		sameDone <- svc.ReserveNodeMutation(storage.NodeID("entity-a"))
	}()
	<-sameStarted
	var other NodeMutation
	select {
	case other = <-otherDone:
	case <-time.After(2 * time.Second):
		finish()
		<-applyDone
		t.Fatal("an independent entity reservation waited for another entity's index application")
	}
	select {
	case <-sameDone:
		t.Fatal("a newer reservation for the same entity bypassed the active application")
	default:
	}
	finish()
	require.NoError(t, <-applyDone)
	same := <-sameDone
	svc.DiscardNodeMutation(other)
	svc.DiscardNodeMutation(same)
}
