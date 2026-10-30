package replication

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// This lower-layer control proves owner continuity/cancellation, not the
// #1667 route consumer's separate forwarded leaf/bulk admission integration.
func TestReplicationR4LifetimeContextOwner(t *testing.T) {
	for _, spec := range []struct {
		mode ReplicationMode
		role string
	}{
		{ModeStandalone, ""}, {ModeRaft, ""}, {ModeHAStandby, "primary"},
		{ModeHAStandby, "standby"}, {ModeMultiRegion, ""},
	} {
		for _, start := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%s/started=%t", spec.mode, spec.role, start), func(t *testing.T) {
				owner, err := NewReplicator(r4NativeConfig(spec.mode, spec.role), NewMockStorage())
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, owner.Shutdown()) })
				provider, ok := owner.(interface{ LifetimeContext() context.Context })
				if !ok {
					t.Error("native replicator has no public lifetime context for its consumers")
					return
				}
				lifetime := provider.LifetimeContext()
				require.NotNil(t, lifetime)
				require.NoError(t, lifetime.Err())
				if start {
					if native, ok := owner.(interface{ SetTransport(Transport) }); ok {
						native.SetTransport(NewMockTransport())
					}
					initialization, cancel := context.WithCancel(context.Background())
					require.NoError(t, owner.Start(initialization))
					cancel()
					require.Same(t, lifetime, provider.LifetimeContext(), "Start replaced an already published owner")
					require.NoError(t, lifetime.Err(), "the initialization caller cannot end the replicator's lifetime")
					require.NoError(t, owner.Start(context.Background()))
					require.Same(t, lifetime, provider.LifetimeContext())
				}
				require.NoError(t, owner.Shutdown())
				require.ErrorIs(t, lifetime.Err(), context.Canceled)
				require.Same(t, lifetime, provider.LifetimeContext())
			})
		}
	}
}
