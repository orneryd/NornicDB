package replication

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type publicationContractStorage struct {
	*MockStorage
	zero bool
}

func (s *publicationContractStorage) ApplyCommand(ctx context.Context, cmd *Command, mode WALPublicationMode) (CommandPublication, error) {
	publication, err := s.MockStorage.ApplyCommand(ctx, cmd, mode)
	if err == nil && s.zero {
		publication.Position = 0
	}
	return publication, err
}

func TestHAQuorumRequiresPositiveOwnPublishedPosition(t *testing.T) {
	nativeFailure := errors.New("native publication failure")
	for _, tc := range []struct {
		name      string
		zero      bool
		nativeErr error
	}{
		{name: "invalid zero", zero: true},
		{name: "valid positive"},
		{name: "native failure takes priority", zero: true, nativeErr: nativeFailure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := &publicationContractStorage{MockStorage: NewMockStorage(), zero: tc.zero}
			owner.applyErr = tc.nativeErr
			cfg := DefaultConfig()
			cfg.Mode = ModeHAStandby
			cfg.NodeID = "publication-contract-primary"
			cfg.HAStandby.Role = "primary"
			cfg.HAStandby.PeerAddr = "standby:7000"
			cfg.HAStandby.SyncMode = SyncQuorum
			primary, err := NewHAStandbyReplicator(cfg, owner)
			require.NoError(t, err)
			primary.SetTransport(NewMockTransport())
			require.NoError(t, primary.Start(t.Context()))
			t.Cleanup(func() { require.NoError(t, primary.Shutdown()) })
			// An existing ACK cannot make an invalid zero own-position meaningful.
			// The positive control instead obtains its ACK from the mock standby.
			if tc.zero {
				primary.walStreamer.AcknowledgePosition(1)
			}
			err = primary.Apply(t.Context(), deletePrefixCommand(t, "test:none"), time.Second)
			switch {
			case tc.nativeErr != nil:
				require.ErrorIs(t, err, tc.nativeErr)
			case tc.zero:
				require.ErrorContains(t, err, "positive WAL position")
			default:
				require.NoError(t, err)
			}
		})
	}
}
