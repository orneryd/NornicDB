package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPreparedEntityCommitPreservesProducerMode(t *testing.T) {
	for _, mode := range []EntityCommitMode{CommitModeExplicit, CommitModeImplicit, CommitModeDirect} {
		t.Run(map[EntityCommitMode]string{CommitModeExplicit: "explicit", CommitModeImplicit: "implicit-single", CommitModeDirect: "direct"}[mode], func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true, manualDrain: true})
			tx, err := BeginEntityTransaction(b, mode)
			require.NoError(t, err)
			require.NoError(t, tx.SetNamespace("test"))
			_, err = tx.CreateNode(&Node{ID: "test:a"})
			require.NoError(t, err)
			p, err := tx.PrepareEntityCommit(context.Background(), "producer-mode")
			require.NoError(t, err)
			require.Equal(t, mode, p.Mode())
			payload, err := EncodeEntityTransaction(p.Namespace(), p.LogicalID(), p.Operations(), p.Mode())
			require.NoError(t, err)
			envelope, _, err := DecodeEntityTransaction(payload)
			require.NoError(t, err)
			require.Equal(t, mode, envelope.Mode)
			r, err := b.SubmitEntityCommit(context.Background(), p, nil)
			require.NoError(t, err)
			require.NoError(t, r.Wait(context.Background()))
			require.Equal(t, mode == CommitModeImplicit, tx.AcknowledgedBuffered())
		})
	}
}
