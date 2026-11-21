package storage

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// L558: a named-vector element changing only from +0 to -0 alongside a
// property change must survive both the live commit and a crash after the
// journal write and before the store write (replay at open).
func TestEI4NamedVectorSignedZeroReplay(t *testing.T) {
	for _, policy := range []WALRecordPolicy{WALRecordsStandard, WALRecordsLarge} {
		for _, replay := range []bool{false, true} {
			name := map[bool]string{false: "live", true: "crash-before-apply"}[replay]
			t.Run(map[WALRecordPolicy]string{WALRecordsStandard: "standard/", WALRecordsLarge: "large/"}[policy]+name, func(t *testing.T) {
				dir := t.TempDir()
				open := func() *BadgerEngine {
					return newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true, records: policy})
				}
				b := open()
				w := b.bufferedCommitWAL
				_, err := NewWALEngine(b, w).CreateNode(t.Context(), &Node{ID: "test:zero", Properties: map[string]any{"text": "alpha"}, NamedEmbeddings: map[string][]float32{"custom": {1, 0}}})
				require.NoError(t, err)
				tx, err := BeginEntityTransaction(context.Background(), b, CommitModeExplicit)
				require.NoError(t, err)
				require.NoError(t, tx.SetNamespace("test"))
				n, err := tx.GetNode(t.Context(), "test:zero")
				require.NoError(t, err)
				n = CopyNode(n)
				n.NamedEmbeddings["custom"][1] = math.Float32frombits(0x80000000)
				n.Properties["changed"] = true
				require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: n}))
				prepared, err := tx.PrepareEntityCommit(t.Context(), EntityCommitRequest{LogicalID: "ei4-zero"})
				require.NoError(t, err)
				if replay {
					data, err := EncodeEntityTransaction(EntityTransactionRequest{Prepared: prepared})
					require.NoError(t, err)
					_, ops, err := DecodeEntityTransaction(data)
					require.NoError(t, err)
					for _, op := range ops {
						if op.Node != nil {
							t.Logf("journal record node named=%v bits=%08x", op.Node.NamedEmbeddings != nil, func() uint32 {
								if v := op.Node.NamedEmbeddings["custom"]; len(v) > 1 {
									return math.Float32bits(v[1])
								}
								return 0xffffffff
							}())
						}
					}
					prepared.Abort()
					require.NoError(t, w.AppendWithDatabase(OpEntityCommit, walEntityCommitData{Payload: data}, "test"))
					crashClose(t, b, w)
					b = open()
					count, err := RecoverBufferedCommitsAtOpen(b, b.bufferedCommitWAL)
					t.Logf("recovered=%d err=%v", count, err)
					require.NoError(t, err)
				} else {
					receipt, err := NewWALEngine(b, w).SubmitEntityCommit(t.Context(), prepared, nil)
					require.NoError(t, err)
					require.NoError(t, receipt.Wait(t.Context()))
				}
				got, err := b.GetNode(t.Context(), "test:zero")
				require.NoError(t, err)
				bits := math.Float32bits(got.NamedEmbeddings["custom"][1])
				t.Logf("SIGNEDZERO %s named vector element bits=%08x changed=%v", name, bits, got.Properties["changed"])
				require.Equal(t, uint32(0x80000000), bits)
			})
		}
	}
}
