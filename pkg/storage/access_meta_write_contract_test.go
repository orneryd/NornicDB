package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

func TestAccessMetaConditionalWriterStoredAndRetiredEntities(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, kind := range []StoredEntityKind{StoredEntityNodeKind, StoredEntityEdgeKind} {
			name := "direct/node"
			if buffered {
				name = "buffered/node"
			}
			if kind == StoredEntityEdgeKind {
				name += "/edge"
			}
			t.Run(name, func(t *testing.T) {
				var b *BadgerEngine
				if buffered {
					b = newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
				} else {
					b = newTestEngine(t)
				}
				id := "test:target"
				create := func() {
					if kind == StoredEntityNodeKind {
						if buffered {
							bufferedCreate(t, b, &Node{ID: NodeID(id), Labels: []string{"Doc"}})
						} else {
							_, err := b.CreateNode(t.Context(), &Node{ID: NodeID(id), Labels: []string{"Doc"}})
							require.NoError(t, err)
						}
					} else {
						if buffered {
							bufferedWrite(t, b, func(tx *BadgerTransaction) error {
								return tx.CreateEdge(t.Context(), &Edge{ID: EdgeID(id), StartNode: "test:a", EndNode: "test:z", Type: "LINK"})
							})
						} else {
							require.NoError(t, b.CreateEdge(t.Context(), &Edge{ID: EdgeID(id), StartNode: "test:a", EndNode: "test:z", Type: "LINK"}))
						}
					}
				}
				if kind == StoredEntityEdgeKind {
					for _, node := range []NodeID{"test:a", "test:z"} {
						_, err := b.CreateNode(t.Context(), &Node{ID: node})
						require.NoError(t, err)
					}
				}
				create()
				if buffered {
					require.Positive(t, b.writeBehind.PendingOps())
				}
				request := knowledgepolicy.AccessMetaWriteRequest{Mode: knowledgepolicy.FlushExistingEntity, EntityID: id, Entry: &knowledgepolicy.AccessMetaEntry{TargetID: id, Fixed: knowledgepolicy.AccessMetaFixedFields{AccessCount: 7}}}
				written, err := b.WriteAccessMeta(context.Background(), request)
				require.NoError(t, err)
				require.True(t, written, "acknowledged buffered creation must be eligible")
				require.NoError(t, b.SetManualSuppression(context.Background(), kind, id, true))
				meta, err := b.GetEntityMeta(context.Background(), id)
				require.NoError(t, err)
				if kind == StoredEntityNodeKind {
					require.Equal(t, knowledgepolicy.ScopeNode, meta.Scope)
				} else {
					require.Equal(t, knowledgepolicy.ScopeEdge, meta.Scope)
				}
				request.Entry.Fixed.AccessCount = 9
				written, err = b.WriteAccessMeta(context.Background(), request)
				require.NoError(t, err)
				require.True(t, written, "hidden stored entity remains eligible")
				tx, txErr := b.BeginTransaction(t.Context())
				require.NoError(t, txErr)
				require.NoError(t, tx.SetNamespace("test"))
				if kind == StoredEntityNodeKind {
					require.NoError(t, tx.DeleteGraphNode(t.Context(), NodeID(id), CascadeEdges))
				} else {
					require.NoError(t, tx.DeleteEdge(t.Context(), EdgeID(id)))
				}
				require.NoError(t, tx.Rollback())
				written, err = b.WriteAccessMeta(context.Background(), request)
				require.NoError(t, err)
				require.True(t, written, "rolled-back deletion never retires live eligibility")

				if buffered {
					bufferedWrite(t, b, func(tx *BadgerTransaction) error {
						if kind == StoredEntityNodeKind {
							return tx.DeleteGraphNode(t.Context(), NodeID(id), CascadeEdges)
						}
						return tx.DeleteEdge(t.Context(), EdgeID(id))
					})
				} else if kind == StoredEntityNodeKind {
					require.NoError(t, b.DeleteNode(t.Context(), NodeID(id)))
				} else {
					require.NoError(t, b.DeleteEdge(t.Context(), EdgeID(id)))
				}
				request.Entry.Fixed.AccessCount = 99
				written, err = b.WriteAccessMeta(context.Background(), request)
				require.NoError(t, err)
				require.False(t, written, "acknowledged deletion must not be overwritten")
				got, err := b.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{EntityID: id})
				require.NoError(t, err)
				require.NotNil(t, got.Entry)
				require.Equal(t, int64(9), got.Entry.Fixed.AccessCount, "retained state survives skipped flush")
				_, err = b.GetEntityMeta(context.Background(), id)
				require.ErrorIs(t, err, knowledgepolicy.ErrEntityMetaNotFound)
				require.ErrorIs(t, err, ErrNotFound)
				create()
				written, err = b.WriteAccessMeta(context.Background(), request)
				require.NoError(t, err)
				require.True(t, written, "ID reuse preserves existing retained-ID semantics")
				got, err = b.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{EntityID: id})
				require.NoError(t, err)
				require.Equal(t, int64(99), got.Entry.Fixed.AccessCount)
			})
		}
	}
}

func TestAccessMetaWriteValidatesModeAndPreservesRejectedState(t *testing.T) {
	b := newTestEngine(t)
	entry := &knowledgepolicy.AccessMetaEntry{TargetID: "test:missing", Fixed: knowledgepolicy.AccessMetaFixedFields{AccessCount: 2}}
	for _, r := range []knowledgepolicy.AccessMetaWriteRequest{{EntityID: entry.TargetID, Entry: entry}, {Mode: 99, EntityID: entry.TargetID, Entry: entry}, {Mode: knowledgepolicy.RecordState, EntityID: entry.TargetID}, {Mode: knowledgepolicy.FlushExistingEntity, Entry: entry}, {Mode: knowledgepolicy.FlushExistingEntity, EntityID: policyWorkMetaIDPrefix + "x", Entry: entry}} {
		written, err := b.WriteAccessMeta(context.Background(), r)
		require.ErrorIs(t, err, ErrInvalidData)
		require.False(t, written)
	}
	written, err := b.WriteAccessMeta(context.Background(), knowledgepolicy.AccessMetaWriteRequest{Mode: knowledgepolicy.RecordState, EntityID: entry.TargetID, Entry: entry})
	require.NoError(t, err)
	require.True(t, written, "explicit restore requires no live body")
	// A serialization rejection commits no metadata or catalog replacement.
	invalid := *entry
	invalid.Overflow = map[string]any{"unsupported": make(chan int)}
	written, err = b.WriteAccessMeta(context.Background(), knowledgepolicy.AccessMetaWriteRequest{Mode: knowledgepolicy.RecordState, EntityID: entry.TargetID, Entry: &invalid})
	require.Error(t, err)
	require.False(t, written)
	got, err := b.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{EntityID: entry.TargetID})
	require.NoError(t, err)
	require.Equal(t, int64(2), got.Entry.Fixed.AccessCount)
	require.Empty(t, got.Entry.Overflow)
	require.NoError(t, b.Close())
	for _, mode := range []knowledgepolicy.AccessMetaWriteMode{knowledgepolicy.RecordState, knowledgepolicy.FlushExistingEntity} {
		written, err = b.WriteAccessMeta(context.Background(), knowledgepolicy.AccessMetaWriteRequest{Mode: mode, EntityID: entry.TargetID, Entry: entry})
		require.ErrorIs(t, err, ErrStorageClosed)
		require.False(t, written)
	}
}

func TestAccessEntityMetadataDoesNotMaskCorruptNodeAsEdge(t *testing.T) {
	b := newTestEngine(t)
	_, err := b.CreateNode(t.Context(), &Node{ID: "test:corrupt"})
	require.NoError(t, err)
	// Real corrupted body, not a fabricated error: the node decoder must fail.
	require.NoError(t, b.db.Update(func(txn *badger.Txn) error { return txn.Set(nodeKey("test:corrupt"), []byte{0xc1}) }))
	_, err = b.GetEntityMeta(context.Background(), "test:corrupt")
	require.Error(t, err)
	require.False(t, errors.Is(err, knowledgepolicy.ErrEntityMetaNotFound))
	require.False(t, errors.Is(err, ErrNotFound))
	written, err := b.WriteAccessMeta(context.Background(), knowledgepolicy.AccessMetaWriteRequest{Mode: knowledgepolicy.FlushExistingEntity, EntityID: "test:corrupt", Entry: &knowledgepolicy.AccessMetaEntry{TargetID: "test:corrupt"}})
	require.Error(t, err)
	require.False(t, written)
	got, err := b.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{EntityID: "test:corrupt"})
	require.NoError(t, err)
	require.Nil(t, got.Entry)
}
