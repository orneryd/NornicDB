package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// IndexSourceStampedView is the metadata certificate of an already captured
// read view. It does not iterate/hydrate candidate nodes, decode properties or
// read vector payloads. The work is O(number of source metadata keys), with
// constant working memory. This work must be reported separately from candidate
// acquisition; a zero node-iterator count does not mean zero validation work.
type IndexSourceStampedView interface {
	IndexSourceStamp(context.Context) (string, error)
}

// ReadIndexSourceStamp obtains an optional durable source certificate from the
// actual engine or captured read view. Custom Engine implementations need not add a new method:
// absent capability means rebuild through their own readers after restart.
// Wrappers may forward only when they preserve their inner committed rows.
func ReadIndexSourceStamp(ctx context.Context, view any) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if source, ok := view.(IndexSourceStampedView); ok {
		return source.IndexSourceStamp(ctx)
	}
	return "", ErrReadSnapshotUnsupported
}

// IndexSourceStamp uses the existing capture owner, including its pending-write
// cut. The digest identifies immutable physical record versions plus the durable
// property-source identity that changes on replacement/restore. It is not a
// checksum of a second projection of the graph.
func (b *BadgerEngine) IndexSourceStamp(ctx context.Context) (string, error) {
	view, err := b.OpenReadSnapshot(ctx)
	if err != nil {
		return "", err
	}
	defer view.Close()
	return view.(IndexSourceStampedView).IndexSourceStamp(ctx)
}

func (s *badgerReadSnapshot) IndexSourceStamp(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	// These readers can select different rows without a durable change:
	// scoring uses the clock/access history, and reveal is query-local.
	// With active manual suppressions a transient reveal scope can change
	// selection and finish before the final check, without any stored write.
	if s.b.decayEnabled || s.b.revealAll.Load() || s.b.HasManualSuppressions() {
		return "", ErrReadSnapshotUnsupported
	}
	// Buffered images have not acquired immutable physical versions yet. They
	// are still read normally by the build, but are never certified as a disk
	// source by their process-local generation or by a write-quiet retry loop.
	if len(s.nodesTouched) != 0 || len(s.edgesTouched) != 0 {
		return "", ErrReadSnapshotUnsupported
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("nornic-index-source-v4\x00"))
	var prefixLength [8]byte
	binary.LittleEndian.PutUint64(prefixLength[:], uint64(len(s.prefix)))
	_, _ = hash.Write(prefixLength[:])
	_, _ = hash.Write([]byte(s.prefix))
	err := s.view.withView(ctx, func(txn *badger.Txn) error {
		identity, err := txn.Get(walPropertyIdentityKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrReadSnapshotUnsupported
		}
		if err != nil {
			return err
		}
		if err := identity.Value(func(value []byte) error { _, err := hash.Write(value); return err }); err != nil {
			return err
		}
		// Nodes include all user properties, labels, visibility, decay and
		// inline vectors. The separate embedding prefix includes sidecar-only
		// updates that intentionally leave body/MVCC sequences unchanged.
		// Schema, heads and dictionaries are reader inputs, not entity counts.
		// Allocation counters, clean-shutdown flags and derived adjacency/label
		// indexes are not these readers' source and are deliberately excluded.
		prefixes := []byte{prefixNode, prefixEdge, prefixEmbedding, prefixSchema,
			prefixMVCCNodeHead, prefixMVCCEdgeHead,
			prefixIDDictNodeForward, prefixIDDictEdgeForward, prefixIDDictNodeReverse, prefixIDDictEdgeReverse,
			prefixPropKeyForward, prefixPropKeyReverse}
		// Schema policy and suppression are consulted live during population.
		// Schema create/drop must change the proof even when its final encoded
		// value is identical: a transient temporal rule can exclude a row.
		// Suppression is separate from body records and is consulted live by
		// relationship streams. Include its newest version even when deleted:
		// otherwise suppress/unsuppress could restore identical bytes after
		// hiding a row during acquisition. The caller must keep its original
		// snapshot pinned through final certification so GC cannot retire that
		// intervening tombstone. GC after release may cause a safe rebuild.
		prefixSets := make([][]byte, 0, len(prefixes)+1)
		for _, prefix := range prefixes {
			prefixSets = append(prefixSets, []byte{prefix})
		}
		prefixSets = append(prefixSets, []byte{prefixMVCCMeta, prefixMVCCMetaManualSuppression})
		var encoded [16]byte
		for _, prefix := range prefixSets {
			history := len(prefix) == 2 || prefix[0] == prefixSchema
			opts := badgerPrefixIteratorOptions(prefix)
			opts.AllVersions = history
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			var previous []byte
			for it.Rewind(); it.Valid(); it.Next() {
				if err := ctx.Err(); err != nil {
					it.Close()
					return err
				}
				item := it.Item()
				key := item.Key()
				if history {
					if bytes.Equal(previous, key) {
						continue
					}
					previous = append(previous[:0], key...)
				}
				binary.LittleEndian.PutUint64(encoded[:8], uint64(len(key)))
				if history || prefix[0] == prefixNode || prefix[0] == prefixEdge || prefix[0] == prefixEmbedding {
					binary.LittleEndian.PutUint64(encoded[8:], item.Version())
					_, _ = hash.Write(encoded[:])
					_, _ = hash.Write(key)
				} else {
					// Reader metadata is small and is hashed by value: cache
					// scope does not imply reader neutrality, and a harmless
					// re-persistence of identical bindings is not a change.
					err := item.Value(func(value []byte) error {
						binary.LittleEndian.PutUint64(encoded[8:], uint64(len(value)))
						_, _ = hash.Write(encoded[:])
						_, _ = hash.Write(key)
						_, _ = hash.Write(value)
						return nil
					})
					if err != nil {
						it.Close()
						return err
					}
				}
			}
			it.Close()
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (s *namespacedReadSnapshot) IndexSourceStamp(ctx context.Context) (string, error) {
	return ReadIndexSourceStamp(ctx, s.inner)
}

// IndexSourceStamp preserves the same namespace composition as capture.
func (n *NamespacedEngine) IndexSourceStamp(ctx context.Context) (string, error) {
	view, err := n.OpenReadSnapshot(ctx)
	if err != nil {
		return "", err
	}
	defer view.Close()
	return ReadIndexSourceStamp(ctx, view)
}

func (w *WALEngine) IndexSourceStamp(ctx context.Context) (string, error) {
	return ReadIndexSourceStamp(ctx, w.engine)
}

func (t *TracedEngine) IndexSourceStamp(ctx context.Context) (string, error) {
	return ReadIndexSourceStamp(ctx, t.Engine)
}
