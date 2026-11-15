package storage

import (
	"context"

	"github.com/dgraph-io/badger/v4"
)

// nodeBodyAccess keeps mutable query results detached. Commit validation may
// borrow an immutable version-matched body only for its immediate comparisons.
type nodeBodyAccess uint8

const (
	nodeBodyDetached nodeBodyAccess = iota
	nodeBodyBorrowed
)

// nodeBodyCacheAdmission controls insertion of newly decoded bodies. Query
// reads admit reusable bodies; one-pass checkpoint streams reuse resident
// version-matched bodies without admitting cold ones or evicting query data.
type nodeBodyCacheAdmission uint8

const (
	// nodeBodyCacheAdmit admits a body decoded on a cache miss.
	nodeBodyCacheAdmit nodeBodyCacheAdmission = iota
	// nodeBodyCacheNoAdmit decodes without admitting. A body already cached at
	// this record's version is still read, exactly as an admitting read reads
	// it.
	nodeBodyCacheNoAdmit
)

type nodeItemHydration uint8

const (
	nodeItemBodyOnly nodeItemHydration = iota
	nodeItemQueryMetadata
	nodeItemFullEmbeddings
)

// readNodeItemInTxn reuses only a body with this record's physical version.
// Worker embedding state is hydrated from the same transaction, independently
// of the body cache: sidecar writes do not change the node record's version.
//
// embeddingRecordsAbsent is established by the scan's own snapshot, never by
// a current-value cache. Legacy body flags still use normal hydration.
//
// decodes, a transaction's chunk-decode memo or nil, goes to the hydration's
// record reader (embeddingRecordReader.decodes). Only a full-embedding
// hydration decodes chunk vectors.
//
// admission decides whether a body this read decoded is admitted to that cache.
// A no-admission read still takes a matching entry, detached exactly as an
// admitting read takes it, and simply does not fill a miss.
func (b *BadgerEngine) readNodeItemInTxn(txn *badger.Txn, item *badger.Item, id NodeID, hydrate nodeItemHydration, admission nodeBodyCacheAdmission, embeddingRecordsAbsent bool, decodes *embeddingChunkDecodes) (*Node, error) {
	node, hit := b.cacheLoadNodeBody(id, item.Version(), nil, nodeBodyDetached)
	if !hit {
		err := item.Value(func(value []byte) error {
			var err error
			node, err = b.decodeNodeProjected(txn, namespaceForNodeID(id), value, nil)
			return err
		})
		if err != nil {
			return nil, err
		}
		if node == nil {
			return nil, ErrNotFound
		}
		if admission == nodeBodyCacheAdmit {
			b.cacheStoreNodeBody(id, item.Version(), node, txn.ReadTs())
		}
	}
	var err error
	switch hydrate {
	case nodeItemBodyOnly:
	case nodeItemQueryMetadata, nodeItemFullEmbeddings:
		if !embeddingRecordsAbsent || node.RuntimeMetadata().EmbeddingsStoredSeparately {
			node, err = b.loadNodeEmbeddingState(embeddingRecordReader{txn: txn, decodes: decodes}, node, id, hydrate == nodeItemFullEmbeddings)
			if err != nil {
				return nil, err
			}
		}
	default:
		return nil, ErrInvalidData
	}
	b.bindNodeReadSource(node, id, item.Version())
	return node, nil
}

// embeddingRecordsAbsentAt proves absence only within this transaction's snapshot.
// Chunks, shards and sidecar metadata share prefixEmbedding; any matching record
// keeps ordinary per-node hydration. An empty nodePrefix checks all namespaces.
func embeddingRecordsAbsentAt(ctx context.Context, txn *badger.Txn, nodePrefix string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	prefix := make([]byte, 0, 1+len(nodePrefix))
	prefix = append(prefix, prefixEmbedding)
	prefix = append(prefix, nodePrefix...)
	options := badger.DefaultIteratorOptions
	options.Prefix = prefix
	options.PrefetchValues = false
	it := txn.NewIterator(options)
	defer it.Close()
	it.Seek(prefix)
	absent := !it.ValidForPrefix(prefix)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return absent, nil
}

// Delay the iterator cost until the snapshot has a population to hydrate.
const snapshotEmbeddingAbsenceProbeReads = 64

// snapshotEmbeddingAbsence is one retained "this immutable physical snapshot
// holds no embedding record at all" proof, with the count of the node reads
// that may take it. It belongs to one snapshot, never to a node: its owner (a
// transaction's pinned snapshot, a scan's own view) keeps one record keyed by
// that snapshot's read version, so a re-pinned snapshot proves again and
// nothing is retained per node.
type snapshotEmbeddingAbsence struct {
	// readTs is the ReadTs of the snapshot this record belongs to; a
	// different version restarts the count and the proof.
	readTs uint64
	// reads counts the node reads of that snapshot that needed the embedding
	// state, so a bounded read never pays for the probe.
	reads uint64
	// proved reports that absent is this snapshot's answered range proof.
	proved bool
	// absent is that proof: the snapshot holds no embedding record at all.
	absent bool
}

// recordsAbsent reuses only snapshot's own range proof, and reports false to
// a read that may not take it: a snapshot below the threshold and a snapshot
// that holds an embedding record both keep their ordinary per-node lookups. It
// is not concurrency-safe; a transaction owns its record under tx.mu and a
// scan owns the view it reads.
func (p *snapshotEmbeddingAbsence) recordsAbsent(ctx context.Context, snapshot *badger.Txn) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	readTs := snapshot.ReadTs()
	if p.readTs != readTs {
		*p = snapshotEmbeddingAbsence{readTs: readTs}
	}
	if p.proved {
		return p.absent, nil
	}
	p.reads++
	if p.reads < snapshotEmbeddingAbsenceProbeReads {
		return false, nil
	}
	absent, err := embeddingRecordsAbsentAt(ctx, snapshot, "")
	if err != nil {
		// A canceled read keeps no proof: the count stays at the threshold, so
		// the next read of this snapshot probes again.
		return false, err
	}
	p.proved = true
	p.absent = absent
	return absent, nil
}

// bindNodeReadSource gives a decoded image the identity of the pinned body
// it came from. Point and scan reads use the same record, independently of
// whether a whole-node cache happened to contain the node. Besides embedding
// writeback provenance, native handlers comparing node images must not see
// different ownership records solely because one read was a scan.
func (b *BadgerEngine) bindNodeReadSource(node *Node, id NodeID, version uint64) {
	if node == nil {
		return
	}
	revision := EmbeddingBodyRevision{physical: version}
	if node.runtime != nil && node.runtime.embeddingOwner == b && node.runtime.embeddingOwnerID == id && node.runtime.embeddingRevision == revision {
		return
	}
	state := node.runtimeState()
	state.embeddingOwner, state.embeddingOwnerID = b, id
	state.embeddingRevision = revision
	node.setRuntimeState(state)
}

// cacheLoadEdgeAtVersion is the version-checked form of the edge body's hot
// cache. A later body can never replace the record in a pinned transaction.
func (b *BadgerEngine) cacheLoadEdgeAtVersion(id EdgeID, version uint64) (*Edge, bool) {
	b.edgeCacheMu.RLock()
	defer b.edgeCacheMu.RUnlock()
	edge, found := b.edgeCache[id]
	return edge, found && edge != nil && b.edgeCacheVersions[id] == version
}

// readProjectedNodeItemInTxn shares the versioned body decision between point,
// label and MVCC projection readers. A nil projection retains full hydration.
func (b *BadgerEngine) readProjectedNodeItemInTxn(txn *badger.Txn, item *badger.Item, id NodeID, properties []string, include map[string]struct{}, access nodeBodyAccess, admitNew ...bool) (*Node, error) {
	if properties == nil {
		return b.readNodeItemInTxn(txn, item, id, nodeItemFullEmbeddings, nodeBodyCacheAdmit, false, nil)
	}
	read := b.readNodeBodyCache(id, item.Version(), properties, access, true, false)
	node := read.node
	// A label scan whose population exceeds the cache cannot retain its scan
	// images. It still widens any version-matched entry already held; point
	// reads retain their usual admission and eviction behavior.
	admit := len(admitNew) == 0 || admitNew[0] || node != nil
	if !read.complete {
		decodeInclude := include
		if node != nil {
			decodeInclude = make(map[string]struct{}, len(read.missing))
			for _, name := range read.missing {
				decodeInclude[name] = struct{}{}
			}
		}
		err := item.Value(func(value []byte) error {
			decoded, err := b.decodeNodeProjected(txn, namespaceForNodeID(id), value, decodeInclude)
			if err != nil {
				return err
			}
			if node == nil {
				node = decoded
			} else if decoded != nil {
				if node.Properties == nil {
					node.Properties = make(map[string]any, len(decoded.Properties))
				}
				for name, value := range decoded.Properties {
					node.Properties[name] = value
				}
			}
			return nil
		})
		if err != nil {
			return node, err
		}
		if admit {
			b.cacheStoreNodeBody(id, item.Version(), node, txn.ReadTs(), properties)
		}
	}
	// A projection that names has_embedding gets the node's embedding metadata
	// from this read's snapshot, as a whole-node read does, and none of the
	// vectors. A borrowed body is the cache's own and is left as it is.
	if node != nil && access == nodeBodyDetached && projectionHydration(nodeItemBodyOnly, include) != nodeItemBodyOnly {
		return b.loadNodeEmbeddingState(embeddingRecordReader{txn: txn}, node, id, false)
	}
	return node, nil
}

// hasEmbeddingProperty is the name a read gives a node's embedding state:
// the has_embedding flag of its embedding metadata, else whether it has chunk
// vectors.
const hasEmbeddingProperty = "has_embedding"

// projectionHydration is the embedding state a read of the projection include
// settles when it was asked for hydration. has_embedding is no stored
// property: it is read from the node's embedding metadata (Node.EmbedMeta,
// Node.HasChunkEmbeddings), which lives with the embedding records, outside
// the body. A read that would decode the body alone therefore settles that
// metadata when its projection names has_embedding. Every projected read
// asks this one rule.
func projectionHydration(hydration nodeItemHydration, include map[string]struct{}) nodeItemHydration {
	if _, named := include[hasEmbeddingProperty]; named && hydration == nodeItemBodyOnly {
		return nodeItemQueryMetadata
	}
	return hydration
}
