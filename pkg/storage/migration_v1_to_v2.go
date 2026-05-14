package storage

import (
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// migrateV1ToV2 walks every node and edge body in the data directory,
// re-encoding each through the tokenized property-key codec. The
// migration is the only place in the engine that ever reads V1 bodies
// — once it completes successfully, the schema version is bumped to
// V2 and the hot path emits V2 exclusively.
//
// Crash safety: each batch is committed atomically. If the migration
// crashes mid-pass, restarting picks up where it left off because the
// per-record skip check looks at the body's leading byte. The version
// bump is the LAST step, so a partially-migrated store stays at V1
// until the entire walk succeeds.
//
// Resumability: nodes that already start with nodeFormatTokenizedV1
// and edges that already start with edgeFormatCompactV2 are skipped.
const migrationV1ToV2BatchSize = 500

func (b *BadgerEngine) migrateV1ToV2() error {
	overallStart := time.Now()
	if b.log != nil {
		b.log.Info("starting v1→v2 storage upgrade",
			"batch_size", migrationV1ToV2BatchSize,
		)
	}

	nodesStart := time.Now()
	nodeStats, err := b.migrateV1ToV2Nodes()
	if err != nil {
		return fmt.Errorf("rewriting nodes: %w", err)
	}
	if b.log != nil {
		b.log.Info("v1→v2 nodes pass complete",
			"scanned", nodeStats.scanned,
			"converted", nodeStats.converted,
			"duration_ms", time.Since(nodesStart).Milliseconds(),
		)
	}

	edgesStart := time.Now()
	edgeStats, err := b.migrateV1ToV2Edges()
	if err != nil {
		return fmt.Errorf("rewriting edges: %w", err)
	}
	if b.log != nil {
		b.log.Info("v1→v2 edges pass complete",
			"scanned", edgeStats.scanned,
			"converted", edgeStats.converted,
			"duration_ms", time.Since(edgesStart).Milliseconds(),
		)
	}

	// v1.0.x stores wrote secondary indexes (label, outgoing, incoming,
	// edge-type) keyed by string IDs and 0x00 separators. The numID
	// rework ahead of v1.1.0 changed those keys to fixed-width 8-byte
	// numeric IDs from idDict. The migration's body rewrite seeds the
	// dict, but the on-disk legacy index entries are still string-keyed
	// — modern reads scan the numID-shaped prefix and find nothing,
	// which is what makes a freshly-migrated graph look "edgeless" in
	// the explorer.
	//
	// Rebuild MUST run before writeSchemaVersion so a crash mid-rebuild
	// leaves the store at v1; the migration restarts from scratch on
	// the next open and re-rebuilds.
	idxStart := time.Now()
	idxStats, err := b.rebuildSecondaryIndexesForV2()
	if err != nil {
		return fmt.Errorf("rebuilding secondary indexes: %w", err)
	}
	if b.log != nil {
		b.log.Info("v1→v2 secondary index rebuild complete",
			"label_entries", idxStats.labels,
			"outgoing_entries", idxStats.outgoing,
			"incoming_entries", idxStats.incoming,
			"edge_type_entries", idxStats.edgeTypes,
			"duration_ms", time.Since(idxStart).Milliseconds(),
		)
	}

	if err := b.writeSchemaVersion(storageVersionPropKeyDictV2); err != nil {
		return fmt.Errorf("writing schema version: %w", err)
	}

	// NOTE: post-migration LSM compaction + vlog GC are NOT run here.
	// At this point most of the v2 bodies still sit in memtables and
	// haven't been flushed to SSTs, so Flatten would only collapse the
	// untouched v1 SSTs against themselves and reclaim very little.
	// The engine open path closes + reopens the DB after migrations,
	// then runs the compaction — that ordering forces every memtable
	// out to disk first so Flatten actually has v2 SSTs to merge with
	// the v1 ones.

	if b.log != nil {
		b.log.Info("v1→v2 storage upgrade complete",
			"nodes_converted", nodeStats.converted,
			"edges_converted", edgeStats.converted,
			"duration_ms", time.Since(overallStart).Milliseconds(),
		)
	}
	return nil
}

// v1ToV2PassStats records the totals of one migration sweep so the
// caller can roll them into the end-of-migration summary log line.
type v1ToV2PassStats struct {
	scanned   int
	converted int
}

// v1ToV2IndexStats counts secondary-index entries written by the
// post-body rebuild step, broken down by index type. Surfaced to the
// migration's summary log so operators can sanity-check that the
// counts match the cardinality of their data.
type v1ToV2IndexStats struct {
	labels    int
	outgoing  int
	incoming  int
	edgeTypes int
}

// rebuildSecondaryIndexesForV2 wipes the legacy string-keyed
// label/outgoing/incoming/edge-type index prefixes and re-emits them
// in the numID-keyed shape used by every read path on the modern
// engine. Runs after v1→v2 body rewrites so the idDict is fully
// populated for every node and edge.
//
// DropPrefix is durable: the prefixes stay empty if a crash hits mid-
// rebuild, but because writeSchemaVersion has not yet been called the
// store is still at v1, so the next open re-runs the entire arm
// including this step from scratch. The intermediate state is
// "indexes wiped, bodies already at v2"; readers fall through the
// edge-between repair self-heal path until the rebuild completes.
func (b *BadgerEngine) rebuildSecondaryIndexesForV2() (v1ToV2IndexStats, error) {
	stats := v1ToV2IndexStats{}

	for _, p := range []byte{
		prefixLabelIndex,
		prefixOutgoingIndex,
		prefixIncomingIndex,
		prefixEdgeTypeIndex,
	} {
		if err := b.db.DropPrefix([]byte{p}); err != nil {
			return stats, fmt.Errorf("drop legacy prefix 0x%02x: %w", p, err)
		}
	}

	if err := b.rebuildLabelIndexForV2(&stats); err != nil {
		return stats, err
	}
	if err := b.rebuildEdgeIndexesForV2(&stats); err != nil {
		return stats, err
	}
	return stats, nil
}

// rebuildLabelIndexForV2 walks every node body and emits one
// labelIndexKey per (node, label) pair. Uses cursor-based pagination
// (not the body-format-byte skip used by the body-rewrite pass) since
// nodes are already at v2 and we'd loop forever otherwise.
func (b *BadgerEngine) rebuildLabelIndexForV2(stats *v1ToV2IndexStats) error {
	const progressEvery = 25_000
	const batchSize = migrationV1ToV2BatchSize
	lastLogged := 0
	var cursor []byte

	for {
		var batch []migrationItem
		err := b.db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			start := cursor
			if len(start) == 0 {
				start = []byte{prefixNode}
			}
			batch = batch[:0]
			for it.Seek(start); it.ValidForPrefix([]byte{prefixNode}); it.Next() {
				if len(batch) >= batchSize {
					return nil
				}
				item := it.Item()
				val, vErr := item.ValueCopy(nil)
				if vErr != nil {
					return vErr
				}
				if len(val) == 0 {
					continue
				}
				batch = append(batch, migrationItem{key: item.KeyCopy(nil), value: val})
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("scan nodes for index rebuild: %w", err)
		}
		if len(batch) == 0 {
			return nil
		}
		err = b.withUpdate(func(txn *badger.Txn) error {
			for _, item := range batch {
				nodeID := NodeID(string(item.key[1:]))
				namespace, _, ok := ParseDatabasePrefix(string(nodeID))
				if !ok {
					return fmt.Errorf("v1→v2 index rebuild: node %q lacks namespace prefix", nodeID)
				}
				node, err := b.decodeNode(namespace, item.value)
				if err != nil {
					return fmt.Errorf("decode v2 node %q: %w", nodeID, err)
				}
				for _, label := range node.Labels {
					key, err := b.labelIndexKeyString(txn, label, nodeID)
					if err != nil {
						return fmt.Errorf("label key for %q/%q: %w", nodeID, label, err)
					}
					if err := txn.Set(key, []byte{}); err != nil {
						return fmt.Errorf("write label index for %q/%q: %w", nodeID, label, err)
					}
					stats.labels++
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if b.log != nil && stats.labels-lastLogged >= progressEvery {
			b.log.Info("v1→v2 index rebuild: labels progress",
				"label_entries", stats.labels,
			)
			lastLogged = stats.labels
		}
		// Advance cursor past the last key we just processed.
		last := batch[len(batch)-1].key
		cursor = make([]byte, len(last)+1)
		copy(cursor, last)
		cursor[len(last)] = 0x00
		if len(batch) < batchSize {
			return nil
		}
	}
}

// rebuildEdgeIndexesForV2 walks every edge body and emits outgoing,
// incoming, and edge-type entries. Cursor-based for the same reason
// as the label pass.
func (b *BadgerEngine) rebuildEdgeIndexesForV2(stats *v1ToV2IndexStats) error {
	const progressEvery = 25_000
	const batchSize = migrationV1ToV2BatchSize
	lastLogged := 0
	var cursor []byte

	for {
		var batch []migrationItem
		err := b.db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			start := cursor
			if len(start) == 0 {
				start = []byte{prefixEdge}
			}
			batch = batch[:0]
			for it.Seek(start); it.ValidForPrefix([]byte{prefixEdge}); it.Next() {
				if len(batch) >= batchSize {
					return nil
				}
				item := it.Item()
				val, vErr := item.ValueCopy(nil)
				if vErr != nil {
					return vErr
				}
				if len(val) == 0 {
					// Empty edge bodies aren't decodable; the body-rewrite
					// pass also skips them. Leave them on disk; index
					// rebuild has nothing to emit for them.
					continue
				}
				batch = append(batch, migrationItem{key: item.KeyCopy(nil), value: val})
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("scan edges for index rebuild: %w", err)
		}
		if len(batch) == 0 {
			return nil
		}
		err = b.withUpdate(func(txn *badger.Txn) error {
			for _, item := range batch {
				edgeID := EdgeID(string(item.key[1:]))
				edge, err := b.decodeEdgeBodyByID(item.value, edgeID)
				if err != nil {
					return fmt.Errorf("decode v2 edge %q: %w", edgeID, err)
				}
				outKey, err := b.outgoingIndexKeyString(txn, edge.StartNode, edgeID)
				if err != nil {
					return fmt.Errorf("outgoing key for %q: %w", edgeID, err)
				}
				if err := txn.Set(outKey, []byte{}); err != nil {
					return fmt.Errorf("write outgoing index for %q: %w", edgeID, err)
				}
				stats.outgoing++

				inKey, err := b.incomingIndexKeyString(txn, edge.EndNode, edgeID)
				if err != nil {
					return fmt.Errorf("incoming key for %q: %w", edgeID, err)
				}
				if err := txn.Set(inKey, []byte{}); err != nil {
					return fmt.Errorf("write incoming index for %q: %w", edgeID, err)
				}
				stats.incoming++

				if edge.Type != "" {
					typeKey, err := b.edgeTypeIndexKeyString(txn, edge.Type, edgeID)
					if err != nil {
						return fmt.Errorf("edge-type key for %q: %w", edgeID, err)
					}
					if err := txn.Set(typeKey, []byte{}); err != nil {
						return fmt.Errorf("write edge-type index for %q: %w", edgeID, err)
					}
					stats.edgeTypes++
				}
			}
			if err := b.idDict.flushTxnCounters(txn); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		if b.log != nil && stats.outgoing-lastLogged >= progressEvery {
			b.log.Info("v1→v2 index rebuild: edges progress",
				"outgoing_entries", stats.outgoing,
				"incoming_entries", stats.incoming,
				"edge_type_entries", stats.edgeTypes,
			)
			lastLogged = stats.outgoing
		}
		last := batch[len(batch)-1].key
		cursor = make([]byte, len(last)+1)
		copy(cursor, last)
		cursor[len(last)] = 0x00
		if len(batch) < batchSize {
			return nil
		}
	}
}

// compactAfterMigration synchronously compacts the LSM tree and runs
// value-log GC until it stops finding rewriteable files. Failures are
// logged but not returned: the migration's data correctness is already
// committed by writeSchemaVersion, and a failed compaction just leaves
// the store with stale bytes that the background GC will eventually
// clean up.
func (b *BadgerEngine) compactAfterMigration() {
	if b.db == nil {
		return
	}
	workers := runtime.NumCPU()
	if workers < 1 {
		workers = 1
	}
	if b.log != nil {
		b.log.Info("post-migration compaction starting", "lsm_workers", workers)
	}
	flattenStart := time.Now()
	if err := b.db.Flatten(workers); err != nil {
		if b.log != nil {
			b.log.Warn("post-migration LSM flatten failed", "err", err)
		}
	} else if b.log != nil {
		b.log.Info("post-migration LSM flatten done", "duration_ms", time.Since(flattenStart).Milliseconds())
	}

	gcStart := time.Now()
	rewrites := 0
	for {
		err := b.db.RunValueLogGC(0.5)
		if err == nil {
			rewrites++
			continue
		}
		if errors.Is(err, badger.ErrNoRewrite) {
			break
		}
		if b.log != nil {
			b.log.Warn("post-migration value-log GC stopped on error", "err", err, "rewrites", rewrites)
		}
		break
	}
	if b.log != nil {
		b.log.Info("post-migration value-log GC done",
			"rewrites", rewrites,
			"duration_ms", time.Since(gcStart).Milliseconds(),
		)
	}
}

// migrateV1ToV2Nodes walks prefixNode in batches. Each batch opens its
// own *badger.Txn so it commits atomically and the property-key
// dictionary's per-txn counter flush runs at the right time. Returns
// pass stats so the caller can roll them into a single end-of-pass log
// line.
func (b *BadgerEngine) migrateV1ToV2Nodes() (v1ToV2PassStats, error) {
	stats := v1ToV2PassStats{}
	const progressEvery = 25_000
	lastLogged := 0

	for {
		batch, err := b.collectBatch(prefixNode, migrationV1ToV2BatchSize, nodeFormatTokenizedV1)
		if err != nil {
			return stats, err
		}
		if len(batch) == 0 {
			break
		}
		err = b.withUpdate(func(txn *badger.Txn) error {
			for _, item := range batch {
				stats.scanned++
				nodeID := NodeID(string(item.key[1:]))
				namespace, _, ok := ParseDatabasePrefix(string(nodeID))
				if !ok {
					return fmt.Errorf("v1→v2: node id %q lacks namespace prefix", nodeID)
				}

				node, err := decodeNodeV1(item.value)
				if err != nil {
					return fmt.Errorf("decoding v1 node %q: %w", nodeID, err)
				}
				node.ID = nodeID

				newBody, _, err := b.encodeNodeInTxn(txn, namespace, node)
				if err != nil {
					return fmt.Errorf("re-encoding node %q: %w", nodeID, err)
				}
				if err := txn.Set(append([]byte{prefixNode}, []byte(nodeID)...), newBody); err != nil {
					return fmt.Errorf("writing v2 node %q: %w", nodeID, err)
				}
				stats.converted++
			}
			if err := b.propKeyDict.flushTxnCounters(txn); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return stats, err
		}
		if b.log != nil && stats.converted-lastLogged >= progressEvery {
			b.log.Info("v1→v2 nodes progress",
				"scanned", stats.scanned,
				"converted", stats.converted,
			)
			lastLogged = stats.converted
		}
		if len(batch) < migrationV1ToV2BatchSize {
			break
		}
	}
	return stats, nil
}

func (b *BadgerEngine) migrateV1ToV2Edges() (v1ToV2PassStats, error) {
	stats := v1ToV2PassStats{}
	const progressEvery = 25_000
	lastLogged := 0

	for {
		batch, err := b.collectBatch(prefixEdge, migrationV1ToV2BatchSize, edgeFormatCompactV2)
		if err != nil {
			return stats, err
		}
		if len(batch) == 0 {
			break
		}
		err = b.withUpdate(func(txn *badger.Txn) error {
			for _, item := range batch {
				stats.scanned++
				edgeID := EdgeID(string(item.key[1:]))

				edge, startNum, endNum, err := decodeEdgeAnyV1(b, txn, item.value, edgeID)
				if err != nil {
					return fmt.Errorf("decoding v1 edge %q: %w", edgeID, err)
				}
				edge.ID = edgeID

				namespace := edgeNamespace(edge)
				if namespace == "" {
					return fmt.Errorf("v1→v2: edge %q endpoints lack namespace prefix", edgeID)
				}

				newBody, err := b.encodeEdgeCompactV2(txn, namespace, edge, startNum, endNum)
				if err != nil {
					return fmt.Errorf("re-encoding edge %q: %w", edgeID, err)
				}
				if err := txn.Set(append([]byte{prefixEdge}, []byte(edgeID)...), newBody); err != nil {
					return fmt.Errorf("writing v2 edge %q: %w", edgeID, err)
				}
				stats.converted++
			}
			if err := b.propKeyDict.flushTxnCounters(txn); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return stats, err
		}
		if b.log != nil && stats.converted-lastLogged >= progressEvery {
			b.log.Info("v1→v2 edges progress",
				"scanned", stats.scanned,
				"converted", stats.converted,
			)
			lastLogged = stats.converted
		}
		if len(batch) < migrationV1ToV2BatchSize {
			break
		}
	}
	return stats, nil
}

// migrationItem holds a key+value pair pulled out of the read txn so
// we can mutate them under a separate update txn. Copies are required
// because Badger reuses the underlying slice memory after Next().
type migrationItem struct {
	key   []byte
	value []byte
}

// collectBatch scans up to limit records under prefix that do NOT
// already start with skipFormatByte. Used to find work for a migration
// pass without holding a read iterator across mutating writes.
func (b *BadgerEngine) collectBatch(prefix byte, limit int, skipFormatByte byte) ([]migrationItem, error) {
	out := make([]migrationItem, 0, limit)
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{prefix}
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(opts.Prefix); it.Next() {
			if len(out) >= limit {
				break
			}
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(val) == 0 {
				continue
			}
			if val[0] == skipFormatByte {
				continue
			}
			out = append(out, migrationItem{
				key:   item.KeyCopy(nil),
				value: val,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// decodeEdgeAnyV1 reads a pre-V2 edge body in either of the two
// formats this codebase has ever produced: the compact V1 codec
// (leading byte edgeFormatCompactV1) and the legacy gob/msgpack-
// wrapped Edge struct from before the compact codec existed. Returns
// the decoded edge plus the start/end numIDs that need to be
// re-encoded under V2.
//
// For the legacy non-compact path, the body carries string IDs
// directly so we resolve numIDs from the dict (allocating if needed,
// since the compact codec depends on them).
func decodeEdgeAnyV1(b *BadgerEngine, txn *badger.Txn, data []byte, edgeID EdgeID) (*Edge, uint64, uint64, error) {
	if len(data) == 0 {
		return nil, 0, 0, fmt.Errorf("edge body empty")
	}
	if data[0] == edgeFormatCompactV1 {
		edge, startNum, endNum, err := decodeEdgeCompactV1(data)
		if err != nil {
			return nil, 0, 0, err
		}
		if startID, ok := b.idDict.lookupNodeIDByNum(startNum); ok {
			edge.StartNode = startID
		}
		if endID, ok := b.idDict.lookupNodeIDByNum(endNum); ok {
			edge.EndNode = endID
		}
		return edge, startNum, endNum, nil
	}
	// Pre-compact body — full Edge struct via the standard decoder.
	edge, err := decodeEdge(data)
	if err != nil {
		return nil, 0, 0, err
	}
	startNum, err := b.idDict.resolveOrAllocateNodeNumIDInTxn(txn, edge.StartNode)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("v1→v2: allocating start numID for %q: %w", edgeID, err)
	}
	endNum, err := b.idDict.resolveOrAllocateNodeNumIDInTxn(txn, edge.EndNode)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("v1→v2: allocating end numID for %q: %w", edgeID, err)
	}
	return edge, startNum, endNum, nil
}

// edgeNamespace recovers the per-namespace bucket the edge's properties
// belong to. Both endpoints are expected to share the same namespace
// prefix; if they disagree we fall back to the start node's, which is
// what other index code paths use.
func edgeNamespace(edge *Edge) string {
	if ns, _, ok := ParseDatabasePrefix(string(edge.StartNode)); ok {
		return ns
	}
	if ns, _, ok := ParseDatabasePrefix(string(edge.EndNode)); ok {
		return ns
	}
	return ""
}
