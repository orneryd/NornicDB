package storage

import (
	"fmt"

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
	if b.log != nil {
		b.log.Info("starting v1→v2 storage upgrade",
			"batch_size", migrationV1ToV2BatchSize,
		)
	}

	if err := b.migrateV1ToV2Nodes(); err != nil {
		return fmt.Errorf("rewriting nodes: %w", err)
	}
	if err := b.migrateV1ToV2Edges(); err != nil {
		return fmt.Errorf("rewriting edges: %w", err)
	}

	if err := b.writeSchemaVersion(storageVersionPropKeyDictV2); err != nil {
		return fmt.Errorf("writing schema version: %w", err)
	}

	if b.log != nil {
		b.log.Info("v1→v2 storage upgrade complete")
	}
	return nil
}

// migrateV1ToV2Nodes walks prefixNode in batches. Each batch opens its
// own *badger.Txn so it commits atomically and the property-key
// dictionary's per-txn counter flush runs at the right time.
func (b *BadgerEngine) migrateV1ToV2Nodes() error {
	scanned := 0
	converted := 0
	skipped := 0

	for {
		batch, err := b.collectBatch(prefixNode, migrationV1ToV2BatchSize, nodeFormatTokenizedV1)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			break
		}
		err = b.withUpdate(func(txn *badger.Txn) error {
			for _, item := range batch {
				scanned++
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
				converted++
			}
			if err := b.propKeyDict.flushTxnCounters(txn); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		if b.log != nil {
			b.log.Info("v1→v2 node batch", "scanned", scanned, "converted", converted, "skipped", skipped)
		}
		if len(batch) < migrationV1ToV2BatchSize {
			break
		}
	}
	return nil
}

func (b *BadgerEngine) migrateV1ToV2Edges() error {
	scanned := 0
	converted := 0
	skipped := 0

	for {
		batch, err := b.collectBatch(prefixEdge, migrationV1ToV2BatchSize, edgeFormatCompactV2)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			break
		}
		err = b.withUpdate(func(txn *badger.Txn) error {
			for _, item := range batch {
				scanned++
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
				converted++
			}
			if err := b.propKeyDict.flushTxnCounters(txn); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		if b.log != nil {
			b.log.Info("v1→v2 edge batch", "scanned", scanned, "converted", converted, "skipped", skipped)
		}
		if len(batch) < migrationV1ToV2BatchSize {
			break
		}
	}
	return nil
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
