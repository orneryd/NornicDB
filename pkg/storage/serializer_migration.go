package storage

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// SerializerMigrationOptions controls the in-place gob → msgpack
// rewrite. There is no source/target choice: the engine only emits
// msgpack, and the only thing this tool does is upgrade legacy gob
// bodies in an existing data directory so they can be read by current
// code without going through the legacy decode arm forever.
type SerializerMigrationOptions struct {
	BatchSize int
	DryRun    bool
}

// SerializerMigrationStats reports conversion results for the in-place
// gob → msgpack rewrite.
type SerializerMigrationStats struct {
	DataDir             string
	HasLegacyData       bool
	NodesConverted      int
	EdgesConverted      int
	EmbeddingsConverted int
	SkippedExisting     int
	TotalScanned        int
}

// MigrateBadgerToMsgpack rewrites any legacy gob-encoded node, edge, or
// embedding bodies in the given data directory as msgpack. The database
// must be offline (no running server). New writes have always been
// msgpack since this engine version, so on a fresh database this is a
// no-op.
func MigrateBadgerToMsgpack(dataDir string, opts SerializerMigrationOptions) (SerializerMigrationStats, error) {
	db, err := badger.Open(badger.DefaultOptions(dataDir).WithLogger(nil))
	if err != nil {
		return SerializerMigrationStats{DataDir: dataDir}, fmt.Errorf("open badger: %w", err)
	}
	defer db.Close()

	return MigrateBadgerToMsgpackWithDB(db, dataDir, opts)
}

// MigrateBadgerToMsgpackWithDB is MigrateBadgerToMsgpack against an
// already-open *badger.DB. Used by tests and offline tooling.
func MigrateBadgerToMsgpackWithDB(db *badger.DB, dataDir string, opts SerializerMigrationOptions) (SerializerMigrationStats, error) {
	stats := SerializerMigrationStats{DataDir: dataDir}

	if db == nil {
		return stats, fmt.Errorf("nil badger db")
	}

	if opts.BatchSize <= 0 {
		opts.BatchSize = 1000
	}

	source, hasData, err := detectStoredSerializer(db)
	if err != nil {
		return stats, err
	}
	stats.HasLegacyData = hasData && source == detectedGob
	if !hasData {
		return stats, nil
	}

	converted, skipped, scanned, err := migratePrefixToMsgpack(db, prefixNode, "node", func(data []byte) (any, error) {
		return decodeNodeV1(data)
	}, func(v any) ([]byte, error) {
		node := v.(*Node)
		return encodeValue(node)
	}, opts)
	if err != nil {
		return stats, err
	}
	stats.NodesConverted += converted
	stats.SkippedExisting += skipped
	stats.TotalScanned += scanned

	// Edges may be in legacy gob/msgpack form OR in the compact-V1 codec
	// (which is a separate framing not produced by encodeValue). The
	// compact codec is already non-gob and needs no rewrite, so we route
	// it through a decoder that re-emits the same compact body — the
	// migrate loop's "already in target format" check will then skip it.
	migrationDict := newIDDictionary()
	if err := migrationDict.loadFromBadger(db); err != nil {
		return stats, fmt.Errorf("loading id dictionary for migration: %w", err)
	}
	edgeDecoder := func(data []byte) (any, error) {
		if len(data) >= 1 && data[0] == edgeFormatCompactV1 {
			edge, startNum, endNum, derr := decodeEdgeCompactV1(data)
			if derr != nil {
				return nil, derr
			}
			if startID, ok := migrationDict.lookupNodeIDByNum(startNum); ok {
				edge.StartNode = startID
			}
			if endID, ok := migrationDict.lookupNodeIDByNum(endNum); ok {
				edge.EndNode = endID
			}
			return edge, nil
		}
		return decodeEdge(data)
	}
	converted, skipped, scanned, err = migratePrefixToMsgpack(db, prefixEdge, "edge", edgeDecoder, func(v any) ([]byte, error) {
		edge := v.(*Edge)
		return encodeValue(edge)
	}, opts)
	if err != nil {
		return stats, err
	}
	stats.EdgesConverted += converted
	stats.SkippedExisting += skipped
	stats.TotalScanned += scanned

	converted, skipped, scanned, err = migratePrefixToMsgpack(db, prefixEmbedding, "embedding", func(data []byte) (any, error) {
		return decodeEmbedding(data)
	}, func(v any) ([]byte, error) {
		emb := v.([]float32)
		return encodeValue(emb)
	}, opts)
	if err != nil {
		return stats, err
	}
	stats.EmbeddingsConverted += converted
	stats.SkippedExisting += skipped
	stats.TotalScanned += scanned

	return stats, nil
}

func migratePrefixToMsgpack(db *badger.DB, prefix byte, kind string, decode func([]byte) (any, error), encode func(any) ([]byte, error), opts SerializerMigrationOptions) (int, int, int, error) {
	converted := 0
	skipped := 0
	scanned := 0

	var batch *badger.WriteBatch
	var batchCount int

	if !opts.DryRun {
		batch = db.NewWriteBatch()
		defer batch.Cancel()
	}

	flushBatch := func() error {
		if opts.DryRun || batchCount == 0 {
			return nil
		}
		if err := batch.Flush(); err != nil {
			return err
		}
		batch.Cancel()
		batch = db.NewWriteBatch()
		batchCount = 0
		return nil
	}

	err := db.View(func(txn *badger.Txn) error {
		iterOpts := badger.DefaultIteratorOptions
		iterOpts.PrefetchValues = true
		iterOpts.Prefix = []byte{prefix}
		it := txn.NewIterator(iterOpts)
		defer it.Close()

		for it.Rewind(); it.ValidForPrefix(iterOpts.Prefix); it.Next() {
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(val) == 0 {
				continue
			}
			scanned++

			headerID, _, ok, err := splitSerializationHeader(val)
			if err != nil {
				return err
			}
			if ok && headerID == serializerIDMsgpack {
				skipped++
				continue
			}
			// Compact edge bodies aren't framed by encodeValue; their
			// first byte is edgeFormatCompactV1 and they have no header.
			if prefix == prefixEdge && len(val) >= 1 && val[0] == edgeFormatCompactV1 {
				skipped++
				continue
			}

			decoded, err := decode(val)
			if err != nil {
				return fmt.Errorf("decode %s value: %w", kind, err)
			}
			encoded, err := encode(decoded)
			if err != nil {
				return fmt.Errorf("encode %s value: %w", kind, err)
			}

			if opts.DryRun {
				converted++
				continue
			}

			key := item.KeyCopy(nil)
			if err := batch.Set(key, encoded); err != nil {
				return err
			}
			converted++
			batchCount++

			if batchCount >= opts.BatchSize {
				if err := flushBatch(); err != nil {
					return err
				}
			}
		}

		return nil
	})
	if err != nil {
		return converted, skipped, scanned, err
	}

	if err := flushBatch(); err != nil {
		return converted, skipped, scanned, err
	}

	return converted, skipped, scanned, nil
}
