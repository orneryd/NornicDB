package storage

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// SerializerMigrationOptions controls migration behavior.
type SerializerMigrationOptions struct {
	BatchSize int
	DryRun    bool
}

// SerializerMigrationStats reports conversion results.
type SerializerMigrationStats struct {
	DataDir             string
	Source              StorageSerializer
	Target              StorageSerializer
	HasData             bool
	NodesConverted      int
	EdgesConverted      int
	EmbeddingsConverted int
	SkippedExisting     int
	TotalScanned        int
}

// MigrateBadgerSerializer converts stored data to the target serializer in place.
// This expects the database to be offline (no running server).
func MigrateBadgerSerializer(dataDir string, target StorageSerializer, opts SerializerMigrationOptions) (SerializerMigrationStats, error) {
	db, err := badger.Open(badger.DefaultOptions(dataDir).WithLogger(nil))
	if err != nil {
		return SerializerMigrationStats{DataDir: dataDir, Target: target}, fmt.Errorf("open badger: %w", err)
	}
	defer db.Close()

	return MigrateBadgerSerializerWithDB(db, dataDir, target, opts)
}

// MigrateBadgerSerializerWithDB converts stored data to the target serializer using an existing DB handle.
// This is primarily used for tests and offline tooling.
func MigrateBadgerSerializerWithDB(db *badger.DB, dataDir string, target StorageSerializer, opts SerializerMigrationOptions) (SerializerMigrationStats, error) {
	stats := SerializerMigrationStats{
		DataDir: dataDir,
		Target:  target,
	}

	if _, err := ParseStorageSerializer(string(target)); err != nil {
		return stats, err
	}

	if opts.BatchSize <= 0 {
		opts.BatchSize = 1000
	}

	source, hasData, err := detectStoredSerializer(db)
	if err != nil {
		return stats, err
	}
	stats.Source = source
	stats.HasData = hasData
	if !hasData {
		return stats, nil
	}

	prev := currentStorageSerializer()
	if err := SetStorageSerializer(target); err != nil {
		return stats, err
	}
	defer func() {
		_ = SetStorageSerializer(prev)
	}()

	converted, skipped, scanned, err := migratePrefix(db, prefixNode, "node", func(data []byte) (any, error) {
		return decodeNode(data)
	}, func(v any) ([]byte, error) {
		node := v.(*Node)
		return encodeValue(node)
	}, target, opts)
	if err != nil {
		return stats, err
	}
	stats.NodesConverted += converted
	stats.SkippedExisting += skipped
	stats.TotalScanned += scanned

	// Migration-time edge decoder must handle both legacy (gob/msgpack)
	// and compact formats. Compact requires the id dictionary's reverse
	// map to resolve endpoint numIDs — load it once up front.
	migrationDict := newIDDictionary()
	if err := migrationDict.loadFromBadger(db); err != nil {
		return stats, fmt.Errorf("loading id dictionary for migration: %w", err)
	}
	edgeDecoder := func(data []byte) (any, error) {
		if len(data) >= 1 && data[0] == edgeFormatCompactV1 {
			edge, startNum, endNum, derr := decodeEdgeCompact(data)
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
	converted, skipped, scanned, err = migratePrefix(db, prefixEdge, "edge", edgeDecoder, func(v any) ([]byte, error) {
		edge := v.(*Edge)
		return encodeValue(edge)
	}, target, opts)
	if err != nil {
		return stats, err
	}
	stats.EdgesConverted += converted
	stats.SkippedExisting += skipped
	stats.TotalScanned += scanned

	converted, skipped, scanned, err = migratePrefix(db, prefixEmbedding, "embedding", func(data []byte) (any, error) {
		return decodeEmbedding(data)
	}, func(v any) ([]byte, error) {
		emb := v.([]float32)
		return encodeValue(emb)
	}, target, opts)
	if err != nil {
		return stats, err
	}
	stats.EmbeddingsConverted += converted
	stats.SkippedExisting += skipped
	stats.TotalScanned += scanned

	return stats, nil
}

func migratePrefix(db *badger.DB, prefix byte, kind string, decode func([]byte) (any, error), encode func(any) ([]byte, error), target StorageSerializer, opts SerializerMigrationOptions) (int, int, int, error) {
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

			serializer, _, ok, err := splitSerializationHeader(val)
			if err != nil {
				return err
			}
			if ok && serializer == target {
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
