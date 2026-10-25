package multidb

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type managedCatalogDiskAudit struct {
	forward       map[string]uint64
	reverse       map[uint64]string
	cachedForward map[string]uint64
	cachedReverse map[uint64]string
	body          []byte
}

func auditManagedCatalogDisk(t *testing.T, dataDir string, base *storage.BadgerEngine) managedCatalogDiskAudit {
	t.Helper()
	db, err := fixtureowner.OwnResult[*badger.DB](t, badger.ErrDBClosed)(badger.Open(badger.DefaultOptions(dataDir).WithReadOnly(true).WithLogger(nil)))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	audit := managedCatalogDiskAudit{forward: map[string]uint64{}, reverse: map[uint64]string{}, cachedForward: map[string]uint64{}, cachedReverse: map[uint64]string{}}
	// The engine is closed, so its retained dictionary cannot be written.
	// Reflection reads primitive map entries without unsafe or a product seam.
	dict := reflect.ValueOf(base).Elem().FieldByName("propKeyDict").Elem()
	forward := dict.FieldByName("forward").MapIndex(reflect.ValueOf("system"))
	if forward.IsValid() {
		for it := forward.MapRange(); it.Next(); {
			audit.cachedForward[it.Key().String()] = it.Value().Uint()
		}
	}
	reverse := dict.FieldByName("reverse").MapIndex(reflect.ValueOf("system"))
	if reverse.IsValid() {
		for it := reverse.MapRange(); it.Next(); {
			audit.cachedReverse[it.Key().Uint()] = it.Value().String()
		}
	}
	err = db.View(func(tx *badger.Txn) error {
		// Documented disk layout: 0x20, length-prefixed namespace/name -> ID.
		prefix := binary.AppendUvarint([]byte{0x20}, uint64(len("system")))
		prefix = append(prefix, "system"...)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := tx.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)[len(prefix):]
			length, used := binary.Uvarint(key)
			if used <= 0 || length != uint64(len(key)-used) {
				return fmt.Errorf("invalid property-forward key")
			}
			name := string(key[used:])
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			id, used := binary.Uvarint(value)
			if used <= 0 || used != len(value) {
				return fmt.Errorf("invalid property-forward ID")
			}
			audit.forward[name] = id
		}
		reversePrefix := append([]byte(nil), prefix...)
		reversePrefix[0] = 0x21
		reverseOpts := badger.DefaultIteratorOptions
		reverseOpts.Prefix = reversePrefix
		ri := tx.NewIterator(reverseOpts)
		defer ri.Close()
		for ri.Seek(reversePrefix); ri.ValidForPrefix(reversePrefix); ri.Next() {
			item := ri.Item()
			key := item.KeyCopy(nil)[len(reversePrefix):]
			id, used := binary.Uvarint(key)
			if used <= 0 || used != len(key) {
				return fmt.Errorf("invalid property-reverse ID")
			}
			name, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			audit.reverse[id] = string(name)
		}
		// Node key prefix 0x01; the body remains opaque in this diagnostic.
		item, err := tx.Get(append([]byte{0x01}, []byte("system:"+metadataNodeID)...))
		if err != nil {
			return err
		}
		audit.body, err = item.ValueCopy(nil)
		return err
	})
	require.NoError(t, err)
	return audit
}

// Test-only follow-up to the #1406 post-Close dictionary audit. This smaller
// managed WAL-only control does not stand in for ByteCapacitySteps.
func TestManagedWALRebuildCacheBindingsControl(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, writeBehind := range []bool{false, true} {
		for _, implicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("writeBehind=%v/implicit=%v", writeBehind, implicit), func(t *testing.T) {
				walDir := t.TempDir()
				for generation := 0; generation < 3; generation++ {
					dataDir := t.TempDir()
					base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
						DataDir: dataDir, WriteBehind: writeBehind, WriteBehindInterval: time.Hour,
					}))
					require.NoError(t, err)
					wal, err := storage.NewWAL(walDir, storage.DefaultWALConfig())
					require.NoError(t, err)
					entries, err := storage.ReadWALEntriesFromDir(walDir)
					require.NoError(t, err)
					result := storage.ReplayWALEntries(base, entries)
					require.Zero(t, result.Failed, "%s errors=%+v", result.Summary(), result.Errors)
					manager, err := NewDatabaseManager(storage.NewWALEngine(base, wal), nil)
					if err != nil {
						_ = wal.Close()
						_ = base.Close()
						audit := auditManagedCatalogDisk(t, dataDir, base)
						assertManagedCacheInverse(t, audit, generation)
						t.Fatalf("managed open after WAL-only rebuild generation=%d: %v", generation, err)
					}
					if generation == 0 {
						require.NoError(t, manager.CreateDatabase("binding_model"))
					}
					view, err := manager.GetStorage("binding_model")
					require.NoError(t, err)
					for prior := 0; prior < generation; prior++ {
						node, err := view.GetNode(storage.NodeID(fmt.Sprintf("n%d", prior)))
						require.NoError(t, err)
						require.Equal(t, fmt.Sprintf("value-%d", prior), node.Properties["v"])
					}
					if generation < 2 {
						// A synchronous metadata write warms the namespace on develop;
						// the separately owned cold-reopen deadlock is outside this control.
						require.NoError(t, manager.SetDatabaseLimits("binding_model", &Limits{}))
						node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", generation)), Properties: map[string]any{"v": fmt.Sprintf("value-%d", generation)}}
						_, err = view.CreateNode(node)
						require.NoError(t, err)
						if implicit {
							// Re-encode the actual catalog body through the buffered writer.
							tx, err := base.BeginTransaction()
							require.NoError(t, err)
							require.NoError(t, tx.SetNamespace("system"))
							require.NoError(t, tx.SetImplicit(true))
							catalog, err := tx.GetNode(storage.NodeID("system:" + metadataNodeID))
							require.NoError(t, err)
							require.NoError(t, tx.UpdateNode(catalog))
							require.NoError(t, tx.Commit())
						}
						require.NoError(t, base.FlushWriteBehind())
					}
					require.NoError(t, manager.Close())
					audit := auditManagedCatalogDisk(t, dataDir, base)
					assertManagedCacheInverse(t, audit, generation)
					if generation == 2 && writeBehind && implicit {
						buffered := 0
						for _, entry := range entries {
							if entry.Operation == storage.OpBufferedCommit {
								buffered++
							}
						}
						require.GreaterOrEqual(t, buffered, 2)
					}
				}
			})
		}
	}
}

func assertManagedCacheInverse(t *testing.T, audit managedCatalogDiskAudit, generation int) {
	t.Helper()
	for name, id := range audit.cachedForward {
		require.Equal(t, name, audit.cachedReverse[id], "cached forward/reverse generation=%d id=%d", generation, id)
	}
	for id, name := range audit.cachedReverse {
		require.Equal(t, id, audit.cachedForward[name], "cached reverse/forward generation=%d name=%s", generation, name)
	}
	require.Equal(t, audit.forward, audit.cachedForward, "persisted/cached forward generation=%d", generation)
	require.Equal(t, audit.reverse, audit.cachedReverse, "persisted/cached reverse generation=%d", generation)
	t.Logf("MANAGED_WAL_CACHE_INVERSE generation=%d rebuilt=%v names=%d", generation, generation > 0, len(audit.cachedForward))
}
