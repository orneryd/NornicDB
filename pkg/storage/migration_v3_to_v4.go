package storage

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// migrateV3ToV4 runs only under the startup runner's existing maintenance lease.
// It does not acquire another lifecycle or call normal entity write methods.
// The exact catalog body and schema marker commit in one native transaction.
func (b *BadgerEngine) migrateV3ToV4(ctx context.Context, replayCurrent bool) error {
	if ctx == nil {
		return fmt.Errorf("catalog upgrade requires a context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	migrated, changed := false, false
	err := b.db.update(ctx, func(txn *badger.Txn) error {
		version, err := accessSchemaVersionInTxn(txn, loadSchemaVersion, 0)
		if err != nil {
			return err
		}
		if version == storageVersionCatalogV4 && !replayCurrent {
			return nil
		}
		if version != storageVersionEdgeAdjacencyV3 && version != storageVersionCatalogV4 {
			return fmt.Errorf("catalog V4 arm requires V3 storage, got %d", version)
		}
		plan := b.catalogUpgradeV4
		if plan == nil {
			if !isStorageEmptyInTxn(txn) {
				return fmt.Errorf("catalog V4 upgrade of existing data requires an explicit transform plan")
			}
		} else {
			if plan.Transform == nil {
				return fmt.Errorf("catalog V4 upgrade requires an explicit transform plan")
			}
			if _, _, qualified := ParseDatabasePrefix(string(plan.NodeID)); plan.NodeID == "" || !qualified {
				return fmt.Errorf("catalog V4 upgrade requires a qualified catalog node ID")
			}
			item, err := txn.Get(nodeKey(plan.NodeID))
			if err != badger.ErrKeyNotFound {
				if err != nil {
					return err
				}
				raw, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				node, err := b.decodeNode(namespaceForNodeID(plan.NodeID), raw)
				if err != nil {
					return fmt.Errorf("decode catalog V4 record: %w", err)
				}
				data, ok := node.Properties["data"].(string)
				if !ok {
					return fmt.Errorf("catalog V4 record requires a data string")
				}
				replacement, replace, err := plan.Transform(ctx, data)
				if err != nil {
					return err
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				if replace {
					node.Properties["data"] = replacement
					encoded, separate, err := b.encodeNodeInTxn(txn, namespaceForNodeID(plan.NodeID), node)
					if err != nil {
						return fmt.Errorf("encode catalog V4 record: %w", err)
					}
					if separate && (len(node.ChunkEmbeddings) > 0 || len(node.NamedEmbeddings) > 0) {
						return fmt.Errorf("unsupported catalog V4 image: reencoding would separate embedded vectors")
					}
					if err := txn.Set(nodeKey(plan.NodeID), encoded); err != nil {
						return err
					}
					changed = true
				}
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if version != storageVersionCatalogV4 {
			if _, err := accessSchemaVersionInTxn(txn, storeSchemaVersion, storageVersionCatalogV4); err != nil {
				return err
			}
		}
		if _, err := txn.Get(v4WALRecoveryKey); err == nil {
			if err := txn.Delete(v4WALRecoveryKey); err != nil {
				return err
			}
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		migrated = version != storageVersionCatalogV4 || changed
		return nil
	}, nil)
	if err != nil {
		return err
	}
	if changed {
		b.cacheDeleteNode(b.catalogUpgradeV4.NodeID, nodeCacheAllForms)
		b.invalidateManagedEntityUsage()
	}
	if migrated {
		b.storageVersion = storageVersionCatalogV4
		b.migrationDidRun = true
	}
	return nil
}
