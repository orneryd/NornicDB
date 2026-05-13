package storage

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// isStorageEmpty returns true when the data directory has no node or
// edge bodies. Used by RunOnStartMigrations to skip the upgrade gate
// on a freshly initialized store.
func (b *BadgerEngine) isStorageEmpty() (bool, error) {
	empty := true
	err := b.db.View(func(txn *badger.Txn) error {
		for _, prefix := range [][]byte{{prefixNode}, {prefixEdge}} {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			it.Rewind()
			if it.ValidForPrefix(prefix) {
				empty = false
			}
			it.Close()
			if !empty {
				return nil
			}
		}
		return nil
	})
	return empty, err
}

// ErrStorageUpgradeRequired is returned by RunOnStartMigrations when
// the on-disk schema version is older than the binary's current
// version and the operator has not authorized an upgrade. Callers
// should surface this directly to the operator with the recommended
// remediation: back up the data directory, then restart with the
// --upgrade-storage flag.
type ErrStorageUpgradeRequired struct {
	OnDisk, Current int
}

func (e *ErrStorageUpgradeRequired) Error() string {
	return fmt.Sprintf(
		"storage upgrade required: on-disk version %d is older than binary version %d; "+
			"back up the data directory and restart with --upgrade-storage to authorize the one-way upgrade",
		e.OnDisk, e.Current,
	)
}

// RunOnStartMigrations advances the on-disk schema version to
// storageVersionCurrent if (and only if) the operator has authorized
// the upgrade via allowUpgrade. Without authorization, an out-of-date
// store causes RunOnStartMigrations to return ErrStorageUpgradeRequired
// and the engine refuses to open.
//
// Migrations run in order based on the on-disk version, each preserving
// the prior step's invariants:
//
//	V0 → V1: extracts legacy access state into AccessMeta records.
//	V1 → V2: eager rewrite of every node and edge body to the tokenized
//	         property-key codec; bumps the version after a clean pass.
//
// Sets engine.storageVersion to the post-migration version so the
// encode path can deterministically pick codecs from it.
func (b *BadgerEngine) RunOnStartMigrations(allowUpgrade bool) error {
	currentVersion, err := b.readSchemaVersion()
	if err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}

	// Fresh data directories have no node/edge bodies and no version
	// key. Skip the upgrade gate for them — there is nothing to lose
	// and no operator decision to gate behind. Detect emptiness by
	// looking for any node body at all.
	if currentVersion < storageVersionCurrent && !allowUpgrade {
		empty, emptyErr := b.isStorageEmpty()
		if emptyErr != nil {
			return fmt.Errorf("checking storage emptiness: %w", emptyErr)
		}
		if !empty {
			return &ErrStorageUpgradeRequired{OnDisk: currentVersion, Current: storageVersionCurrent}
		}
	}

	if currentVersion < storageVersionV1 {
		if err := b.migrateV0ToV1(); err != nil {
			return fmt.Errorf("migration v0→v1 failed: %w", err)
		}
		currentVersion = storageVersionV1
	}

	if currentVersion < storageVersionPropKeyDictV2 {
		if err := b.migrateV1ToV2(); err != nil {
			return fmt.Errorf("migration v1→v2 failed: %w", err)
		}
		currentVersion = storageVersionPropKeyDictV2
	}

	b.storageVersion = currentVersion
	return nil
}
