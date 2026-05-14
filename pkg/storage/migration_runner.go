package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

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

	if b.log != nil {
		b.log.Info("storage migration check",
			"on_disk_version", currentVersion,
			"binary_version", storageVersionCurrent,
			"upgrade_authorized", allowUpgrade,
		)
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
		if b.log != nil {
			b.log.Info("storage is empty; skipping upgrade gate", "on_disk_version", currentVersion)
		}
	}

	if currentVersion == storageVersionCurrent {
		if b.log != nil {
			b.log.Info("storage version current; no migrations to run", "version", currentVersion)
		}
		b.storageVersion = currentVersion
		return nil
	}

	migrationStart := time.Now()
	dataDir := b.dataDir
	bytesBefore := dirSizeBytes(dataDir)
	if b.log != nil {
		b.log.Info("storage upgrade plan",
			"from_version", currentVersion,
			"to_version", storageVersionCurrent,
			"data_dir_bytes_before", bytesBefore,
		)
	}

	if currentVersion < storageVersionV1 {
		armStart := time.Now()
		if b.log != nil {
			b.log.Info("running migration arm v0→v1")
		}
		if err := b.migrateV0ToV1(); err != nil {
			return fmt.Errorf("migration v0→v1 failed: %w", err)
		}
		currentVersion = storageVersionV1
		b.migrationDidRun = true
		if b.log != nil {
			b.log.Info("migration arm v0→v1 complete",
				"duration_ms", time.Since(armStart).Milliseconds(),
			)
		}
	}

	if currentVersion < storageVersionPropKeyDictV2 {
		armStart := time.Now()
		if b.log != nil {
			b.log.Info("running migration arm v1→v2")
		}
		if err := b.migrateV1ToV2(); err != nil {
			return fmt.Errorf("migration v1→v2 failed: %w", err)
		}
		currentVersion = storageVersionPropKeyDictV2
		b.migrationDidRun = true
		if b.log != nil {
			b.log.Info("migration arm v1→v2 complete",
				"duration_ms", time.Since(armStart).Milliseconds(),
			)
		}
	}

	b.storageVersion = currentVersion

	if b.log != nil {
		bytesAfter := dirSizeBytes(dataDir)
		var lsmBytes, vlogBytes int64
		if b.db != nil {
			lsmBytes, vlogBytes = b.db.Size()
		}
		var deltaBytes int64
		if bytesBefore > 0 {
			deltaBytes = bytesAfter - bytesBefore
		}
		b.log.Info("storage upgrade summary",
			"final_version", currentVersion,
			"total_duration_ms", time.Since(migrationStart).Milliseconds(),
			"data_dir_bytes_before", bytesBefore,
			"data_dir_bytes_after", bytesAfter,
			"data_dir_bytes_delta", deltaBytes,
			"lsm_bytes", lsmBytes,
			"vlog_bytes", vlogBytes,
		)
	}
	return nil
}

// dirSizeBytes walks dir and returns the sum of regular-file sizes, in
// bytes. Used purely for migration logging — failures return 0 so the
// log line still emits without short-circuiting the caller.
func dirSizeBytes(dir string) int64 {
	var total int64
	if dir == "" {
		return 0
	}
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info == nil {
			return nil
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	})
	return total
}
