package storage

import (
	"context"
	"errors"
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
		empty = isStorageEmptyInTxn(txn)
		return nil
	})
	return empty, err
}

func isStorageEmptyInTxn(txn *badger.Txn) bool {
	for _, prefix := range [][]byte{{prefixNode}, {prefixEdge}} {
		opts := badgerIteratorOptions()
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		it.Rewind()
		present := it.ValidForPrefix(prefix)
		it.Close()
		if present {
			return false
		}
	}
	return true
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
//	V2 → V3: restore versioned adjacency and exact-case label/type indexes.
//
// Manual invocations cannot perform a V4 catalog payload upgrade: reopening
// is required before any manager can retain the legacy catalog snapshot.
// Existing V0/V1/V2/V3 manual migration behavior is retained.
//
// Sets engine.storageVersion to the post-migration version so the
// encode path can deterministically pick codecs from it.
func (b *BadgerEngine) RunOnStartMigrations(allowUpgrade bool) error {
	return b.runOnStartMigrations(allowUpgrade, migrationManual)
}

type migrationPhase uint8

const (
	migrationStartupOpening migrationPhase = iota
	migrationManual
)

// runOnStartMigrations is the single ordered runner. Opening owns the only
// point before a DatabaseManager can retain the catalog image; manual calls
// cannot safely replace that image without a supported owner refresh protocol.
func (b *BadgerEngine) runOnStartMigrations(allowUpgrade bool, phase migrationPhase) error {
	// Opening has not published this engine, so recovery can use the normal
	// transaction owner before the migration takes its exclusive write barrier.
	// A durable marker lets a retry finish recovery and the V4 catalog transform.
	replayCurrentCatalog := false
	upgradeAuthorized := allowUpgrade
	if phase == migrationStartupOpening && !b.inMemory {
		version, err := b.readSchemaVersion()
		if err != nil {
			return fmt.Errorf("read schema version: %w", err)
		}
		catalogPending, formatPending, err := b.v4WALRecoveryState()
		if err != nil {
			return err
		}
		pending := catalogPending || formatPending
		if formatPending && version != storageVersionCatalogV4 {
			return fmt.Errorf("wal: format-only recovery requires a V4 store")
		}
		// The marker is written only after the upgrade gate has admitted the
		// original attempt, including the empty-store case. A retry must finish
		// that admitted transaction even if replay made the store non-empty.
		upgradeAuthorized = upgradeAuthorized || pending
		oldWAL := false
		dir := filepath.Join(b.dataDir, "wal")
		if version == storageVersionCatalogV4 && allowUpgrade {
			if err := ValidateWALFormat(dir, WALValidateCurrent); err != nil {
				if !errors.Is(err, ErrWALUnsupportedVersion) {
					return err
				}
				oldWAL = true
			}
		}
		formatOnly := formatPending
		if version == storageVersionCatalogV4 && oldWAL && !pending {
			formatOnly, err = walV5OrLaterOnly(dir)
			if err != nil {
				return err
			}
		}
		if version < storageVersionCatalogV4 || pending || oldWAL {
			if version < storageVersionCatalogV4 && !upgradeAuthorized {
				empty, err := b.isStorageEmpty()
				if err != nil {
					return fmt.Errorf("checking storage emptiness: %w", err)
				}
				if !empty {
					return &ErrStorageUpgradeRequired{OnDisk: version, Current: storageVersionCurrent}
				}
				// The empty store is admitted without a flag. Remember that
				// decision: replay can populate it before the migration's
				// second emptiness check runs.
				upgradeAuthorized = true
			}
			if version == storageVersionCatalogV4 && !formatOnly && b.catalogUpgradeV4 == nil {
				empty, err := b.isStorageEmpty()
				if err != nil {
					return err
				}
				if !empty {
					return fmt.Errorf("V4 WAL recovery requires an explicit catalog transform plan")
				}
			}
			b.storageVersion = version
			recovered, err := b.recoverV4WALAtOpen(allowUpgrade, pending, formatOnly)
			if err != nil {
				return fmt.Errorf("V4 WAL migration and recovery: %w", err)
			}
			replayCurrentCatalog = version == storageVersionCatalogV4 && recovered && !formatOnly
		}
	}
	// The migrations rewrite stored state wholesale: a maintenance write
	// (beginMaintenanceWrite). Opened by NewBadgerEngineWithOptions, the engine
	// has no write-behind buffer yet and nothing else holds it; called on an
	// engine that serves, it also applies every acknowledged commit first and
	// keeps every write out.
	lifecycle, err := b.beginMaintenanceWrite(nil)
	if err != nil {
		return err
	}
	defer lifecycle.finish()
	currentVersion, err := b.readSchemaVersion()
	if err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}

	// Reject before any older arm can mutate a store: V4 catalog replacement
	// is startup-only even when the store first needs V0/V1/V2 repairs.
	if storageVersionCurrent >= storageVersionCatalogV4 && currentVersion < storageVersionCatalogV4 && phase != migrationStartupOpening {
		return fmt.Errorf("catalog V4 payload upgrade is startup-only; reopen the database with --upgrade-storage")
	}

	if b.log != nil {
		b.log.Info("storage migration check",
			"on_disk_version", currentVersion,
			"binary_version", storageVersionCurrent,
			"upgrade_authorized", upgradeAuthorized,
		)
	}

	// Fresh data directories have no node/edge bodies and no version
	// key. Skip the upgrade gate for them — there is nothing to lose
	// and no operator decision to gate behind. Detect emptiness by
	// looking for any node body at all.
	if currentVersion < storageVersionCurrent && !upgradeAuthorized {
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
		if replayCurrentCatalog {
			if err := b.migrateV3ToV4(context.Background(), true); err != nil {
				return fmt.Errorf("V4 catalog recovery: %w", err)
			}
		}
		if phase == migrationStartupOpening && !b.inMemory {
			if err := finalizeV4WALBackup(b.db, filepath.Join(b.dataDir, "wal")); err != nil {
				return err
			}
		}
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
	if currentVersion < storageVersionEdgeAdjacencyV3 {
		armStart := time.Now()
		if err := b.migrateV2ToV3(); err != nil {
			return fmt.Errorf("migration v2→v3 failed: %w", err)
		}
		currentVersion = storageVersionEdgeAdjacencyV3
		if b.log != nil {
			b.log.Info("migration arm v2→v3 complete", "duration_ms", time.Since(armStart).Milliseconds())
		}
	}
	if currentVersion < storageVersionCatalogV4 {
		if err := b.migrateV3ToV4(context.Background(), false); err != nil {
			return fmt.Errorf("migration v3→v4 failed: %w", err)
		}
		currentVersion = storageVersionCatalogV4
		if phase == migrationStartupOpening && !b.inMemory && b.dataDir != "" {
			if err := finalizeV4WALBackup(b.db, filepath.Join(b.dataDir, "wal")); err != nil {
				return err
			}
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
