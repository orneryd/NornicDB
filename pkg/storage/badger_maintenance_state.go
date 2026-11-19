package storage

import (
	"context"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

var cleanShutdownMarkerKey = []byte{prefixMVCCMeta, prefixMVCCMetaCleanShutdown}

// ConsumeCleanShutdownMarker atomically reads and removes the durable clean
// shutdown marker. Removing it before serving traffic makes an ungraceful exit
// conservative: the next startup cannot mistake the crashed run for a clean one.
func (b *BadgerEngine) ConsumeCleanShutdownMarker(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := b.ensureOpen(false); err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	clean := b.startupCleanShutdown
	if !b.startupCleanShutdownChecked {
		var err error
		clean, err = b.takeCleanShutdownMarker(ctx)
		if err != nil {
			return false, err
		}
	}
	b.startupCleanShutdown = false
	b.startupCleanShutdownChecked = true
	if clean {
		// The marker also certifies the temporal index format.
		current, err := b.temporalIndexFormatIsCurrent()
		if err != nil {
			return false, err
		}
		if current {
			b.temporalIndexReady.Store(true)
		} else if err := b.RebuildTemporalIndexes(ctx); err != nil {
			return false, err
		}
	}
	return clean, nil
}

func (b *BadgerEngine) takeCleanShutdownMarker(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !b.work.Enter() {
		return false, ErrStorageClosed
	}
	defer b.work.Leave()
	clean := false
	err := b.db.updateMetadataContext(ctx, func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := txn.Get(cleanShutdownMarkerKey)
		switch {
		case err == nil:
			clean = true
			return txn.Delete(cleanShutdownMarkerKey)
		case errors.Is(err, badger.ErrKeyNotFound):
			return nil
		default:
			return err
		}
	})
	if err != nil {
		return false, err
	}
	if clean {
		if err := b.db.Sync(); err != nil {
			return false, err
		}
	}
	return clean, nil
}

// MarkCleanShutdown durably records that all writers were stopped and flushed.
// It must be called only at the end of an orderly owner shutdown.
func (b *BadgerEngine) MarkCleanShutdown(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !b.work.Enter() {
		return ErrStorageClosed
	}
	defer b.work.Leave()
	if err := b.db.updateMetadataContext(ctx, func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return txn.Set(cleanShutdownMarkerKey, []byte{1})
	}); err != nil {
		return err
	}
	if err := b.db.Sync(); err != nil {
		return err
	}
	b.startupCleanShutdownChecked = false
	return nil
}
