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
	if ctx == nil {
		ctx = context.Background()
	}
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
		// The marker certifies the derived storage indexes as current, which
		// the startup rebuild then skips; the temporal history index is one of
		// them, so NO OVERLAP validation can trust it (#1126). Without this a
		// clean restart left every temporal constraint on the label scan.
		// A store whose temporal index predates the stable key format is
		// rebuilt once first: its entries were keyed per process.
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

// MarkCleanShutdown durably records that all writers were stopped and flushed.
// It must be called only at the end of an orderly owner shutdown.
func (b *BadgerEngine) MarkCleanShutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
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
	return b.db.Sync()
}
