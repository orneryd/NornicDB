package storage

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

var projectedStoreMarkerKey = []byte{prefixMVCCMeta, prefixMVCCMetaProjectedFormat}

// validateProjectedStoreMode prevents a catalog restore or changed open option
// from reinterpreting an existing store under a different physical format.
// Existing default-off stores are read-only to this function: no marker is
// written unless a new, empty store explicitly opts in.
func validateProjectedStoreMode(db *managedBadgerDB, projected bool) error {
	marked := false
	hasExistingData := false
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(projectedStoreMarkerKey)
		if err == nil {
			marked = true
			return item.Value(func(value []byte) error {
				if len(value) != 1 || value[0] != 1 {
					return fmt.Errorf("invalid projected store marker: %w", ErrInvalidData)
				}
				return nil
			})
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		if !projected {
			return nil
		}
		it := txn.NewIterator(badgerIteratorOptions())
		defer it.Close()
		it.Rewind()
		hasExistingData = it.Valid()
		return nil
	})
	if err != nil {
		return fmt.Errorf("checking projected store mode: %w", err)
	}
	if marked {
		if !projected {
			return fmt.Errorf("projected store cannot be opened without projected mode: %w", ErrInvalidData)
		}
		return nil
	}
	if !projected {
		return nil
	}
	if hasExistingData {
		return fmt.Errorf("projected mode cannot be enabled on an existing store: %w", ErrInvalidData)
	}
	if err := db.Update(func(txn *badger.Txn) error { return txn.Set(projectedStoreMarkerKey, []byte{1}) }); err != nil {
		return fmt.Errorf("writing projected store marker: %w", err)
	}
	return nil
}
