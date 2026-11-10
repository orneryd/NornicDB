package storage

import (
	"context"
	"maps"
)

// writeOwnedReadImagesLocked composes staged writes with committed images whose
// write locks advanced this transaction past its snapshot. A no-op mutation can
// hold such a lock without staging a write; its image must still shadow the old
// snapshot in collections, as it already does in point reads. These are read
// images only: they never enter pending writes, WAL, claims or mutation counts.
// Caller holds the transaction mutex. Ordinary reads return pending unchanged.
func writeOwnedReadImagesLocked[ID ~string, T any](ctx context.Context, pending map[ID]T, deleted map[ID]struct{}, locks *entityWriteLocks, kind uint8, read func(context.Context, ID) (T, error)) (map[ID]T, error) {
	if locks.latest == 0 {
		return pending, nil
	}
	var images map[ID]T
	for _, held := range locks.held {
		if !held.latest || held.request.key.entityKind != kind {
			continue
		}
		id := ID(held.request.key.entityID)
		if _, ok := pending[id]; ok {
			continue
		}
		if _, gone := deleted[id]; gone {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		image, err := read(ctx, id)
		if err != nil {
			return nil, err
		}
		if images == nil {
			images = maps.Clone(pending)
			if images == nil {
				images = make(map[ID]T)
			}
		}
		images[id] = image
	}
	if images == nil {
		return pending, nil
	}
	return images, nil
}
