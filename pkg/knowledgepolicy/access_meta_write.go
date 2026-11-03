package knowledgepolicy

import "errors"

// ErrEntityMetaNotFound is an absent stored entity, not a lookup failure.
// Storage implementations may wrap this together with their own not-found error.
var ErrEntityMetaNotFound = errors.New("access entity metadata not found")

// AccessMetaWriteMode selects unconditional recording or live-entity flushing.
// The zero value is invalid.
type AccessMetaWriteMode uint8

const (
	// RecordState preserves unconditional restore, internal work-key and catalog merging.
	RecordState AccessMetaWriteMode = iota + 1
	// FlushExistingEntity skips an absent entity atomically with the metadata write.
	// Hidden stored entities remain eligible. Retained metadata is not removed.
	FlushExistingEntity
)

// AccessMetaWriteRequest is one metadata write. Entry and an explicit Mode are required.
type AccessMetaWriteRequest struct {
	Mode     AccessMetaWriteMode
	EntityID string
	Entry    *AccessMetaEntry
}
