package knowledgepolicy

import "context"

// AccessMetaReadMode selects what AccessMetaReadRequest reads from the access
// metadata namespace.
type AccessMetaReadMode int

const (
	// AccessMetaReadPoint is the default: it reads the entry stored for
	// AccessMetaReadRequest.EntityID.
	AccessMetaReadPoint AccessMetaReadMode = iota
	// AccessMetaReadAll reads every entity access-state entry. Any EntityID is
	// ignored.
	AccessMetaReadAll
)

// AccessMetaReadRequest is one committed access metadata read. The zero value
// reads the point entry for an empty entity ID.
type AccessMetaReadRequest struct {
	Mode     AccessMetaReadMode
	EntityID string
}

// AccessMetaReadResult is the result of an AccessMetaReadRequest. Point reads
// return Entry; AccessMetaReadAll returns Entries.
type AccessMetaReadResult struct {
	Entry   *AccessMetaEntry
	Entries []*AccessMetaEntry
}

// AccessMetaStore is the persistence interface for AccessMetaEntry. ReadAccessMeta
// is the only read entry point; ctx-first WriteAccessMeta writes one entity's
// entry under an explicit policy.
//
// ctx belongs to the operation that owns the write: an access flush passes its
// own flush lifetime, never an expired initiating request. ctx owns admission
// and preparation only; once a write is accepted its publication completes in
// the store, so a canceled ctx returns its own error and leaves no published
// entry.
type AccessMetaStore interface {
	ReadAccessMeta(request AccessMetaReadRequest) (AccessMetaReadResult, error)
	WriteAccessMeta(ctx context.Context, request AccessMetaWriteRequest) (written bool, err error)
}
