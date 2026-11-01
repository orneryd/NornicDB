package knowledgepolicy

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
// is the only read entry point; WriteAccessMeta writes one entity's entry under an explicit policy.
type AccessMetaStore interface {
	ReadAccessMeta(request AccessMetaReadRequest) (AccessMetaReadResult, error)
	WriteAccessMeta(request AccessMetaWriteRequest) (written bool, err error)
}
