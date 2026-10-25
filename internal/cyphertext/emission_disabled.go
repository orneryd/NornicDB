//go:build !cypheremission

package cyphertext

func recordNameEmission([]string, string) {}

// RecordPropertyReference marks the written-input DDL reader boundary. The
// recorder exists only in lab binaries built with -tags=cypheremission.
func RecordPropertyReference() {}

// RecordNameConstruction has no production observer. Tagged lab binaries
// record the caller; ordinary builds inline this empty marker away.
func RecordNameConstruction(string) {}
