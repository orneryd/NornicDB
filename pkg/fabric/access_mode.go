package fabric

import (
	"sync/atomic"
)

// AccessMode decides whether a statement writes, from its text: query is the
// statement, remote says whether it runs on a remote constituent. The Cypher
// executor installs it (SetAccessMode). It decides which session a fragment
// opens when it runs; no write is claimed from it before then.
type AccessMode func(query string, remote bool) bool

var accessMode atomic.Pointer[AccessMode]

// SetAccessMode installs the access-mode owner every fragment's write
// decision comes from.
func SetAccessMode(mode AccessMode) {
	accessMode.Store(&mode)
}

// fragmentMayWrite is the access mode of a fragment wherever it runs, for a
// choice of execution strategy made before its graph is resolved: a fragment
// that writes on a remote constituent may write.
func fragmentMayWrite(query string) bool {
	return decideAccessMode(query, true)
}

// decideAccessMode is the installed owner's decision. Without an owner every
// statement counts as a write, so no write runs without its claim.
func decideAccessMode(query string, remote bool) bool {
	mode := accessMode.Load()
	if mode == nil || *mode == nil {
		return true
	}
	return (*mode)(query, remote)
}

// LocalParticipant is the participant name of local database dbName: the
// identity a write to it claims (FabricTransaction.ClaimWrite), whichever
// name or alias selected it.
func LocalParticipant(dbName string) string {
	return participantKeyFromLocation(&LocationLocal{DBName: dbName})
}

// ParticipantOf is the participant name of the database loc names.
func ParticipantOf(loc Location) string {
	return participantKeyFromLocation(loc)
}
