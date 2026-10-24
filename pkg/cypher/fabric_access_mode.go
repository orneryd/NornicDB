package cypher

import (
	"sync"

	"github.com/orneryd/nornicdb/pkg/fabric"
)

func init() {
	fabric.SetAccessMode(fragmentWrites)
}

// fragmentWrites is the access mode of a statement run inside a composite
// statement, from its text, as Neo4j decides a query's access mode: it
// writes when it has a write clause or a schema command, or calls a
// procedure that writes. It is a hint: which session a remote participant
// opens, and how Fabric dispatches a fragment. Which database a statement
// writes is decided where its writes reach storage (newParticipantStorage).
//
// On a local database the procedures are this server's own: one registered
// as WRITE or SCHEMA writes, and a name it doesn't know counts as a write
// (QueryPermissionRequirements). On a remote constituent a procedure is the
// remote's own definition, so only a built-in's mode says what it does: a
// user or plugin procedure counts as a write, and so does a READ built-in
// that runs a statement it is given (ProcedureSpec.ExecutesStatements),
// since a NornicDB remote lets that statement write.
func fragmentWrites(query string, remote bool) bool {
	return fragmentAccessModes.decide(query, remote)
}

// accessModeMemo keeps the access mode of recent statement texts, for the
// procedure modes it was decided with (ProcedureRegistry.modeGeneration): a
// fragment is dispatched once per input row.
type accessModeMemo struct {
	mu         sync.RWMutex
	generation uint64
	modes      map[accessModeKey]bool
}

type accessModeKey struct {
	query  string
	remote bool
}

// accessModeMemoLimit bounds the memo; it starts over when full.
const accessModeMemoLimit = 4096

var fragmentAccessModes = &accessModeMemo{}

func (m *accessModeMemo) decide(query string, remote bool) bool {
	key := accessModeKey{query: query, remote: remote}
	generation := globalProcedureRegistry.modeGeneration.Load()
	m.mu.RLock()
	writes, known := m.modes[key]
	known = known && m.generation == generation
	m.mu.RUnlock()
	if known {
		return writes
	}
	writes = statementWrites(query, remote)
	m.mu.Lock()
	if m.generation != generation || len(m.modes) >= accessModeMemoLimit {
		m.generation, m.modes = generation, make(map[accessModeKey]bool)
	}
	m.modes[key] = writes
	m.mu.Unlock()
	return writes
}

// statementWrites is fragmentWrites, undecided.
func statementWrites(query string, remote bool) bool {
	query = classificationText(query)
	requirements := permissionRequirements(query)
	if requirements.Write || requirements.Schema {
		return true
	}
	for _, name := range calledProcedureNames(query) {
		procedure, known := globalProcedureRegistry.Get(name)
		if !known || remote && (procedure.User || procedure.Spec.ExecutesStatements) {
			return true
		}
	}
	return false
}
