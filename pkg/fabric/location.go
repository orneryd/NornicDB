package fabric

import "github.com/orneryd/nornicdb/pkg/storage"

// Location represents where a FragmentExec runs.
// It mirrors Neo4j's Location.java sealed hierarchy.
type Location interface {
	// location is a marker method preventing external implementations.
	location()

	// DatabaseName returns the target database name.
	DatabaseName() string
}

// LocationLocal indicates execution on the current NornicDB instance.
type LocationLocal struct {
	// DBName is the local database name.
	DBName string
}

func (*LocationLocal) location() {}

// DatabaseName returns the local database name.
func (l *LocationLocal) DatabaseName() string {
	return l.DBName
}

// LocationRemote indicates execution on a remote NornicDB instance.
type LocationRemote struct {
	// GraphName identifies a managed constituent, including its composite
	// namespace. Aliases at the same endpoint may have different credentials.
	GraphName string

	// ResolveEngine obtains a managed constituent's runtime engine from its
	// DatabaseManager. Credentials remain owned by the manager rather than
	// being copied from persisted metadata into the Fabric catalog.
	ResolveEngine func(authToken string) (storage.Engine, error)

	// DBName is the database name on the remote instance.
	DBName string

	// URI is the remote endpoint URI (bolt://, neo4j://, http://, https://).
	URI string

	// AuthMode is the authentication mode: "oidc_forwarding" or "user_password".
	AuthMode string

	// User is the explicit username (only when AuthMode == "user_password").
	User string

	// Password is the explicit password (only when AuthMode == "user_password").
	// This is the decrypted plaintext resolved at runtime; never persisted.
	Password string
}

func (*LocationRemote) location() {}

// DatabaseName returns the remote database name.
func (l *LocationRemote) DatabaseName() string {
	return l.DBName
}
