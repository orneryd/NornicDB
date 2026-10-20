package cypher

import (
	"net/http"

	"github.com/orneryd/nornicdb/internal/testutil/remotenamespace"
)

// The shared HTTP fixture owns the same handshake for Cypher and server tests.
func respondRemoteNamespaceFixture(w http.ResponseWriter, r *http.Request) bool {
	return remotenamespace.Respond(w, r)
}
