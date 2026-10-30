package bolt

import (
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
)

func (session *Session) publishConnectionListing() {
	if session.server == nil || session.connectionID == "" {
		return
	}
	listing := cypher.ConnectionListing{ConnectionID: session.connectionID, Connector: "bolt", UserAgent: session.userAgent}
	if !session.connectedAt.IsZero() {
		listing.ConnectTime = session.connectedAt.Format(time.RFC3339Nano)
	}
	if session.conn != nil {
		if address := session.conn.LocalAddr(); address != nil {
			listing.ServerAddress = address.String()
		}
		if address := session.conn.RemoteAddr(); address != nil {
			listing.ClientAddress = address.String()
		}
	}
	if session.authResult != nil {
		listing.Username = session.authResult.Username
	}
	server := session.server
	server.mu.Lock()
	if server.connectionListings == nil {
		server.connectionListings = make(map[string]cypher.ConnectionListing)
	}
	server.connectionListings[session.connectionID] = listing
	server.mu.Unlock()
}

func (session *Session) removeConnectionListing() {
	if session.server != nil {
		session.server.mu.Lock()
		delete(session.server.connectionListings, session.connectionID)
		session.server.mu.Unlock()
	}
}

// ConnectionListings returns an immutable snapshot of accepted Bolt connections.
// A companion HTTP server can use it as its DBMS connection listing source.
func (server *Server) ConnectionListings() []cypher.ConnectionListing {
	server.mu.RLock()
	defer server.mu.RUnlock()
	listings := make([]cypher.ConnectionListing, 0, len(server.connectionListings))
	for _, listing := range server.connectionListings {
		listings = append(listings, listing)
	}
	return listings
}
