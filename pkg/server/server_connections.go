package server

import "github.com/orneryd/nornicdb/pkg/cypher"

// SetConnectionLister installs the instance's Bolt connection inventory before Start.
// For example, use boltServer.ConnectionListings to share network metadata with HTTP queries.
func (server *Server) SetConnectionLister(lister func() []cypher.ConnectionListing) {
	server.connectionLister = lister
}
