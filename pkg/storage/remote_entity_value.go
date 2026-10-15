package storage

// RemoteEntityValue is a graph entity decoded from a remote database transport.
// It preserves the remote element ID and nested properties without confusing an
// ordinary Cypher or JSON map with a node or relationship. RemoteEngine creates
// these values only from Bolt graph values or HTTP graph response metadata.
//
// The map representation preserves existing JSON output. For example, a remote
// node marshals as {"elementId":"4:remote:n","labels":["Doc"],"properties":{}}.
// Consumers should preserve this type when forwarding query bindings; converting
// it to map[string]interface{} intentionally discards its graph provenance.
// This Go type is a representation marker, not an authorization boundary.
type RemoteEntityValue map[string]interface{}
