package storage

import valueconvert "github.com/orneryd/nornicdb/pkg/convert"

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

// RemotePathValue is an ordered remote path: node, relationship, node, and so
// on. Remote transports construct it from actual path metadata; an ordinary
// list never gains path identity. Preserve it when forwarding query bindings.
// For example, RETURN p through a remote Fabric constituent keeps its source
// node and relationship identities rather than becoming a generic list.
type RemotePathValue []RemoteEntityValue

// CypherOpaqueSlice preserves path identity when a generic list reader sees
// this slice carrier. A remote path is a graph scalar, not a Cypher list.
func (RemotePathValue) CypherOpaqueSlice() {}

const remoteIdentityKey = "\x00remoteIdentity"

type remoteNumericIdentity struct{ id, start, end int64 }

// LegacyIDs returns the deprecated numeric source IDs retained by the remote
// transport. Older hand-built markers have no numeric identity and return -1.
// Element IDs remain available on the value's existing elementId fields.
func (v RemoteEntityValue) LegacyIDs() (id, start, end int64) {
	if identity, ok := v[remoteIdentityKey].(remoteNumericIdentity); ok {
		return identity.id, identity.start, identity.end
	}
	return -1, -1, -1
}

// MarshalJSON preserves the public remote map shape without exposing private
// numeric transport identity. Entity properties are never modified.
func (v RemoteEntityValue) MarshalJSON() ([]byte, error) {
	return valueconvert.PropertyJSON.Marshal(PublicRemoteMap(map[string]interface{}(v)))
}

// PublicRemoteMap removes sealed internal transport metadata from a map
// converted from RemoteEntityValue. Ordinary maps, including a user string
// under the same key, are returned unchanged without copying or graph identity.
// Use valueconvert.PropertyJSON.Marshal(PublicRemoteMap(map[string]interface{}(entity))) when a Go
// integration explicitly needs an ordinary public map rather than the marker.
func PublicRemoteMap(value map[string]interface{}) map[string]interface{} {
	if _, private := value[remoteIdentityKey].(remoteNumericIdentity); !private {
		return value
	}
	public := make(map[string]interface{}, len(value)-1)
	for key, element := range value {
		if key != remoteIdentityKey {
			public[key] = element
		}
	}
	return public
}

// Parts validates a remote path and returns its ordered nodes and relationships.
// Nil and empty carriers are not paths; a zero-hop path contains one node.
// For example, nodes, relationships, ok := path.Parts() preserves the remote
// markers for nodes(path), relationships(path), and protocol serialization.
func (p RemotePathValue) Parts() (nodes, relationships []RemoteEntityValue, ok bool) {
	if len(p) == 0 || len(p)%2 == 0 {
		return nil, nil, false
	}
	nodes = make([]RemoteEntityValue, 0, (len(p)+1)/2)
	relationships = make([]RemoteEntityValue, 0, len(p)/2)
	for i, value := range p {
		id, _ := value["elementId"].(string)
		if id == "" {
			return nil, nil, false
		}
		_, relationship := value["type"].(string)
		if i%2 == 0 {
			if relationship {
				return nil, nil, false
			}
			nodes = append(nodes, value)
			continue
		}
		if !relationship {
			return nil, nil, false
		}
		start, _ := value["startNodeElementId"].(string)
		end, _ := value["endNodeElementId"].(string)
		from, _ := p[i-1]["elementId"].(string)
		to, _ := p[i+1]["elementId"].(string)
		if !((start == from && end == to) || (start == to && end == from)) {
			return nil, nil, false
		}
		relationships = append(relationships, value)
	}
	return nodes, relationships, true
}
