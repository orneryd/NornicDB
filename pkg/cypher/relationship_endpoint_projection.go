package cypher

import (
	"slices"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// An internal parameter binding survives row-local WITH projection, but is
// never a user-visible column. The source owns and drops the cache.
const endpointPropertyCacheKey = "$\x00nornic_endpoint_properties"
const endpointPropertyCacheBytes = 1 << 20

type endpointPropertyKey struct {
	id       storage.NodeID
	property string
}

type endpointPropertyCache struct {
	values map[endpointPropertyKey]interface{}
	bytes  int64
}

// relationshipEndpointReader owns endpoint visibility and selected properties.
// Every endpoint first crosses the canonical query read, including scalar and
// existence-only reads. Storage projection and existence hints do not carry
// the query view's complete visibility contract.
type relationshipEndpointReader struct {
	executor   *StorageExecutor
	store      storage.Engine
	hideSystem func() bool
}

func (e *StorageExecutor) newRelationshipEndpointReader(store storage.Engine) relationshipEndpointReader {
	lookup := e.identityLookup(store)
	return relationshipEndpointReader{executor: e, store: store, hideSystem: func() bool { return lookup.Database() != "system" }}
}

func (reader relationshipEndpointReader) read(id storage.NodeID, _ []string) (*storage.Node, error) {
	node, err := reader.executor.getQueryNode(reader.store, id)
	if err != nil || node == nil {
		return node, err
	}
	if reader.hideSystem() && isSystemNode(node) {
		return nil, storage.ErrNotFound
	}
	// Transient scalar consumers inspect the canonical view directly. Owners
	// that retain an endpoint must use readRetained with their property demand.
	return node, nil
}

// readRetained takes ownership of the detached canonical read before it can
// escape into a traversal cache or binding. A nil projection requires the full
// entity; a non-nil projection retains only its demanded property values.
// Reuse the caller-owned node and small map instead of copying every read.
func (reader relationshipEndpointReader) readRetained(id storage.NodeID, properties []string) (*storage.Node, error) {
	node, err := reader.read(id, properties)
	if err != nil || node == nil || properties == nil {
		return node, err
	}
	var selected map[string]interface{}
	if len(properties) > 0 {
		selected = node.Properties
		if len(selected) > 8 && len(selected) > 2*len(properties) {
			// Do not retain a wide body's map buckets for a narrow selection.
			selected = make(map[string]interface{}, len(properties))
			for _, property := range properties {
				if value, ok := node.Properties[property]; ok {
					selected[property] = value
				}
			}
		} else {
			for property := range selected {
				if !slices.Contains(properties, property) {
					delete(selected, property)
				}
			}
		}
	}
	var metadata map[string]interface{}
	if slices.Contains(properties, "has_embedding") {
		metadata = map[string]interface{}{"has_embedding": nodeHasEmbeddingValue(node)}
	}
	// Timestamps belong to property visibility. Vectors, embedding bodies and
	// storage snapshot owners are unrelated to an ID/label/scalar projection.
	*node = storage.Node{ID: node.ID, Database: node.Database, Labels: node.Labels, Properties: selected,
		CreatedAt: node.CreatedAt, UpdatedAt: node.UpdatedAt,
		VisibilitySuppressed: node.VisibilitySuppressed, EmbedMeta: metadata}
	return node, nil
}

func (c *endpointPropertyCache) keep(key endpointPropertyKey, value interface{}) {
	// Include map/entry overhead and key storage, not just property payloads.
	size := int64(128+len(key.id)+len(key.property)) + util.EstimateValueBytes(value)
	if size > endpointPropertyCacheBytes {
		return
	}
	if c.bytes+size > endpointPropertyCacheBytes {
		clear(c.values)
		c.bytes = 0
	}
	if c.values == nil {
		c.values = make(map[endpointPropertyKey]interface{})
	}
	c.values[key] = value
	c.bytes += size
}

// rowEndpointPropertyNode selects a property after endpoint visibility. Both the text evaluator and compiled value program use
// this same reader and the ordinary endpoint existence/type contract.
type rowEndpointPropertyNode struct {
	argument, property string
	end                bool
}

func endpointPropertyNode(base, property string) *rowEndpointPropertyNode {
	function, argument, call := parseFunctionCallWS(base)
	if !call || (!equalFoldASCII(function, "startNode") && !equalFoldASCII(function, "endNode")) || !isValidIdentifier(property) {
		return nil
	}
	return &rowEndpointPropertyNode{argument: strings.TrimSpace(argument), property: property, end: equalFoldASCII(function, "endNode")}
}

func (n *rowEndpointPropertyNode) eval(e *StorageExecutor, values map[string]interface{}, _ [][]rowValueMemo) (interface{}, bool, error) {
	value, resolved, err := e.evaluateRowValue(n.argument, values)
	if !resolved || err != nil {
		return nil, resolved, err
	}
	edge, err := relationshipEndpointArgument(value, n.end)
	if err != nil {
		return nil, false, err
	}
	id, exists, err := e.relationshipEndpointID(edge, n.end)
	if err != nil { return nil, false, err }
	if !exists {
		return nil, true, nil
	}
	cache, _ := values[endpointPropertyCacheKey].(*endpointPropertyCache)
	key := endpointPropertyKey{id: id, property: n.property}
	if cache != nil {
		if value, cached := cache.values[key]; cached {
			return value, true, nil
		}
	}
	node, err := e.readRelationshipEndpoint(id, []string{n.property})
	if err != nil { return nil, false, err }
	value, ok := evaluateRowPropertyChain(e.queryVisibleValue(node), n.property)
	if ok && cache != nil {
		cache.keep(key, value)
	}
	return value, ok, nil
}
