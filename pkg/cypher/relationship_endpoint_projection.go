package cypher

import (
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
// Transaction reads use the canonical query snapshot. Read-only auto-commit
// scalar reads may use storage projection when no embedding metadata is
// requested; a projected storage read cannot replace the transaction view.
type relationshipEndpointReader struct {
	executor *StorageExecutor
	store    storage.Engine
}

func (e *StorageExecutor) newRelationshipEndpointReader(store storage.Engine) relationshipEndpointReader {
	return relationshipEndpointReader{executor: e, store: store}
}

func (reader relationshipEndpointReader) read(id storage.NodeID, properties []string) (*storage.Node, error) {
	// Project selected scalar properties only for auto-commit read-only
	// statements. Transaction snapshots and embedding metadata must use the
	// canonical query-node read to preserve visibility and sidecar state.
	if properties != nil && reader.executor.readOnlyQueryNodes {
		metadata := false
		for _, property := range properties {
			if storage.IsEmbeddingMetadataProperty(property) {
				metadata = true
				break
			}
		}
		if !metadata {
			if projected, ok := reader.store.(storage.ProjectedNodeReader); ok {
				node, err := projected.GetNodeProjected(id, properties)
				if err != nil || node == nil {
					return node, err
				}
				if shouldHideSystemNodes(reader.store) && isSystemNode(node) {
					return nil, storage.ErrNotFound
				}
				return node, nil
			}
		}
	}
	node, err := reader.executor.getQueryNode(reader.store, id)
	if err != nil || node == nil {
		return node, err
	}
	if shouldHideSystemNodes(reader.store) && isSystemNode(node) {
		return nil, storage.ErrNotFound
	}
	if properties == nil {
		return node, nil
	}
	out := *node
	out.ChunkEmbeddings, out.NamedEmbeddings = nil, nil
	out.Properties = make(map[string]interface{}, len(properties))
	for _, property := range properties {
		if value, exists := node.Properties[property]; exists {
			out.Properties[property] = value
		}
	}
	return &out, nil
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
	id, exists := e.relationshipEndpointID(edge, n.end)
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
	node := e.readRelationshipEndpoint(id, []string{n.property})
	value, ok := evaluateRowPropertyChain(e.queryVisibleValue(node), n.property)
	if ok && cache != nil {
		cache.keep(key, value)
	}
	return value, ok, nil
}
