package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// How GDS and node-similarity procedures read, match and show nodes
// (F-GDS-SIM). This file owns all three: readGDSNode is the only reader of
// a node-valued input, gdsNodeRef.find the only matching rule, and
// renderGDSNodeColumns the only way a node id leaves a result row.
// gdsNodeProcedures lists every procedure and function of the family with
// the kinds each input accepts and the columns its ids leave through
// (TestGDSNodeIdentityOwner checks the list against the registries and the
// source).

// gdsNodeKind is the kind of value an input named a node with.
type gdsNodeKind uint8

const (
	gdsLocalNode  gdsNodeKind = 1 << iota // a stored node value
	gdsRemoteNode                         // a remote constituent's node (a composite database's USE), by its elementId
	gdsIDText                             // an id given as a STRING
	gdsIntegerID                          // an id given as an INTEGER
)

// The kinds each input accepts.
const (
	// gdsStoredKinds: inputs resolved against a projection or storage,
	// which hold no remote node.
	gdsStoredKinds = gdsLocalNode | gdsIDText | gdsIntegerID
	// gdsRowKinds: nornicdb.similarity.*, which read nothing from storage,
	// so a remote node is a node like another.
	gdsRowKinds = gdsStoredKinds | gdsRemoteNode
	// gdsAggregationKinds: an aggregation projection's nodes, as in GDS.
	gdsAggregationKinds = gdsLocalNode | gdsIntegerID
	// gdsNodeValueKinds: inputs that need the node itself.
	gdsNodeValueKinds = gdsLocalNode
)

// gdsRemoteKey prefixes a remote node's elementId in a key, so a remote node
// never shares a key with a local id, even when the texts are equal.
const gdsRemoteKey = "\x00remote\x00"

// gdsNodeRef is a node an input named: its key and the kind it was given
// as. The key is a stored id, an id's text, or gdsRemoteKey and an
// elementId.
type gdsNodeRef struct {
	key  string
	kind gdsNodeKind
}

// readGDSNode reads a node-valued input; false when value is no node of an
// accepted kind.
func readGDSNode(value interface{}, accepted gdsNodeKind) (gdsNodeRef, bool) {
	var ref gdsNodeRef
	switch node := value.(type) {
	case *storage.Node:
		if node == nil {
			return ref, false
		}
		ref = gdsNodeRef{key: string(node.ID), kind: gdsLocalNode}
	case storage.RemoteEntityValue:
		id, ok := node["elementId"].(string)
		if node == nil || cypherValueKindOf(node) != valueKindNode || !ok || id == "" {
			return ref, false
		}
		ref = gdsNodeRef{key: gdsRemoteKey + id, kind: gdsRemoteNode}
	case string:
		ref = gdsNodeRef{key: node, kind: gdsIDText}
	default:
		if !isIntegerProcedureValue(value) {
			return ref, false
		}
		ref = gdsNodeRef{key: fmt.Sprint(value), kind: gdsIntegerID}
	}
	return ref, accepted&ref.kind != 0
}

// find is the key ref names among the keys has holds: its own, or, for an
// id string only, a remote node with that elementId. A node value names
// only a node of its own kind.
func (ref gdsNodeRef) find(has func(key string) bool) (string, bool) {
	if has(ref.key) {
		return ref.key, true
	}
	if ref.kind == gdsIDText && has(gdsRemoteKey+ref.key) {
		return gdsRemoteKey + ref.key, true
	}
	return "", false
}

// isNode reports whether ref was given as a node value, local or remote.
func (ref gdsNodeRef) isNode() bool {
	return ref.kind&(gdsLocalNode|gdsRemoteNode) != 0
}

// gdsShownKey is a key as output shows it: a remote node's elementId, else
// the id.
func gdsShownKey(key string) string {
	return strings.TrimPrefix(key, gdsRemoteKey)
}

// gdsShownBefore orders two keys by the ids output shows, then by key: a
// local and a remote node can show the same id.
func gdsShownBefore(a, b string) bool {
	if shownA, shownB := gdsShownKey(a), gdsShownKey(b); shownA != shownB {
		return shownA < shownB
	}
	return a < b
}

// gdsNodeRendering is where a procedure's shown ids come from.
type gdsNodeRendering uint8

const (
	gdsRenderNone       gdsNodeRendering = iota // no node ids out
	gdsRenderProjection                         // the projection the stream read; the stored ids as read without one
	gdsRenderRows                               // the rows' keys (gdsShownKey)
)

// gdsNodeProcedure is how one procedure or function of the family takes and
// returns nodes.
type gdsNodeProcedure struct {
	// inputs are its node-valued inputs and the kinds each accepts.
	inputs map[string]gdsNodeKind
	// columns are its output columns holding node ids: an id, a list of
	// ids, or a list of maps whose nodeId is one.
	columns   []string
	rendering gdsNodeRendering
}

// gdsLinkPredictionInputs are every gds.linkPrediction.*.stream's inputs.
var gdsLinkPredictionInputs = map[string]gdsNodeKind{"sourceNode": gdsStoredKinds, "targetNodes": gdsStoredKinds}

// gdsNodeProcedures is every GDS and similarity procedure.
var gdsNodeProcedures = map[string]gdsNodeProcedure{
	"gds.version":                        {},
	"gds.graph.exists":                   {},
	"gds.graph.list":                     {},
	"gds.graph.drop":                     {},
	"gds.graph.project":                  {},
	"gds.fastRP.stats":                   {},
	"gds.fastRP.stream":                  {columns: []string{"nodeId"}, rendering: gdsRenderProjection},
	"gds.louvain.stream":                 {columns: []string{"nodeId"}, rendering: gdsRenderProjection},
	"gds.nodeSimilarity.stream":          {columns: []string{"node1", "node2", "shared"}, rendering: gdsRenderProjection},
	"gds.nodeSimilarity.filtered.stream": {inputs: map[string]gdsNodeKind{"sourceNodeFilter": gdsStoredKinds, "targetNodeFilter": gdsStoredKinds}, columns: []string{"node1", "node2", "shared"}, rendering: gdsRenderProjection},
	"gds.linkPrediction.adamicAdar.stream":             {inputs: gdsLinkPredictionInputs, columns: []string{"node1", "node2"}, rendering: gdsRenderProjection},
	"gds.linkPrediction.commonNeighbors.stream":        {inputs: gdsLinkPredictionInputs, columns: []string{"node1", "node2"}, rendering: gdsRenderProjection},
	"gds.linkPrediction.resourceAllocation.stream":     {inputs: gdsLinkPredictionInputs, columns: []string{"node1", "node2"}, rendering: gdsRenderProjection},
	"gds.linkPrediction.preferentialAttachment.stream": {inputs: gdsLinkPredictionInputs, columns: []string{"node1", "node2"}, rendering: gdsRenderProjection},
	"gds.linkPrediction.jaccard.stream":                {inputs: gdsLinkPredictionInputs, columns: []string{"node1", "node2"}, rendering: gdsRenderProjection},
	"gds.linkPrediction.predict.stream":                {inputs: gdsLinkPredictionInputs, columns: []string{"node1", "node2"}, rendering: gdsRenderProjection},
	"nornicdb.similarity.stream": {inputs: map[string]gdsNodeKind{"sourceNode": gdsRowKinds, "rows.source": gdsRowKinds, "rows.thing": gdsRowKinds},
		columns: []string{"node1", "node2", "shared"}, rendering: gdsRenderRows},
}

// gdsNodeFunctions is every GDS function, by its catalog name, with the
// node-valued arguments each reads. The aggregation form of
// gds.graph.project reads its rows' nodes; gds.util.* read a node or an id;
// gds.alpha.linkprediction.* need the nodes themselves.
var gdsNodeFunctions = map[string]map[string]gdsNodeKind{
	"gds.graph.project":                               {"sourceNode": gdsAggregationKinds, "targetNode": gdsAggregationKinds},
	"gds.graph.exists":                                nil,
	"gds.version":                                     nil,
	"gds.util.asNode":                                 {"nodeId": gdsStoredKinds},
	"gds.util.asNodes":                                {"nodeIds": gdsStoredKinds},
	"gds.util.nodeProperty":                           {"nodeId": gdsStoredKinds},
	"gds.util.NaN":                                    nil,
	"gds.util.infinity":                               nil,
	"gds.util.isFinite":                               nil,
	"gds.util.isInfinite":                             nil,
	"gds.alpha.linkprediction.adamicAdar":             {"node1": gdsNodeValueKinds, "node2": gdsNodeValueKinds},
	"gds.alpha.linkprediction.commonNeighbors":        {"node1": gdsNodeValueKinds, "node2": gdsNodeValueKinds},
	"gds.alpha.linkprediction.preferentialAttachment": {"node1": gdsNodeValueKinds, "node2": gdsNodeValueKinds},
	"gds.alpha.linkprediction.resourceAllocation":     {"node1": gdsNodeValueKinds, "node2": gdsNodeValueKinds},
	"gds.alpha.linkprediction.totalNeighbors":         {"node1": gdsNodeValueKinds, "node2": gdsNodeValueKinds},
	"gds.alpha.linkprediction.sameCommunity":          {"node1": gdsNodeValueKinds, "node2": gdsNodeValueKinds},
}

// readGDSNodeValue reads an input that needs the node itself
// (gdsNodeValueKinds): the stored node value.
func readGDSNodeValue(value interface{}) (*storage.Node, bool) {
	if _, ok := readGDSNode(value, gdsNodeValueKinds); !ok {
		return nil, false
	}
	return value.(*storage.Node), true
}

// gdsStreamHandler wraps a procedure's handler so the node ids of a stream
// that read a projection leave through that projection's renderer; other
// procedures are unchanged.
func gdsStreamHandler(procedure string, handler ProcedureHandler) ProcedureHandler {
	spec, ok := gdsNodeProcedures[procedure]
	if !ok || spec.rendering != gdsRenderProjection {
		return handler
	}
	return func(ctx context.Context, e *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
		result, err := handler(ctx, e, cypher, args)
		if err != nil || result == nil {
			return result, err
		}
		// The projection the stream computed from, not a catalog lookup now:
		// the name may have been dropped or replaced meanwhile. Nil when the
		// stream read the stored graph: its ids are the stored ids as read.
		if projection := result.gdsProjection; projection != nil {
			result.gdsProjection = nil
			projection.renderNodeIDs(result, spec.columns)
		}
		return result, nil
	}
}

// renderNodeIDs renders the result's node-id columns through nodeIDValue.
func (p *GraphProjection) renderNodeIDs(result *ExecuteResult, columns []string) {
	renderGDSNodeColumns(result, columns, p.nodeIDValue)
}

// renderGDSRowKeys renders a rows procedure's node-id columns: a remote
// node's elementId, else the id.
func renderGDSRowKeys(result *ExecuteResult, columns []string) {
	renderGDSNodeColumns(result, columns, func(key string) interface{} { return gdsShownKey(key) })
}

// renderGDSNodeColumns renders the result's node-id columns through render:
// a key there, each key of a list there, and the nodeId of each map of a
// list there.
func renderGDSNodeColumns(result *ExecuteResult, columns []string, render func(key string) interface{}) {
	var indexes []int
	for i, column := range result.Columns {
		for _, name := range columns {
			if column == name {
				indexes = append(indexes, i)
			}
		}
	}
	one := func(value interface{}) interface{} {
		if key, ok := value.(string); ok {
			return render(key)
		}
		return value
	}
	for _, row := range result.Rows {
		for _, i := range indexes {
			if i >= len(row) {
				continue
			}
			list, ok := row[i].([]interface{})
			if !ok {
				row[i] = one(row[i])
				continue
			}
			for j, item := range list {
				if entry, ok := item.(map[string]interface{}); ok {
					entry["nodeId"] = one(entry["nodeId"])
				} else {
					list[j] = one(item)
				}
			}
		}
	}
}
