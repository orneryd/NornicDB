package cypher

import (
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// How GDS and node-similarity procedures read, match and show nodes
// (F-GDS-SIM). This file owns all three: readGDSNode is the only reader of
// a node-valued input, gdsNodeRef.find the only matching rule, and
// gdsNodeOutput (projection streams) and gdsRowNodes (rows procedures) the
// only way a node id leaves a result row, once, as the row is built.
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

// A node's key in a rows procedure's maps, where local and remote nodes
// meet: a remote node's elementId after gdsRemoteKey, a local id that starts
// with NUL after gdsLocalKey, and any other id as it is. Keys of the two
// kinds never collide, whatever bytes a stored id holds, and the common id
// needs no copy.
const (
	gdsRemoteKey = "\x00R"
	gdsLocalKey  = "\x00L"
)

// gdsNodeRef is a node an input named: its id as given (a stored id, an
// id's text, an INTEGER id's digits, or a remote node's elementId) and the
// kind it was given as.
type gdsNodeRef struct {
	id   string
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
		ref = gdsNodeRef{id: string(node.ID), kind: gdsLocalNode}
	case storage.RemoteEntityValue:
		id, ok := node["elementId"].(string)
		if node == nil || cypherValueKindOf(node) != valueKindNode || !ok || id == "" {
			return ref, false
		}
		ref = gdsNodeRef{id: id, kind: gdsRemoteNode}
	case string:
		ref = gdsNodeRef{id: node, kind: gdsIDText}
	default:
		if !isIntegerProcedureValue(value) {
			return ref, false
		}
		ref = gdsNodeRef{id: fmt.Sprint(value), kind: gdsIntegerID}
	}
	return ref, accepted&ref.kind != 0
}

// key is ref's key in a rows procedure's maps.
func (ref gdsNodeRef) key() string {
	if ref.kind == gdsRemoteNode {
		return gdsRemoteKey + ref.id
	}
	if strings.HasPrefix(ref.id, "\x00") {
		return gdsLocalKey + ref.id
	}
	return ref.id
}

// find is the key ref names among the keys has holds: its own, or, for an
// id string only, a remote node with that elementId. A node value names
// only a node of its own kind.
func (ref gdsNodeRef) find(has func(key string) bool) (string, bool) {
	if key := ref.key(); has(key) {
		return key, true
	}
	if ref.kind == gdsIDText && has(gdsRemoteKey+ref.id) {
		return gdsRemoteKey + ref.id, true
	}
	return "", false
}

// isNode reports whether ref was given as a node value, local or remote.
func (ref gdsNodeRef) isNode() bool {
	return ref.kind&(gdsLocalNode|gdsRemoteNode) != 0
}

// gdsNodeRendering is where a procedure's shown ids come from.
type gdsNodeRendering uint8

const (
	gdsRenderNone       gdsNodeRendering = iota // no node ids out
	gdsRenderProjection                         // gdsNodeOutput: the projection the stream read; the stored ids as read without one
	gdsRenderRows                               // gdsRowNodes: the ids the rows gave
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
	"gds.version":                                      {},
	"gds.graph.exists":                                 {},
	"gds.graph.list":                                   {},
	"gds.graph.drop":                                   {},
	"gds.graph.project":                                {},
	"gds.fastRP.stats":                                 {},
	"gds.fastRP.stream":                                {columns: []string{"nodeId"}, rendering: gdsRenderProjection},
	"gds.louvain.stream":                               {columns: []string{"nodeId"}, rendering: gdsRenderProjection},
	"gds.nodeSimilarity.stream":                        {columns: []string{"node1", "node2", "shared"}, rendering: gdsRenderProjection},
	"gds.nodeSimilarity.filtered.stream":               {inputs: map[string]gdsNodeKind{"sourceNodeFilter": gdsStoredKinds, "targetNodeFilter": gdsStoredKinds}, columns: []string{"node1", "node2", "shared"}, rendering: gdsRenderProjection},
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

// gdsNodeOutput is the value a projection stream's node id leaves its row
// as, produced once when the row is built: the projection's (an
// aggregation row's INTEGER, else the stored string id; nodeIDValue), or the
// id as read for a stream over the stored graph (projection nil). The
// projection is the one the stream computed from, so dropping or replacing
// the graph's name meanwhile changes nothing.
func gdsNodeOutput(projection *GraphProjection, id string) interface{} {
	if projection == nil {
		return id
	}
	return projection.nodeIDValue(id)
}

// gdsNodeOutputs is gdsNodeOutput for each map's nodeId: the shared lists
// the node-similarity kernel builds fresh for each pair.
func gdsNodeOutputs(projection *GraphProjection, shared []interface{}) []interface{} {
	for _, item := range shared {
		if entry, ok := item.(map[string]interface{}); ok {
			if id, ok := entry["nodeId"].(string); ok {
				entry["nodeId"] = gdsNodeOutput(projection, id)
			}
		}
	}
	return shared
}

// gdsRowNodes are the nodes a rows procedure met, by key (gdsNodeRef.key),
// with the id each leaves a row as, recorded once when its input is read.
// A row's ids are read here; a key is never decoded.
type gdsRowNodes map[string]string

// add records ref and returns its key.
func (nodes gdsRowNodes) add(ref gdsNodeRef) string {
	key := ref.key()
	if _, seen := nodes[key]; !seen {
		nodes[key] = ref.id
	}
	return key
}

// before orders two keys by the ids they leave rows as, then by key: a
// local and a remote node can have the same id.
func (nodes gdsRowNodes) before(a, b string) bool {
	if idA, idB := nodes[a], nodes[b]; idA != idB {
		return idA < idB
	}
	return a < b
}
