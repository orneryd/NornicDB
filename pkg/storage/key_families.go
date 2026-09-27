package storage

import (
	"bytes"
	"encoding/binary"
)

// badgerKeyFamilies is the inventory for storage accounting and namespace cleanup.
// Global families remain accounted for but are not removed by a namespace drop.
var badgerKeyFamilies = []struct {
	prefix byte
	kind   string
}{
	{prefixNode, "nodes"}, {prefixEdge, "edges"},
	{prefixLabelIndex, "index"}, {prefixOutgoingIndex, "index"},
	{prefixIncomingIndex, "index"}, {prefixEdgeTypeIndex, "index"},
	{prefixPendingEmbed, "index"}, {prefixEmbedding, "index"},
	{prefixSchema, "metadata"}, {prefixTemporalIndex, "index"},
	{prefixTemporalHead, "index"}, {prefixMVCCNode, "mvcc"},
	{prefixMVCCEdge, "mvcc"}, {prefixMVCCNodeHead, "mvcc"},
	{prefixMVCCEdgeHead, "mvcc"}, {prefixMVCCMeta, "metadata"},
	{prefixAccessMeta, "metadata"}, {prefixIndexEntryCatalog, "index"},
	{prefixDeindexWorkItem, "metadata"}, {prefixDecayProfile, "metadata"},
	{prefixPromotionProfile, "metadata"}, {prefixPromotionPolicy, "metadata"},
	{prefixIndexTombstone, "index"}, {prefixEdgeBetweenIndex, "index"},
	{prefixEdgeBetweenHead, "index"}, {prefixIDDictNodeForward, "metadata"},
	{prefixIDDictEdgeForward, "metadata"}, {prefixIDDictCounter, "metadata"},
	{prefixIDDictNodeReverse, "metadata"}, {prefixIDDictEdgeReverse, "metadata"},
	{prefixIDFreelist, "metadata"}, {prefixPropKeyForward, "metadata"},
	{prefixPropKeyReverse, "metadata"}, {prefixPropKeyCounter, "metadata"},
	{prefixMVCCOutgoingAdj, "mvcc"}, {prefixMVCCIncomingAdj, "mvcc"},
	{prefixMVCCPruneFloor, "mvcc"},
}

func namespaceOwnsBadgerKey(key []byte, prefix []byte, namespace string, wholeNamespace bool, nodes, edges map[uint64]struct{}) bool {
	if len(key) < 2 {
		return false
	}
	hasNode := func(offset int) bool {
		if offset < 1 || len(key) < offset+8 {
			return false
		}
		_, ok := nodes[binary.BigEndian.Uint64(key[offset:])]
		return ok
	}
	hasEdge := func(offset int) bool {
		if offset < 1 || len(key) < offset+8 {
			return false
		}
		_, ok := edges[binary.BigEndian.Uint64(key[offset:])]
		return ok
	}
	switch key[0] {
	case prefixNode, prefixEdge, prefixPendingEmbed, prefixEmbedding,
		prefixAccessMeta, prefixIndexEntryCatalog, prefixDeindexWorkItem:
		return bytes.HasPrefix(key[1:], prefix)
	case prefixIDDictNodeForward, prefixIDDictEdgeForward:
		return bytes.HasPrefix(key[1:], prefix)
	case prefixIDDictNodeReverse, prefixMVCCNode, prefixMVCCNodeHead,
		prefixMVCCOutgoingAdj, prefixMVCCIncomingAdj:
		return hasNode(1)
	case prefixIDDictEdgeReverse, prefixMVCCEdge, prefixMVCCEdgeHead:
		return hasEdge(1)
	case prefixMVCCPruneFloor:
		return len(key) == 10 && ((key[1] == prefixMVCCNode && hasNode(2)) || (key[1] == prefixMVCCEdge && hasEdge(2)))
	case prefixLabelIndex:
		return len(key) >= 10 && key[len(key)-9] == 0 && hasNode(len(key)-8)
	case prefixEdgeTypeIndex:
		return len(key) >= 10 && key[len(key)-9] == 0 && hasEdge(len(key)-8)
	case prefixOutgoingIndex, prefixIncomingIndex:
		return hasNode(1) || hasEdge(9)
	case prefixEdgeBetweenIndex:
		return hasNode(1) || hasNode(9) || hasEdge(len(key)-8)
	case prefixEdgeBetweenHead:
		return hasNode(1) || hasNode(9)
	case prefixIndexTombstone:
		return namespaceOwnsBadgerKey(key[1:], prefix, namespace, wholeNamespace, nodes, edges)
	case prefixTemporalIndex, prefixTemporalHead:
		return wholeNamespace && bytes.HasPrefix(key[1:], append([]byte(namespace), 0))
	case prefixMVCCMeta:
		return wholeNamespace && len(key) >= 2 && key[1] == prefixMVCCMetaNamespaceSeq && string(key[2:]) == namespace
	case prefixPropKeyForward, prefixPropKeyReverse, prefixPropKeyCounter:
		if !wholeNamespace {
			return false
		}
		length, used := binary.Uvarint(key[1:])
		return used > 0 && length == uint64(len(namespace)) && len(key) >= 1+used+len(namespace) && string(key[1+used:1+used+len(namespace)]) == namespace
	default:
		return false
	}
}
