package storage

import (
	"bytes"
	"encoding/binary"
)

// physicalCacheProjection describes only key-family effects. It never loads an
// entity or visits property/embedding payloads. Logical metadata is produced by
// appendOperationPublicationKeys; this projection adds raw maintenance aliases.
type physicalCacheProjection struct {
	facts   []AffectedFact
	unknown bool
}

func (p *physicalCacheProjection) VisitAffectedFacts(visit func(AffectedFact)) {
	for _, fact := range p.facts {
		visit(fact)
	}
}

func (p *physicalCacheProjection) scope() cacheWriteScope {
	return cacheWriteScope{known: !p.unknown, facts: p}
}

func (p *physicalCacheProjection) add(b *BadgerEngine, key, value []byte) {
	if b == nil || len(key) == 0 {
		return
	}
	fact := AffectedFact{}
	switch key[0] {
	case prefixNode:
		fact.NodeID = NodeID(string(key[1:]))
	case prefixPendingEmbed:
		// Queue membership does not change any stored body or embedding range.
		return
	case prefixEmbedding:
		separator := bytes.IndexByte(key[1:], 0)
		if separator < 0 {
			p.unknown = true
			return
		}
		fact.NodeID = NodeID(string(key[1 : 1+separator]))
	case prefixEdge:
		fact.EdgeID = EdgeID(string(key[1:]))
		if len(value) > 0 {
			var header Edge
			start, end, _, err := decodeEdgeCompactHeaderInto(value, edgeFormatCompactV2, &header)
			if err != nil {
				p.unknown = true
				return
			}
			fact.Type = header.Type
			fact.StartNode, _ = b.idDict.lookupNodeIDByNum(start)
			fact.EndNode, _ = b.idDict.lookupNodeIDByNum(end)
		}
	case prefixOutgoingIndex, prefixIncomingIndex, prefixMVCCOutgoingAdj, prefixMVCCIncomingAdj:
		if len(key) < 17 {
			p.unknown = true
			return
		}
		fact.EdgeID, _ = b.idDict.lookupEdgeIDByNum(binary.BigEndian.Uint64(key[9:17]))
		endpoint, _ := b.idDict.lookupNodeIDByNum(binary.BigEndian.Uint64(key[1:9]))
		if key[0] == prefixOutgoingIndex || key[0] == prefixMVCCOutgoingAdj {
			fact.StartNode = endpoint
		} else {
			fact.EndNode = endpoint
		}
		if fact.EdgeID == "" {
			p.unknown = true
			return
		}
	case prefixEdgeTypeIndex:
		if len(key) < 10 || key[len(key)-9] != 0 {
			p.unknown = true
			return
		}
		fact.Type = string(key[1 : len(key)-9])
		fact.EdgeID, _ = b.idDict.lookupEdgeIDByNum(binary.BigEndian.Uint64(key[len(key)-8:]))
		if fact.EdgeID == "" {
			p.unknown = true
			return
		}
	case prefixLabelIndex:
		if len(key) < 10 || key[len(key)-9] != 0 {
			p.unknown = true
			return
		}
		fact.Label = string(key[1 : len(key)-9])
		fact.NodeID, _ = b.idDict.lookupNodeIDByNum(binary.BigEndian.Uint64(key[len(key)-8:]))
		if fact.NodeID == "" {
			p.unknown = true
			return
		}
	case prefixSchema, prefixMVCCMeta, prefixBufferedFlush, prefixChangeEvent,
		prefixIDDictNodeForward, prefixIDDictEdgeForward, prefixIDDictCounter,
		prefixIDDictNodeReverse, prefixIDDictEdgeReverse, prefixIDFreelist,
		prefixPropKeyForward, prefixPropKeyReverse, prefixPropKeyCounter,
		prefixMVCCNode, prefixMVCCEdge, prefixMVCCNodeHead, prefixMVCCEdgeHead,
		prefixMVCCPruneFloor, prefixTemporalIndex, prefixTemporalHead,
		prefixEdgeBetweenIndex, prefixEdgeBetweenHead:
		// These keys do not install bodies in the caches governed here. Their
		// query/index readers retain the release's own validation and views.
		return
	default:
		p.unknown = true
		return
	}
	p.facts = append(p.facts, fact)
}
