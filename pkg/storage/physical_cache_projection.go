package storage

import (
	"bytes"
	"encoding/binary"
)

// physicalCacheProjection describes only key-family effects. It never loads an
// entity or visits property/embedding payloads. Logical metadata is produced by
// appendOperationPublicationKeys; this projection adds raw maintenance aliases.
type physicalCacheProjection struct {
	facts   []projectedCacheFact
	unknown bool
}

// projectedCacheFact retains only the fields a physical key can name. Node
// facts use id and labelOrStart; edge facts use id, labelOrStart, end and typ.
// One ordered slice keeps small commits to one allocation while avoiding the
// unused fields and flags in the wider public AffectedFact representation.
// Strings are owned/resolved when add runs, as before; emission never reloads IDs.
type projectedCacheFact struct {
	id           string
	labelOrStart string
	end          NodeID
	typ          string
	node         bool
}

func (p *physicalCacheProjection) VisitAffectedFacts(visit func(AffectedFact)) {
	for _, fact := range p.facts {
		if fact.node {
			visit(AffectedFact{NodeID: NodeID(fact.id), Label: fact.labelOrStart})
		} else {
			visit(AffectedFact{EdgeID: EdgeID(fact.id), StartNode: NodeID(fact.labelOrStart), EndNode: fact.end, Type: fact.typ})
		}
	}
}

func (p *physicalCacheProjection) scope() cacheWriteScope {
	return cacheWriteScope{known: !p.unknown, facts: p}
}

func (p *physicalCacheProjection) add(b *BadgerEngine, key, value []byte) {
	if b == nil || len(key) == 0 {
		return
	}
	fact := projectedCacheFact{}
	switch key[0] {
	case prefixNode:
		fact.id, fact.node = string(key[1:]), true
	case prefixPendingEmbed:
		// Queue membership does not change any stored body or embedding range.
		return
	case prefixEmbedding:
		separator := bytes.IndexByte(key[1:], 0)
		if separator < 0 {
			p.unknown = true
			return
		}
		fact.id, fact.node = string(key[1:1+separator]), true
	case prefixEdge:
		fact.id = string(key[1:])
		if len(value) > 0 {
			var header Edge
			start, end, _, err := decodeEdgeCompactHeaderInto(value, edgeFormatCompactV2, &header)
			if err != nil {
				p.unknown = true
				return
			}
			fact.typ = header.Type
			startID, _ := b.idDict.lookupNodeIDByNum(start)
			fact.labelOrStart = string(startID)
			fact.end, _ = b.idDict.lookupNodeIDByNum(end)
		}
	case prefixOutgoingIndex, prefixIncomingIndex, prefixMVCCOutgoingAdj, prefixMVCCIncomingAdj:
		if len(key) < 17 {
			p.unknown = true
			return
		}
		edgeID, _ := b.idDict.lookupEdgeIDByNum(binary.BigEndian.Uint64(key[9:17]))
		fact.id = string(edgeID)
		endpoint, _ := b.idDict.lookupNodeIDByNum(binary.BigEndian.Uint64(key[1:9]))
		if key[0] == prefixOutgoingIndex || key[0] == prefixMVCCOutgoingAdj {
			fact.labelOrStart = string(endpoint)
		} else {
			fact.end = endpoint
		}
		if fact.id == "" {
			p.unknown = true
			return
		}
	case prefixEdgeTypeIndex:
		if len(key) < 10 || key[len(key)-9] != 0 {
			p.unknown = true
			return
		}
		fact.typ = string(key[1 : len(key)-9])
		edgeID, _ := b.idDict.lookupEdgeIDByNum(binary.BigEndian.Uint64(key[len(key)-8:]))
		fact.id = string(edgeID)
		if fact.id == "" {
			p.unknown = true
			return
		}
	case prefixLabelIndex:
		if len(key) < 10 || key[len(key)-9] != 0 {
			p.unknown = true
			return
		}
		fact.labelOrStart, fact.node = string(key[1:len(key)-9]), true
		nodeID, _ := b.idDict.lookupNodeIDByNum(binary.BigEndian.Uint64(key[len(key)-8:]))
		fact.id = string(nodeID)
		if fact.id == "" {
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
