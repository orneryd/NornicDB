package storage

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// A historical writer serialized WALBufferedCommitData as a plain JSON object,
// and encoding/json replaces every invalid UTF-8 byte inside a map key with
// U+FFFD. Derived physical keys embed big-endian numeric IDs, so a key naming an
// ID with a byte in 0x80..0xFF lost those bytes, and two IDs whose bytes differ
// only there collapse into one decoded key that keeps one of their entries.
//
// This file is the one owner of the V4 upgrade's response to that damage. A
// buffered commit whose source encoding replaced physical key bytes is repaired
// from the facts the record itself still names exactly, or refused as a whole.
// Nothing durable is consulted: an unapplied commit's entities exist only inside
// its own record, and a durable flush receipt says the commit reached Badger, not
// that the record became replayable.
//
// Exactly the derived families are re-derivable, because each of their entries is
// a pure function of record-local facts:
//
//	[0x03][label][0x00][node numID]                      node body labels + binding
//	[0x04][start numID][edge numID]                      edge header + binding
//	[0x05][end numID][edge numID]                        edge header + binding
//	[0x06][type][0x00][edge numID]                       edge header + binding
//	[0x11][start][end][type][0x00][edge numID]           edge header + binding
//	[0x12][start][end][type] -> relationship ID          edge header + binding
//	[0x1D][node numID] -> node ID                        node binding
//	[0x1E][edge numID] -> relationship ID                edge binding
//
// The record's Ops carry property values the same lossy encoder rewrote, so they
// are never read here: a lost value cannot become authority by being decoded.

// errHistoricalAmbiguousPhysicalKey refuses a buffered commit whose damaged
// physical keys its own surviving facts do not determine completely.
var errHistoricalAmbiguousPhysicalKey = errors.New("wal: historical physical key contains ambiguous UTF-8 replacement")

// historicalDerivedKeyFamilies lists every Badger key family the upgrade can
// reconstruct, with the name used in refusal diagnostics.
var historicalDerivedKeyFamilies = []struct {
	prefix byte
	name   string
}{
	{prefixLabelIndex, "label index"},
	{prefixOutgoingIndex, "outgoing adjacency"},
	{prefixIncomingIndex, "incoming adjacency"},
	{prefixEdgeTypeIndex, "relationship type index"},
	{prefixEdgeBetweenIndex, "relationship between set"},
	{prefixEdgeBetweenHead, "relationship between head"},
	{prefixIDDictNodeReverse, "node reverse dictionary"},
	{prefixIDDictEdgeReverse, "relationship reverse dictionary"},
}

// historicalDerivedKeyFamily classifies one physical key by its family byte,
// reporting the family only when the record's own facts can re-derive its
// entries exactly. Every other family, including one the writer does not know,
// has no reconstruction evidence.
func historicalDerivedKeyFamily(key []byte) (string, bool) {
	if len(key) == 0 {
		return "", false
	}
	for _, family := range historicalDerivedKeyFamilies {
		if family.prefix == key[0] {
			return family.name, true
		}
	}
	return "", false
}

// historicalPhysicalKeyFamilyName names the key family of prefix for refusal
// diagnostics, using the storage inventory so an unclassified family reads as
// such instead of borrowing a re-derivable family's name.
func historicalPhysicalKeyFamilyName(prefix byte) string {
	if name, ok := historicalDerivedKeyFamily([]byte{prefix}); ok {
		return name
	}
	if walPhysicalKeyFamilies[prefix] {
		for _, family := range badgerKeyFamilies {
			if family.prefix == prefix {
				return fmt.Sprintf("known non-derived family 0x%02x (%s)", prefix, family.kind)
			}
		}
		return fmt.Sprintf("known non-derived family 0x%02x", prefix)
	}
	return fmt.Sprintf("unclassified family 0x%02x", prefix)
}

// historicalPhysicalKeyAmbiguous reports whether an old JSON map encoding
// replaced bytes inside key, which is exactly the U+FFFD the reader sees.
func historicalPhysicalKeyAmbiguous(key string) bool {
	return strings.ContainsRune(key, '\ufffd')
}

// historicalPhysicalKeysAmbiguous reports whether any physical key of one
// record lost bytes to the encoding that wrote it.
func historicalPhysicalKeysAmbiguous(writes map[string][]byte, deletes []string) bool {
	for key := range writes {
		if historicalPhysicalKeyAmbiguous(key) {
			return true
		}
	}
	for _, key := range deletes {
		if historicalPhysicalKeyAmbiguous(key) {
			return true
		}
	}
	return false
}

// historicalPhysicalKeyJSONForm returns the key the old JSON map encoding decodes
// to. encoding/json writes one U+FFFD per invalid UTF-8 byte of a map key, so a
// key whose form differs from itself no longer names its own entry, and two keys
// with one form collapse into a single map entry. Mapping every rune through its
// own replacement reproduces that loss byte for byte.
func historicalPhysicalKeyJSONForm(key string) string {
	if utf8.ValidString(key) {
		return key
	}
	return strings.Map(func(r rune) rune {
		if r == utf8.RuneError {
			return '\ufffd'
		}
		return r
	}, key)
}

// historicalPhysicalKeySurvivesJSON reports whether the old JSON map encoding
// kept every byte of key, i.e. whether the record still names that entry.
func historicalPhysicalKeySurvivesJSON(key string) bool {
	return historicalPhysicalKeyJSONForm(key) == key
}

// historicalDerivedKeyFacts are the record-local facts that determine the
// derived entries of one buffered commit: the labels and relationship headers its
// entity bodies hold, and the forward numeric bindings it writes.
type historicalDerivedKeyFacts struct {
	labels   map[NodeID][]string
	nodeNums map[NodeID]uint64
	headers  map[EdgeID][]byte
	edges    map[EdgeID]historicalDerivedEdgeFacts
	edgeNums map[EdgeID]uint64
}

// historicalDerivedEdgeFacts is one relationship's compact header as its body
// holds it: both endpoints and its type, all keyed by numeric IDs.
type historicalDerivedEdgeFacts struct {
	startNum uint64
	endNum   uint64
	edgeType string
}

func newHistoricalDerivedKeyFacts() historicalDerivedKeyFacts {
	return historicalDerivedKeyFacts{
		labels:   map[NodeID][]string{},
		nodeNums: map[NodeID]uint64{},
		headers:  map[EdgeID][]byte{},
		edges:    map[EdgeID]historicalDerivedEdgeFacts{},
		edgeNums: map[EdgeID]uint64{},
	}
}

// historicalDerivedKeyFactsFrom reads every authoritative fact a record's writes
// and deletions still name exactly, with the reasons any body or binding it
// cannot read provides instead. It decodes node labels through the stored-body
// owner the label index rebuild uses and relationship endpoints and type through
// the compact header owner, and it never reads a damaged key.
func historicalDerivedKeyFactsFrom(writes map[string][]byte, deletes []string) (historicalDerivedKeyFacts, []string) {
	facts := newHistoricalDerivedKeyFacts()
	// A deleted body is this commit's authoritative statement that the entity no
	// longer exists, so no fact may be completed for it afterwards.
	removed := make(map[string]bool, len(deletes))
	for _, key := range deletes {
		removed[key] = true
	}
	var refused []string
	for key, value := range writes {
		raw := []byte(key)
		if len(raw) == 0 || removed[key] {
			continue
		}
		switch raw[0] {
		case prefixNode:
			labels, err := decodeStoredNodeLabels(value)
			if err != nil {
				refused = append(refused, fmt.Sprintf("node body %q is not decodable: %v", key[1:], err))
				continue
			}
			facts.labels[NodeID(key[1:])] = labels
		case prefixEdge:
			var edge Edge
			startNum, endNum, headerEnd, err := decodeEdgeCompactHeaderInto(value, edgeFormatCompactV2, &edge)
			if err != nil {
				refused = append(refused, fmt.Sprintf("relationship body %q has no compact header: %v", key[1:], err))
				continue
			}
			facts.headers[EdgeID(key[1:])] = append([]byte(nil), value[:headerEnd]...)
			facts.edges[EdgeID(key[1:])] = historicalDerivedEdgeFacts{startNum: startNum, endNum: endNum, edgeType: edge.Type}
		case prefixIDDictNodeForward:
			num, ok := decodeNodeRefNum(value)
			if !ok {
				refused = append(refused, fmt.Sprintf("node numeric binding %q is malformed", key[1:]))
				continue
			}
			facts.nodeNums[NodeID(key[1:])] = num
		case prefixIDDictEdgeForward:
			num, ok := decodeNodeRefNum(value)
			if !ok {
				refused = append(refused, fmt.Sprintf("relationship numeric binding %q is malformed", key[1:]))
				continue
			}
			facts.edgeNums[EdgeID(key[1:])] = num
		}
	}
	return facts, refused
}

// historicalDerivedKeyCandidates re-derives every derived entry the facts
// determine, using the production key builders so the bytes are the bytes the
// write path emits. A relationship sharing one between-head key with another of
// different identity leaves no fact that says which value the record wrote, so
// that key is reported as a conflict instead.
func historicalDerivedKeyCandidates(facts historicalDerivedKeyFacts) (map[string][]byte, map[string][]EdgeID) {
	candidates := make(map[string][]byte, len(facts.labels))
	for id, labels := range facts.labels {
		num, ok := facts.nodeNums[id]
		if !ok {
			continue
		}
		for _, label := range labels {
			candidates[string(labelIndexKey(label, num))] = []byte{}
		}
		candidates[string(append([]byte{prefixIDDictNodeReverse}, encodeNumID(num)...))] = []byte(id)
	}
	heads := make(map[string]EdgeID, len(facts.edges))
	conflicts := map[string][]EdgeID{}
	for id, edge := range facts.edges {
		num, ok := facts.edgeNums[id]
		if !ok {
			continue
		}
		adjacency := adjacencyValueFromRecord(facts.headers[id])
		candidates[string(outgoingIndexKey(edge.startNum, num))] = adjacency
		candidates[string(incomingIndexKey(edge.endNum, num))] = adjacency
		candidates[string(edgeTypeIndexKey(edge.edgeType, num))] = []byte{}
		candidates[string(edgeBetweenIndexKey(edge.startNum, edge.endNum, edge.edgeType, num))] = []byte(id)
		candidates[string(append([]byte{prefixIDDictEdgeReverse}, encodeNumID(num)...))] = []byte(id)
		head := string(edgeBetweenHeadKey(edge.startNum, edge.endNum, edge.edgeType))
		if other, seen := heads[head]; seen && other != id {
			if other < id {
				conflicts[head] = []EdgeID{other, id}
			} else {
				conflicts[head] = []EdgeID{id, other}
			}
			continue
		}
		heads[head] = id
		candidates[head] = []byte(id)
	}
	return candidates, conflicts
}

// historicalDerivedKeyProofComplete reports the entity bodies whose numeric
// binding this record does not carry. A damaged key keeps only the structure of
// its numeric IDs, so any entity body the record writes could be the one that key
// belongs to: without its binding in the record, no derivation can prove it
// restored every entry the record wrote, and restoring a subset would apply an
// incomplete commit.
func historicalDerivedKeyProofComplete(facts historicalDerivedKeyFacts, damaged map[string]bool) []string {
	needsNodes, needsEdges := false, false
	for key := range damaged {
		switch []byte(key)[0] {
		case prefixLabelIndex, prefixIDDictNodeReverse:
			needsNodes = true
		case prefixOutgoingIndex, prefixIncomingIndex, prefixEdgeTypeIndex,
			prefixEdgeBetweenIndex, prefixEdgeBetweenHead, prefixIDDictEdgeReverse:
			needsEdges = true
		}
	}
	var refused []string
	if needsNodes {
		// A surviving collision member cannot prove that its peers are complete.
		// Forward bindings survive JSON independently of the bodies. Every such
		// binding must therefore have its body before labels can be re-derived;
		// otherwise a merged-away entry could disappear without leaving another
		// damaged string that the coverage check below can detect.
		for id := range facts.nodeNums {
			if _, ok := facts.labels[id]; !ok {
				refused = append(refused, fmt.Sprintf("node %q has no body in this record, so its collision-class entries cannot be proven complete", id))
			}
		}
		for id := range facts.labels {
			if _, ok := facts.nodeNums[id]; !ok {
				refused = append(refused, fmt.Sprintf(
					"node %q has no numeric binding in this record, so the derived entries that name its numeric ID cannot be proven complete", id))
			}
		}
	}
	if needsEdges {
		for id := range facts.edgeNums {
			if _, ok := facts.edges[id]; !ok {
				refused = append(refused, fmt.Sprintf("relationship %q has no body in this record, so its collision-class entries cannot be proven complete", id))
			}
		}
		for id := range facts.edges {
			if _, ok := facts.edgeNums[id]; !ok {
				refused = append(refused, fmt.Sprintf(
					"relationship %q has no numeric binding in this record, so the derived entries that name its numeric ID cannot be proven complete", id))
			}
		}
	}
	return refused
}

// historicalAmbiguousKeyError reports every reason one record cannot be repaired.
func historicalAmbiguousKeyError(reasons []string) error {
	return fmt.Errorf("%w: %s", errHistoricalAmbiguousPhysicalKey, strings.Join(reasons, "; "))
}

// repairV4BufferedCommitPhysicalKeys restores every derived physical entry one
// legacy buffered commit's encoding destroyed, from that record's own facts, or
// refuses the whole record with every reason it cannot.
//
// It restores an entry only when the old encoder would have rewritten its bytes
// and the record names the damaged key it decodes to, so it never invents an
// index entry the commit did not write. Because the old map encoding merges all
// entries of one form into one, a whole collision class is restored from the
// facts that name its members; a class the facts do not name completely refuses
// the record instead of keeping a surviving entry.
//
// The returned record carries the physical-property-image marker, because the
// repaired bytes are written with the lossless typed encoding while the Ops
// property values it also carries are still the lossy ones the old encoder wrote.
func repairV4BufferedCommitPhysicalKeys(data WALBufferedCommitData) (WALBufferedCommitData, error) {
	damaged := make(map[string]bool)
	for key := range data.Writes {
		if historicalPhysicalKeyAmbiguous(key) {
			damaged[key] = true
		}
	}
	var reasons []string
	for _, key := range data.Deletes {
		if historicalPhysicalKeyAmbiguous(key) {
			reasons = append(reasons, fmt.Sprintf(
				"deleted physical key %q lost its bytes and the record holds no old entity state that would justify the removal", key))
		}
	}
	if len(damaged) != 0 && len(data.NumericBindings) != 0 {
		reasons = append(reasons, "the record carries cross-store numeric bindings, so its numeric IDs are not this store's dictionary")
	}
	for key := range damaged {
		if _, ok := historicalDerivedKeyFamily([]byte(key)); !ok {
			reasons = append(reasons, fmt.Sprintf("physical key %q is not in a re-derivable numeric-key family (%s)",
				key, historicalPhysicalKeyFamilyName([]byte(key)[0])))
		}
	}
	if len(reasons) != 0 {
		return WALBufferedCommitData{}, historicalAmbiguousKeyError(reasons)
	}
	if len(damaged) == 0 {
		return data, nil
	}

	facts, refused := historicalDerivedKeyFactsFrom(data.Writes, data.Deletes)
	reasons = append(reasons, refused...)
	reasons = append(reasons, historicalDerivedKeyProofComplete(facts, damaged)...)
	candidates, headConflicts := historicalDerivedKeyCandidates(facts)
	for head, ids := range headConflicts {
		// Only a head key this record damaged needs restoring; a conflict on an
		// entry it never damaged belongs to another record.
		if !damaged[historicalPhysicalKeyJSONForm(head)] {
			continue
		}
		reasons = append(reasons, fmt.Sprintf(
			"relationships %q and %q share one between-head key, whose value is a write-order artifact", ids[0], ids[1]))
	}
	if len(reasons) != 0 {
		return WALBufferedCommitData{}, historicalAmbiguousKeyError(reasons)
	}

	recovered := make(map[string][]byte, len(damaged))
	for key, value := range candidates {
		if historicalPhysicalKeySurvivesJSON(key) {
			continue
		}
		if !damaged[historicalPhysicalKeyJSONForm(key)] {
			continue
		}
		recovered[key] = value
	}
	named := make(map[string]bool, len(recovered))
	for key := range recovered {
		named[historicalPhysicalKeyJSONForm(key)] = true
	}
	for key := range damaged {
		if !named[key] {
			reasons = append(reasons, fmt.Sprintf("no surviving fact determines the damaged physical key %q", key))
		}
	}
	if len(reasons) != 0 {
		return WALBufferedCommitData{}, historicalAmbiguousKeyError(reasons)
	}

	repaired := data
	repaired.Writes = make(map[string][]byte, len(data.Writes)+len(recovered))
	for key, value := range data.Writes {
		if damaged[key] {
			continue
		}
		repaired.Writes[key] = value
	}
	for key, value := range recovered {
		repaired.Writes[key] = value
	}
	repaired.PhysicalPropertyImages = true
	// The repaired record is written with the lossless encoding, and its named
	// property images stay historical: they were never authoritative.
	repaired.losslessPhysicalKeys = true
	repaired.namedPropertiesAuthoritative = false
	return repaired, nil
}
