package storage

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// Numeric IDs are local to one Badger store. A buffered WAL record carries
// bindings for the IDs in its physical writes so a foreign replay can resolve
// them in the destination's dictionary instead of installing source IDs.
const (
	walNumericNode byte = 'n'
	walNumericEdge byte = 'e'
	walNumericKey  byte = 'k'
	walNumericBody byte = 'v'
)

type walNumericToken struct {
	kind byte
	id   uint64
}

// The storage-family inventory is also the admission boundary for physical
// buffered WAL records. Flush markers are global and deliberately excluded
// from the namespace-cleanup inventory, but may appear in recovery records.
var walPhysicalKeyFamilies = func() [256]bool {
	var known [256]bool
	for _, family := range badgerKeyFamilies {
		known[family.prefix] = true
	}
	known[prefixBufferedFlush] = true
	return known
}()

// visitWALNumericFields identifies numeric IDs in current physical index keys
// and compact relationship bodies. The callback receives the component's
// kind, its location (key or value), its byte offset and source ID.
func visitWALNumericFields(key string, value []byte, visit func(byte, byte, int, uint64) error) error {
	if len(key) == 0 {
		return fmt.Errorf("wal: empty buffered key")
	}
	if !walPhysicalKeyFamilies[key[0]] {
		return fmt.Errorf("wal: unsupported physical key family %#x", key[0])
	}
	field := func(kind, location byte, offset int) error {
		if location == walNumericKey {
			if offset < 0 || offset+8 > len(key) {
				return fmt.Errorf("wal: malformed numeric key family %x", key[0])
			}
			return visit(kind, location, offset, binary.BigEndian.Uint64([]byte(key[offset:offset+8])))
		}
		if offset < 0 || offset+8 > len(value) {
			return fmt.Errorf("wal: malformed numeric value for family %x", key[0])
		}
		return visit(kind, location, offset, binary.BigEndian.Uint64(value[offset:offset+8]))
	}
	edgeBody := func() error {
		if len(value) == 0 {
			return nil
		}
		if value[0] != edgeFormatCompactV2 || len(value) < 18 {
			return fmt.Errorf("wal: malformed compact relationship value")
		}
		if err := field(walNumericNode, walNumericBody, 2); err != nil {
			return err
		}
		return field(walNumericNode, walNumericBody, 10)
	}
	switch key[0] {
	case prefixMVCCMeta:
		// Policy tombstones embed an index key after their textual prefix.
		// The stored metadata value does not contain that index's IDs.
		prefix := string(accessMetaKey(policyIndexMetaIDPrefix))
		if strings.HasPrefix(key, prefix) {
			return visitWALNumericFields(key[len(prefix):], nil, func(kind, location byte, offset int, id uint64) error {
				if location == walNumericKey {
					offset += len(prefix)
				}
				return visit(kind, location, offset, id)
			})
		}
	case prefixIDDictNodeForward, prefixIDDictEdgeForward:
		if len(value) == 0 { // delete: the logical name is already in the key
			return nil
		}
		kind := walNumericNode
		if key[0] == prefixIDDictEdgeForward {
			kind = walNumericEdge
		}
		if len(value) != 8 {
			return fmt.Errorf("wal: malformed numeric dictionary value")
		}
		return field(kind, walNumericBody, 0)
	case prefixLabelIndex, prefixEdgeTypeIndex:
		if len(key) < 10 || key[len(key)-9] != 0 {
			return fmt.Errorf("wal: malformed named numeric index")
		}
		kind := walNumericNode
		if key[0] == prefixEdgeTypeIndex {
			kind = walNumericEdge
		}
		return field(kind, walNumericKey, len(key)-8)
	case prefixOutgoingIndex, prefixIncomingIndex:
		if len(key) != 17 {
			return fmt.Errorf("wal: malformed adjacency index")
		}
		if err := field(walNumericNode, walNumericKey, 1); err != nil {
			return err
		}
		if err := field(walNumericEdge, walNumericKey, 9); err != nil {
			return err
		}
		return edgeBody()
	case prefixEdgeBetweenIndex, prefixEdgeBetweenHead:
		minimum := 17
		if key[0] == prefixEdgeBetweenIndex {
			minimum = 26
		}
		if len(key) < minimum {
			return fmt.Errorf("wal: malformed between index")
		}
		if err := field(walNumericNode, walNumericKey, 1); err != nil {
			return err
		}
		if err := field(walNumericNode, walNumericKey, 9); err != nil {
			return err
		}
		if key[0] == prefixEdgeBetweenIndex {
			return field(walNumericEdge, walNumericKey, len(key)-8)
		}
	case prefixEdge:
		return edgeBody()
	case prefixMVCCNode, prefixMVCCNodeHead:
		return field(walNumericNode, walNumericKey, 1)
	case prefixMVCCEdge, prefixMVCCEdgeHead:
		return field(walNumericEdge, walNumericKey, 1)
	case prefixMVCCOutgoingAdj, prefixMVCCIncomingAdj:
		if err := field(walNumericNode, walNumericKey, 1); err != nil {
			return err
		}
		return field(walNumericEdge, walNumericKey, 9)
	case prefixMVCCPruneFloor:
		if len(key) != 10 {
			return fmt.Errorf("wal: malformed MVCC prune key")
		}
		switch key[1] {
		case prefixMVCCNode:
			return field(walNumericNode, walNumericKey, 2)
		case prefixMVCCEdge:
			return field(walNumericEdge, walNumericKey, 2)
		default:
			return fmt.Errorf("wal: unknown MVCC prune key family")
		}
	}
	return nil
}

// mergeWALNumericSourceEntries adds paired dictionary writes to the source
// identity table used only while replaying an original store's older WAL.
// A later record may refer to an ID whose allocating commit was still buffered.
func mergeWALNumericSourceEntries(writes map[string][]byte, aliases map[walNumericToken]string) error {
	staged := make(map[walNumericToken]string)
	for key, value := range writes {
		if len(key) == 0 {
			continue
		}
		var kind, reverse byte
		switch key[0] {
		case prefixIDDictNodeForward:
			kind, reverse = walNumericNode, prefixIDDictNodeReverse
		case prefixIDDictEdgeForward:
			kind, reverse = walNumericEdge, prefixIDDictEdgeReverse
		default:
			continue
		}
		if len(key) == 1 || len(value) != 8 {
			return fmt.Errorf("wal: malformed historical numeric dictionary entry")
		}
		num := binary.BigEndian.Uint64(value)
		if num == 0 {
			return fmt.Errorf("wal: invalid historical zero numeric ID")
		}
		var reverseKey [9]byte
		reverseKey[0] = reverse
		binary.BigEndian.PutUint64(reverseKey[1:], num)
		name := key[1:]
		if reverseValue, ok := writes[string(reverseKey[:])]; !ok || string(reverseValue) != name {
			return fmt.Errorf("wal: historical numeric dictionary pair is incomplete")
		}
		token := walNumericToken{kind: kind, id: num}
		if prior, ok := staged[token]; ok && prior != name {
			return fmt.Errorf("wal: conflicting historical numeric dictionary entries")
		}
		staged[token] = name
	}
	for key, value := range writes {
		if len(key) == 0 {
			continue
		}
		var kind byte
		switch key[0] {
		case prefixIDDictNodeReverse:
			kind = walNumericNode
		case prefixIDDictEdgeReverse:
			kind = walNumericEdge
		default:
			continue
		}
		if len(key) != 9 {
			return fmt.Errorf("wal: malformed historical reverse dictionary entry")
		}
		token := walNumericToken{kind: kind, id: binary.BigEndian.Uint64([]byte(key[1:]))}
		if token.id == 0 || staged[token] != string(value) {
			return fmt.Errorf("wal: historical numeric dictionary pair is incomplete")
		}
	}
	for token, name := range staged {
		aliases[token] = name
	}
	return nil
}

// captureWALNumericBindings resolves every referenced source number before the
// commit is acknowledged. Each distinct ID appears once in the compact table.
func captureWALNumericBindings(dictionary *idDictionary, writes map[string][]byte, deletes map[string]bool, aliases map[walNumericToken]string) ([]byte, error) {
	seen := make(map[walNumericToken]struct{})
	out := []byte{1}
	visit := func(kind, _ byte, _ int, id uint64) error {
		if id == 0 {
			return fmt.Errorf("wal: invalid zero numeric %c ID", kind)
		}
		token := walNumericToken{kind: kind, id: id}
		if _, ok := seen[token]; ok {
			return nil
		}
		var name string
		var found bool
		if len(aliases) != 0 {
			name, found = aliases[token]
		}
		if !found && kind == walNumericNode {
			var node NodeID
			node, found = dictionary.lookupNodeIDByNum(id)
			name = string(node)
		} else if !found {
			var edge EdgeID
			edge, found = dictionary.lookupEdgeIDByNum(id)
			name = string(edge)
		}
		if !found || name == "" {
			return fmt.Errorf("wal: missing source numeric %c ID %d", kind, id)
		}
		seen[token] = struct{}{}
		out = append(out, kind)
		out = binary.AppendUvarint(out, id)
		out = binary.AppendUvarint(out, uint64(len(name)))
		out = append(out, name...)
		return nil
	}
	if err := visitWALWriteSetNumericFields(writes, deletes, visit); err != nil {
		return nil, err
	}
	return out, nil
}

// visitWALWriteSetNumericFields checks each physical write and deletion in a
// buffered record through the same family and numeric-field owner.
func visitWALWriteSetNumericFields(writes map[string][]byte, deletes map[string]bool, visit func(byte, byte, int, uint64) error) error {
	for key, value := range writes {
		if err := visitWALNumericFields(key, value, visit); err != nil {
			return err
		}
	}
	for key := range deletes {
		if err := visitWALNumericFields(key, nil, visit); err != nil {
			return err
		}
	}
	return nil
}

func decodeWALNumericBindings(data []byte) (map[walNumericToken]string, error) {
	if len(data) == 0 || data[0] != 1 {
		return nil, fmt.Errorf("wal: missing or unsupported numeric ID bindings")
	}
	data = data[1:]
	bindings := make(map[walNumericToken]string)
	byName := make(map[struct {
		kind byte
		name string
	}]uint64)
	for len(data) > 0 {
		kind := data[0]
		data = data[1:]
		if kind != walNumericNode && kind != walNumericEdge {
			return nil, fmt.Errorf("wal: invalid numeric binding kind %x", kind)
		}
		id, n := binary.Uvarint(data)
		if n <= 0 || id == 0 {
			return nil, fmt.Errorf("wal: invalid numeric binding ID")
		}
		data = data[n:]
		length, n := binary.Uvarint(data)
		if n <= 0 {
			return nil, fmt.Errorf("wal: invalid numeric binding name length")
		}
		data = data[n:]
		if length == 0 || length > uint64(len(data)) {
			return nil, fmt.Errorf("wal: truncated numeric binding name")
		}
		name := string(data[:int(length)])
		data = data[int(length):]
		token := walNumericToken{kind: kind, id: id}
		if prior, exists := bindings[token]; exists && prior != name {
			return nil, fmt.Errorf("wal: conflicting numeric binding for %c ID %d", kind, id)
		}
		nameKey := struct {
			kind byte
			name string
		}{kind, name}
		if prior, exists := byName[nameKey]; exists && prior != id {
			return nil, fmt.Errorf("wal: conflicting numeric bindings for %c name %q", kind, name)
		}
		bindings[token], byName[nameKey] = name, id
	}
	return bindings, nil
}

// rewriteWALNumericFields copies only the bytes whose numeric IDs change.
// The original WAL map remains immutable, including on malformed input.
func rewriteWALNumericFields(key string, value []byte, bindings map[walNumericToken]uint64) (string, []byte, error) {
	var changedKey, changedValue []byte
	err := visitWALNumericFields(key, value, func(kind, location byte, offset int, id uint64) error {
		mapped, ok := bindings[walNumericToken{kind: kind, id: id}]
		if !ok || mapped == 0 {
			return fmt.Errorf("wal: missing numeric %c ID binding for %d", kind, id)
		}
		if mapped == id {
			return nil
		}
		if location == walNumericKey {
			if changedKey == nil {
				changedKey = []byte(key)
			}
			binary.BigEndian.PutUint64(changedKey[offset:offset+8], mapped)
		} else {
			if changedValue == nil {
				changedValue = append([]byte(nil), value...)
			}
			binary.BigEndian.PutUint64(changedValue[offset:offset+8], mapped)
		}
		return nil
	})
	if err != nil {
		return "", nil, err
	}
	if changedKey != nil {
		key = string(changedKey)
	}
	if changedValue != nil {
		value = changedValue
	}
	return key, value, nil
}

// rebindBufferedNumericIDs stages destination dictionary entries with the
// replayed write set. Source numbers, counters, and freelist entries are local
// to their originating store and must never be installed in another one.
// The complete source table is checked before any destination allocation.
func (b *BadgerEngine) rebindBufferedNumericIDs(txn *badger.Txn, c *bufferedCommit) (map[string][]byte, map[string]bool, error) {
	bindings, err := decodeWALNumericBindings(c.numericBindings)
	if err != nil {
		return nil, nil, err
	}
	identity := make(map[walNumericToken]uint64, len(bindings))
	tokens := make([]walNumericToken, 0, len(bindings))
	for token := range bindings {
		identity[token] = token.id
		tokens = append(tokens, token)
	}
	check := func(key string, value []byte, deleting bool) error {
		if len(key) == 0 {
			return fmt.Errorf("wal: empty buffered key")
		}
		switch key[0] {
		case prefixIDDictNodeForward, prefixIDDictEdgeForward, prefixIDDictNodeReverse, prefixIDDictEdgeReverse:
			if deleting {
				return fmt.Errorf("wal: unsupported numeric dictionary deletion")
			}
		case prefixIDFreelist:
			if !deleting {
				return fmt.Errorf("wal: unsupported numeric freelist write")
			}
			return nil
		case prefixIDDictCounter:
			return nil
		}
		_, _, err := rewriteWALNumericFields(key, value, identity)
		return err
	}
	for key, value := range c.writes {
		if err := check(key, value, false); err != nil {
			return nil, nil, err
		}
	}
	for key := range c.deletes {
		if err := check(key, nil, true); err != nil {
			return nil, nil, err
		}
	}
	sort.Slice(tokens, func(i, j int) bool {
		if tokens[i].kind != tokens[j].kind {
			return tokens[i].kind < tokens[j].kind
		}
		return tokens[i].id < tokens[j].id
	})
	staged := &BadgerTransaction{
		badgerTx: txn, pendingWrites: make(map[string][]byte), pendingDeletes: make(map[string]bool),
	}
	mapped := make(map[walNumericToken]uint64, len(bindings))
	for _, token := range tokens {
		var id uint64
		if token.kind == walNumericNode {
			id, err = resolveOrAllocateNumericIDInTxn(b.idDict, staged.staged(), NodeID(NodeID(bindings[token])))
		} else {
			id, err = resolveOrAllocateNumericIDInTxn(b.idDict, staged.staged(), EdgeID(EdgeID(bindings[token])))
		}
		if err != nil {
			return nil, nil, fmt.Errorf("wal: resolve numeric %c ID %d: %w", token.kind, token.id, err)
		}
		mapped[token] = id
	}
	// Dictionary allocation uses a transaction-shaped staging writer, but the
	// recovered record is not a normal transaction. Copy its staged entries
	// into the replay-owned write set before merging the recovered keys.
	writes := make(map[string][]byte, len(staged.pendingWrites)+len(c.writes))
	deletes := make(map[string]bool, len(staged.pendingDeletes)+len(c.deletes))
	for key, value := range staged.pendingWrites {
		writes[key] = value
	}
	for key, value := range staged.pendingDeletes {
		deletes[key] = value
	}
	localMetadata := func(key string) bool {
		switch key[0] {
		case prefixIDDictNodeForward, prefixIDDictEdgeForward, prefixIDDictNodeReverse, prefixIDDictEdgeReverse, prefixIDDictCounter, prefixIDFreelist:
			return true
		}
		return false
	}
	for key, value := range c.writes {
		if localMetadata(key) {
			continue
		}
		rebound, value, err := rewriteWALNumericFields(key, value, mapped)
		if err != nil {
			return nil, nil, err
		}
		if _, exists := writes[rebound]; exists || deletes[rebound] {
			return nil, nil, fmt.Errorf("wal: numeric rebinding produced a duplicate write key")
		}
		writes[rebound] = value
	}
	for key := range c.deletes {
		if localMetadata(key) {
			continue
		}
		rebound, _, err := rewriteWALNumericFields(key, nil, mapped)
		if err != nil {
			return nil, nil, err
		}
		if _, exists := writes[rebound]; exists || deletes[rebound] {
			return nil, nil, fmt.Errorf("wal: numeric rebinding produced a duplicate delete key")
		}
		deletes[rebound] = true
	}
	return writes, deletes, nil
}

// validateReboundWALWrites checks the complete replay-owned set after all
// rebinding and before any recovered physical keys reach the batch writer.
// This also checks the destination dictionary entries added during rebinding.
func validateReboundWALWrites(writes map[string][]byte, deletes map[string]bool) error {
	accept := func(kind, _ byte, _ int, id uint64) error {
		if id == 0 {
			return fmt.Errorf("wal: invalid zero numeric %c ID", kind)
		}
		return nil
	}
	return visitWALWriteSetNumericFields(writes, deletes, accept)
}
