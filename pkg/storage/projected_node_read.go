package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
)

// projectedTokenResolver memoizes forward property bindings only for one
// Badger read snapshot. A namespace change clears the memo, so a dropped and
// recreated database cannot give an older body a newer token meaning.
type projectedTokenResolver struct {
	b         *BadgerEngine
	txn       *badger.Txn
	namespace string
	tokens    map[string]projectedResolvedToken
}

type projectedResolvedToken struct {
	id    uint64
	found bool
}

func (r *projectedTokenResolver) resolve(id NodeID, name string) (uint64, bool, error) {
	namespace := namespaceForNodeID(id)
	if namespace != r.namespace {
		r.namespace = namespace
		clear(r.tokens)
	}
	if token, ok := r.tokens[name]; ok {
		return token.id, token.found, nil
	}
	idValue, found, err := r.b.readStoredPropertyToken(r.txn, namespace, name)
	if err == nil {
		if r.tokens == nil {
			r.tokens = make(map[string]projectedResolvedToken)
		}
		r.tokens[name] = projectedResolvedToken{id: idValue, found: found}
	}
	return idValue, found, err
}

func (b *BadgerEngine) readPhysicalProjectedNode(txn *badger.Txn, item *badger.Item, id NodeID, include map[string]struct{}, into *Node, resolver *projectedTokenResolver) (*Node, bool, error) {
	if !b.projected {
		return nil, false, nil
	}
	// Inline SST values are already resident when Badger returns the item.
	// A single Value call avoids repeating Badger's value setup for every
	// selected property. Large values retain bounded value-log range reads.
	if size := item.ValueSize(); size > 0 && size <= 1024 {
		var node *Node
		var used bool
		err := item.Value(func(value []byte) error {
			var decodeErr error
			node, used, decodeErr = b.decodePhysicalProjectedNodeRanges(txn, item, id, include, into, resolver, value)
			return decodeErr
		})
		return node, used, err
	}
	return b.decodePhysicalProjectedNodeRanges(txn, item, id, include, into, resolver, nil)
}

// decodePhysicalProjectedNodeRanges uses the item selected by the caller's
// Badger snapshot. A non-nil inline value is read once; otherwise each
// requested region is read through the same current value pointer.
func (b *BadgerEngine) decodePhysicalProjectedNodeRanges(txn *badger.Txn, item *badger.Item, id NodeID, include map[string]struct{}, into *Node, resolver *projectedTokenResolver, inline []byte) (*Node, bool, error) {
	readRange := func(start, length uint32, fn func([]byte) error) error {
		if inline == nil {
			return item.ValueRange(start, length, fn)
		}
		if uint64(start)+uint64(length) > uint64(len(inline)) {
			return fmt.Errorf("projected node range exceeds value: %w", ErrInvalidData)
		}
		return fn(inline[start : start+length])
	}
	var format byte
	if err := readRange(0, 1, func(value []byte) error { format = value[0]; return nil }); err != nil {
		return nil, false, err
	}
	if format != nodeFormatProjectedV1 {
		return nil, false, nil
	}
	var fixed [projectedNodeHeaderSize]byte
	if err := readRange(0, projectedNodeHeaderSize, func(value []byte) error { copy(fixed[:], value); return nil }); err != nil {
		return nil, true, err
	}
	dirLength := binary.BigEndian.Uint32(fixed[1:5])
	if dirLength < 4 || (dirLength-4)%projectedNodeDirectoryEntrySize != 0 || dirLength > projectedNodeDirectoryMaxBytes {
		return nil, true, fmt.Errorf("projected node directory length is invalid: %w", ErrInvalidData)
	}
	dir := make([]byte, dirLength)
	if err := readRange(projectedNodeHeaderSize, dirLength, func(value []byte) error { copy(dir, value); return nil }); err != nil {
		return nil, true, err
	}
	header, err := parseProjectedNodePrefix(fixed[:], dir)
	if err != nil {
		return nil, true, err
	}
	if into == nil {
		into = &Node{}
		if err := readRange(header.metadataStart, header.metadataLength, func(value []byte) error {
			if crc32.Checksum(value, projectedNodeCRC) != header.metadataCRC {
				return fmt.Errorf("projected node metadata checksum mismatch: %w", ErrInvalidData)
			}
			return decodeValue(value, into)
		}); err != nil {
			return nil, true, err
		}
	}
	if into.Properties == nil {
		into.Properties = make(map[string]any, len(include))
	}
	var valueReader bytes.Reader
	var decoder *msgpack.Decoder
	defer func() {
		if decoder != nil {
			msgpack.PutDecoder(decoder)
		}
	}()
	namespace := namespaceForNodeID(id)
	for name := range include {
		if name == hasEmbeddingProperty {
			continue
		}
		var token uint64
		var found bool
		var err error
		if resolver != nil {
			token, found, err = resolver.resolve(id, name)
		} else {
			token, found, err = b.readStoredPropertyToken(txn, namespace, name)
		}
		if err != nil {
			return nil, true, err
		}
		if !found {
			continue
		}
		region, found, err := lookupProjectedNodeRegion(dir, token, header.metadataStart)
		if err != nil {
			return nil, true, err
		}
		if !found {
			continue
		}
		if err := readRange(region.start, region.length, func(value []byte) error {
			if crc32.Checksum(value, projectedNodeCRC) != region.crc {
				return fmt.Errorf("projected node property %q checksum mismatch: %w", name, ErrInvalidData)
			}
			if decoder == nil {
				decoder = msgpack.GetDecoder()
			}
			valueReader.Reset(value)
			decoder.Reset(&valueReader)
			decoded, err := decodeStrictTypedValue(decoder)
			if err != nil {
				return err
			}
			if valueReader.Len() != 0 {
				return fmt.Errorf("projected node property %q has trailing bytes: %w", name, ErrInvalidData)
			}
			into.Properties[name] = restoreStoredTemporalValue(decoded)
			return nil
		}); err != nil {
			return nil, true, err
		}
	}
	return into, true, nil
}
