package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
)

// readPhysicalProjectedNode uses the item selected by the caller's Badger
// snapshot. The property directory and each requested value are read through
// the same value pointer; no physical address survives compaction or vlog GC.
func (b *BadgerEngine) readPhysicalProjectedNode(txn *badger.Txn, item *badger.Item, id NodeID, include map[string]struct{}, into *Node) (*Node, bool, error) {
	if !b.projected {
		return nil, false, nil
	}
	var format byte
	if err := item.ValueRange(0, 1, func(value []byte) error { format = value[0]; return nil }); err != nil {
		return nil, false, err
	}
	if format != nodeFormatProjectedV1 {
		return nil, false, nil
	}
	var fixed [projectedNodeHeaderSize]byte
	if err := item.ValueRange(0, projectedNodeHeaderSize, func(value []byte) error { copy(fixed[:], value); return nil }); err != nil {
		return nil, true, err
	}
	dirLength := binary.BigEndian.Uint32(fixed[1:5])
	if dirLength < 4 || (dirLength-4)%projectedNodeDirectoryEntrySize != 0 || dirLength > projectedNodeDirectoryMaxBytes {
		return nil, true, fmt.Errorf("projected node directory length is invalid: %w", ErrInvalidData)
	}
	dir := make([]byte, dirLength)
	if err := item.ValueRange(projectedNodeHeaderSize, dirLength, func(value []byte) error { copy(dir, value); return nil }); err != nil {
		return nil, true, err
	}
	header, err := parseProjectedNodePrefix(fixed[:], dir)
	if err != nil {
		return nil, true, err
	}
	if into == nil {
		into = &Node{}
		if err := item.ValueRange(header.metadataStart, header.metadataLength, func(value []byte) error {
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
	namespace := namespaceForNodeID(id)
	for name := range include {
		if name == hasEmbeddingProperty {
			continue
		}
		token, found, err := b.readStoredPropertyToken(txn, namespace, name)
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
		if err := item.ValueRange(region.start, region.length, func(value []byte) error {
			if crc32.Checksum(value, projectedNodeCRC) != region.crc {
				return fmt.Errorf("projected node property %q checksum mismatch: %w", name, ErrInvalidData)
			}
			reader := bytes.NewReader(value)
			decoder := msgpack.GetDecoder()
			decoder.Reset(reader)
			defer msgpack.PutDecoder(decoder)
			decoded, err := decodeStrictTypedValue(decoder)
			if err != nil {
				return err
			}
			if reader.Len() != 0 {
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
