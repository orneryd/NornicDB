package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"sort"
)

// nodeFormatProjectedV1 is written only by stores created with projected:true.
// The legacy V2 body follows a small, sorted property-ID directory, so full
// readers can borrow the same payload slices and projected readers can fetch
// selected byte ranges from Badger's current value pointer.
const nodeFormatProjectedV1 byte = 0x11

const projectedNodeHeaderSize = 21
const projectedNodeDirectoryEntrySize = 20
const projectedNodeDirectoryMaxBytes = 1 << 20

var projectedNodeCRC = crc32.MakeTable(crc32.Castagnoli)

type projectedPropertyRegion struct {
	id     uint64
	start  uint32
	length uint32
	crc    uint32
}

type projectedNodeHeader struct {
	directoryLength uint32
	metadataStart   uint32
	metadataLength  uint32
	metadataCRC     uint32
	directoryCRC    uint32
}

func buildProjectedNodeFrame(legacy []byte) ([]byte, error) {
	envelope, err := SplitNodeEnvelope(legacy)
	if err != nil {
		return nil, err
	}
	return buildProjectedNodeFrameFromParts(envelope.properties, envelope.metadata)
}

// buildProjectedNodeFrameFromParts writes the directory and tokenized body to
// one final allocation. The normal writer already owns separate property and
// metadata buffers, so assembling an intermediate 2 MiB V2 body would double
// the copy and retained allocation cost of a large write.
func buildProjectedNodeFrameFromParts(properties, metadata []byte) ([]byte, error) {
	regions := make([]projectedPropertyRegion, 0, 8)
	_, err := walkPhysicalPropertyValues(properties, nil, func(id uint64, start, end int) {
		regions = append(regions, projectedPropertyRegion{id: id, start: uint32(start), length: uint32(end - start), crc: crc32.Checksum(properties[start:end], projectedNodeCRC)})
	})
	if err != nil {
		return nil, fmt.Errorf("projected node property framing: %w", err)
	}
	return buildProjectedNodeFrameFromRegions(properties, metadata, regions)
}

func buildProjectedNodeFrameFromRegions(properties, metadata []byte, regions []projectedPropertyRegion) ([]byte, error) {
	var lengthWord [binary.MaxVarintLen64]byte
	lengthSize := binary.PutUvarint(lengthWord[:], uint64(len(properties)))
	legacyLength := 1 + lengthSize + len(properties) + len(metadata)
	if len(regions) > (math.MaxUint32-4)/projectedNodeDirectoryEntrySize || uint64(legacyLength) > math.MaxUint32 {
		return nil, fmt.Errorf("projected node directory exceeds value size: %w", ErrInvalidData)
	}
	sort.Slice(regions, func(i, j int) bool { return regions[i].id < regions[j].id })
	for i := 1; i < len(regions); i++ {
		if regions[i-1].id == regions[i].id {
			return nil, fmt.Errorf("duplicate property ID %d: %w", regions[i].id, ErrInvalidData)
		}
	}
	directoryLength := 4 + len(regions)*projectedNodeDirectoryEntrySize
	if directoryLength > projectedNodeDirectoryMaxBytes {
		return nil, fmt.Errorf("projected node directory exceeds maximum size: %w", ErrInvalidData)
	}
	legacyStart := projectedNodeHeaderSize + directoryLength
	propertiesStart := legacyStart + 1 + lengthSize
	metadataStart := propertiesStart + len(properties)
	if uint64(metadataStart) > math.MaxUint32 || uint64(legacyStart)+uint64(legacyLength) > math.MaxUint32 {
		return nil, fmt.Errorf("projected node exceeds value size: %w", ErrInvalidData)
	}
	out := make([]byte, legacyStart+legacyLength)
	out[0] = nodeFormatProjectedV1
	binary.BigEndian.PutUint32(out[1:5], uint32(directoryLength))
	binary.BigEndian.PutUint32(out[5:9], uint32(metadataStart))
	binary.BigEndian.PutUint32(out[9:13], uint32(len(metadata)))
	binary.BigEndian.PutUint32(out[13:17], crc32.Checksum(metadata, projectedNodeCRC))
	dir := out[projectedNodeHeaderSize:legacyStart]
	binary.BigEndian.PutUint32(dir[:4], uint32(len(regions)))
	for i, region := range regions {
		entry := dir[4+i*projectedNodeDirectoryEntrySize:][:projectedNodeDirectoryEntrySize]
		binary.BigEndian.PutUint64(entry[:8], region.id)
		binary.BigEndian.PutUint32(entry[8:12], uint32(propertiesStart)+region.start)
		binary.BigEndian.PutUint32(entry[12:16], region.length)
		binary.BigEndian.PutUint32(entry[16:20], region.crc)
	}
	checksum := crc32.New(projectedNodeCRC)
	_, _ = checksum.Write(out[:17])
	_, _ = checksum.Write(dir)
	binary.BigEndian.PutUint32(out[17:21], checksum.Sum32())
	out[legacyStart] = nodeFormatTokenizedV1
	copy(out[legacyStart+1:], lengthWord[:lengthSize])
	copy(out[propertiesStart:], properties)
	copy(out[metadataStart:], metadata)
	return out, nil
}

func parseProjectedNodeHeader(data []byte) (projectedNodeHeader, error) {
	if len(data) < projectedNodeHeaderSize || data[0] != nodeFormatProjectedV1 {
		return projectedNodeHeader{}, fmt.Errorf("invalid projected node header: %w", ErrInvalidData)
	}
	header := projectedNodeHeader{
		directoryLength: binary.BigEndian.Uint32(data[1:5]),
		metadataStart:   binary.BigEndian.Uint32(data[5:9]),
		metadataLength:  binary.BigEndian.Uint32(data[9:13]),
		metadataCRC:     binary.BigEndian.Uint32(data[13:17]),
		directoryCRC:    binary.BigEndian.Uint32(data[17:21]),
	}
	if header.directoryLength < 4 || (header.directoryLength-4)%projectedNodeDirectoryEntrySize != 0 ||
		uint64(projectedNodeHeaderSize)+uint64(header.directoryLength) > uint64(len(data)) ||
		uint64(header.metadataStart)+uint64(header.metadataLength) != uint64(len(data)) {
		return projectedNodeHeader{}, fmt.Errorf("projected node has invalid bounds: %w", ErrInvalidData)
	}
	dir := data[projectedNodeHeaderSize : projectedNodeHeaderSize+header.directoryLength]
	if err := verifyProjectedNodeDirectory(data[:projectedNodeHeaderSize], dir); err != nil {
		return projectedNodeHeader{}, err
	}
	if crc32.Checksum(data[header.metadataStart:], projectedNodeCRC) != header.metadataCRC {
		return projectedNodeHeader{}, fmt.Errorf("projected node metadata checksum mismatch: %w", ErrInvalidData)
	}
	return header, nil
}

// parseProjectedNodePrefix validates the fixed header and directory fetched
// with two bounded Badger reads. It deliberately does not require the large
// property payload or metadata to be present in memory.
func parseProjectedNodePrefix(fixed, dir []byte) (projectedNodeHeader, error) {
	if len(fixed) != projectedNodeHeaderSize || fixed[0] != nodeFormatProjectedV1 {
		return projectedNodeHeader{}, fmt.Errorf("invalid projected node header: %w", ErrInvalidData)
	}
	header := projectedNodeHeader{
		directoryLength: binary.BigEndian.Uint32(fixed[1:5]),
		metadataStart:   binary.BigEndian.Uint32(fixed[5:9]),
		metadataLength:  binary.BigEndian.Uint32(fixed[9:13]),
		metadataCRC:     binary.BigEndian.Uint32(fixed[13:17]),
		directoryCRC:    binary.BigEndian.Uint32(fixed[17:21]),
	}
	if header.directoryLength < 4 || len(dir) != int(header.directoryLength) ||
		(header.directoryLength-4)%projectedNodeDirectoryEntrySize != 0 ||
		uint64(header.metadataStart) < uint64(projectedNodeHeaderSize)+uint64(header.directoryLength) ||
		uint64(header.metadataStart)+uint64(header.metadataLength) > math.MaxUint32 {
		return projectedNodeHeader{}, fmt.Errorf("projected node has invalid bounds: %w", ErrInvalidData)
	}
	if err := verifyProjectedNodeDirectory(fixed, dir); err != nil {
		return projectedNodeHeader{}, err
	}
	return header, nil
}

func verifyProjectedNodeDirectory(fixed, dir []byte) error {
	checksum := crc32.New(projectedNodeCRC)
	_, _ = checksum.Write(fixed[:17])
	_, _ = checksum.Write(dir)
	if checksum.Sum32() != binary.BigEndian.Uint32(fixed[17:21]) ||
		binary.BigEndian.Uint32(dir[:4]) != uint32((len(dir)-4)/projectedNodeDirectoryEntrySize) {
		return fmt.Errorf("projected node directory checksum mismatch: %w", ErrInvalidData)
	}
	return nil
}

func lookupProjectedNodeRegion(dir []byte, id uint64, metadataStart uint32) (projectedPropertyRegion, bool, error) {
	count := int(binary.BigEndian.Uint32(dir[:4]))
	index := sort.Search(count, func(i int) bool {
		return binary.BigEndian.Uint64(dir[4+i*projectedNodeDirectoryEntrySize:]) >= id
	})
	if index == count {
		return projectedPropertyRegion{}, false, nil
	}
	entry := dir[4+index*projectedNodeDirectoryEntrySize:][:projectedNodeDirectoryEntrySize]
	if binary.BigEndian.Uint64(entry[:8]) != id {
		return projectedPropertyRegion{}, false, nil
	}
	region := projectedPropertyRegion{
		id: id, start: binary.BigEndian.Uint32(entry[8:12]),
		length: binary.BigEndian.Uint32(entry[12:16]), crc: binary.BigEndian.Uint32(entry[16:20]),
	}
	if region.start < uint32(projectedNodeHeaderSize+len(dir)) ||
		uint64(region.start)+uint64(region.length) > uint64(metadataStart) {
		return projectedPropertyRegion{}, false, fmt.Errorf("projected node property region is out of bounds: %w", ErrInvalidData)
	}
	return region, true, nil
}
