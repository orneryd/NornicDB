package storage

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProjectedNodeFramePreservesLegacyBodyAndOffsets(t *testing.T) {
	properties := []byte{2, 7, 0xa1, 'x', 3, 0x2a}
	metadata := []byte{1, 2, 3, 4, 5}
	legacy := binary.AppendUvarint([]byte{nodeFormatTokenizedV1}, uint64(len(properties)))
	legacy = append(legacy, properties...)
	legacy = append(legacy, metadata...)
	framed, err := buildProjectedNodeFrame(legacy)
	require.NoError(t, err)
	header, err := parseProjectedNodeHeader(framed)
	require.NoError(t, err)
	require.Equal(t, uint32(len(metadata)), header.metadataLength)
	envelope, err := SplitNodeEnvelope(framed)
	require.NoError(t, err)
	require.Equal(t, properties, envelope.PropertyPayload())
	require.Equal(t, metadata, envelope.MetadataPayload())
	framed[len(framed)-1] ^= 1
	require.Error(t, func() error { _, err := parseProjectedNodeHeader(framed); return err }())
}

func TestProjectedNodeDirectoryRebuiltAfterWALPropertyIDRemap(t *testing.T) {
	properties := []byte{1, 7, 0xa1, 'x'}
	legacy := binary.AppendUvarint([]byte{nodeFormatTokenizedV1}, uint64(len(properties)))
	legacy = append(legacy, properties...)
	legacy = append(legacy, []byte{1, 2, 3}...)
	framed, err := buildProjectedNodeFrame(legacy)
	require.NoError(t, err)
	key := string(nodeKey("ns:n"))
	rewritten, err := rewriteWALPropertyBodies(map[string][]byte{key: framed}, func(namespace string, token uint64) (uint64, error) {
		require.Equal(t, "ns", namespace)
		require.Equal(t, uint64(7), token)
		return 9, nil
	})
	require.NoError(t, err)
	require.Equal(t, nodeFormatProjectedV1, rewritten[key][0])
	header, err := parseProjectedNodeHeader(rewritten[key])
	require.NoError(t, err)
	dir := rewritten[key][projectedNodeHeaderSize : projectedNodeHeaderSize+header.directoryLength]
	_, found, err := lookupProjectedNodeRegion(dir, 7, header.metadataStart)
	require.NoError(t, err)
	require.False(t, found)
	region, found, err := lookupProjectedNodeRegion(dir, 9, header.metadataStart)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte{0xa1, 'x'}, rewritten[key][region.start:region.start+region.length])
}
