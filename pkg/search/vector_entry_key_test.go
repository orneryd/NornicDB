package search

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Node IDs, vector names and property keys are free strings; these include
// every separator of the legacy suffix grammar, the key marker itself, and an
// ID long enough to need a multi-byte length prefix.
var vectorEntryKeyAdversarialIDs = []string{
	"x", "doc", "doc-chunk-1", "x-named-v", "x-prop-emb", "a-named-b-chunk-2",
	"\x00", "\x00m\x01a", "\x00c\x01x1", "a\x00b", "nornic:ünï-cødé", strings.Repeat("n", 300),
}

func vectorEntryKeyFor(entry vectorEntry) string {
	switch entry.Kind {
	case vectorEntryChunk:
		return nodeChunkVectorKey(entry.NodeID, entry.Chunk)
	case vectorEntryNamed:
		return nodeNamedVectorKey(entry.NodeID, entry.Name)
	case vectorEntryProperty:
		return nodePropertyVectorKey(entry.NodeID, entry.Name)
	}
	return nodeMainVectorKey(entry.NodeID)
}

func TestVectorEntryKeyRoundTripsAndIsInjective(t *testing.T) {
	var entries []vectorEntry
	for _, id := range vectorEntryKeyAdversarialIDs {
		entries = append(entries,
			vectorEntry{NodeID: id, Kind: vectorEntryMain},
			vectorEntry{NodeID: id, Kind: vectorEntryChunk, Chunk: 1},
			vectorEntry{NodeID: id, Kind: vectorEntryChunk, Chunk: 12},
		)
		for _, name := range []string{"", "v", "a-named-b", "p-chunk-1", "-prop-", "\x00", "1"} {
			entries = append(entries,
				vectorEntry{NodeID: id, Kind: vectorEntryNamed, Name: name},
				vectorEntry{NodeID: id, Kind: vectorEntryProperty, Name: name})
		}
	}
	owners := make(map[string]vectorEntry, len(entries))
	for _, entry := range entries {
		key := vectorEntryKeyFor(entry)
		previous, seen := owners[key]
		require.False(t, seen, "key %q is shared by %+v and %+v", key, previous, entry)
		owners[key] = entry

		decoded, ok := decodeVectorEntryKey(key)
		require.True(t, ok, "key %q of %+v must decode", key, entry)
		assert.Equal(t, entry, decoded)
		owner, ok := vectorEntryOwner(key)
		require.True(t, ok)
		assert.Equal(t, entry.NodeID, owner)
	}
	// A plain node's main vector is keyed by its ID; an ID starting with the
	// marker is escaped, so no raw ID equals another entry's key.
	assert.Equal(t, "doc-chunk-1", nodeMainVectorKey("doc-chunk-1"))
	assert.Equal(t, "doc", nodeChunkVectorKey("doc", 0))
	assert.NotEqual(t, "\x00", nodeMainVectorKey("\x00"))
}

func TestDecodeVectorEntryKeyRejectsNonCanonicalKeys(t *testing.T) {
	encoded := nodeChunkVectorKey("x", 3)
	for name, key := range map[string]string{
		"empty":                     "",
		"marker only":               "\x00",
		"marker and kind":           "\x00c",
		"unknown kind":              "\x00z\x01x",
		"length past end":           "\x00n\x05ab",
		"over-long length":          "\x00n\x81\x00x",
		"empty node ID":             "\x00n\x00v",
		"chunk zero":                "\x00c\x01x0",
		"chunk leading zero":        "\x00c\x01x01",
		"chunk sign":                "\x00c\x01x+1",
		"chunk negative":            "\x00c\x01x-1",
		"chunk not a number":        "\x00c\x01xa",
		"chunk overflow":            "\x00c\x01x99999999999999999999",
		"main with suffix":          "\x00m\x01xy",
		"main escape of a plain ID": "\x00m\x01x",
		"truncated valid chunk key": encoded[:len(encoded)-1],
	} {
		t.Run(name, func(t *testing.T) {
			_, ok := decodeVectorEntryKey(key)
			assert.False(t, ok)
			owner, ok := vectorEntryOwner(key)
			assert.False(t, ok)
			assert.Empty(t, owner)
		})
	}
}

func TestVectorEntryChunkIndex(t *testing.T) {
	for _, tc := range []struct {
		nodeID, key string
		chunk       int
		ok          bool
	}{
		{"doc", "doc", 0, true},
		{"doc", nodeChunkVectorKey("doc", 2), 2, true},
		{"doc", nodeNamedVectorKey("doc", "v"), 0, false},
		{"doc", nodePropertyVectorKey("doc", "p"), 0, false},
		// Another node's entries are not this node's chunks, even when the
		// legacy grammar would have read them that way.
		{"doc", "doc-chunk-2", 0, false},
		{"doc", nodeChunkVectorKey("doc-chunk-1", 2), 0, false},
		{"doc", "", 0, false},
		{"\x00", nodeChunkVectorKey("\x00", 0), 0, true},
	} {
		chunk, ok := vectorEntryChunkIndex(tc.nodeID, tc.key)
		assert.Equal(t, tc.ok, ok, "%q in %q", tc.key, tc.nodeID)
		assert.Equal(t, tc.chunk, chunk, "%q in %q", tc.key, tc.nodeID)
	}
}

func TestVectorEntryDescribeAndMainKeys(t *testing.T) {
	assert.Equal(t, "main", vectorEntry{Kind: vectorEntryMain}.describe())
	assert.Equal(t, "chunk 3", vectorEntry{Kind: vectorEntryChunk, Chunk: 3}.describe())
	assert.Equal(t, "named[v]", vectorEntry{Kind: vectorEntryNamed, Name: "v"}.describe())
	assert.Equal(t, "property[p]", vectorEntry{Kind: vectorEntryProperty, Name: "p"}.describe())
	assert.Equal(t, []string{"a", nodeMainVectorKey("\x00b")}, nodeMainVectorKeys([]string{"a", "\x00b"}))
	assert.Equal(t, nodeChunkVectorKey("d", 2), NodeChunkVectorKey("d", 2))
}

// Every (node, entry) maps to its own key, and every key decodes back to
// exactly the entry that produced it.
func FuzzVectorEntryKey(f *testing.F) {
	f.Add("doc", "doc-chunk", byte('c'), 1, "", byte('m'), 0, "")
	f.Add("x", "x-named-v", byte('n'), 0, "v", byte('m'), 0, "")
	f.Add("\x00", "\x00m\x01", byte('m'), 0, "", byte('p'), 0, "p")
	f.Fuzz(func(t *testing.T, idA, idB string, kindA byte, chunkA int, nameA string, kindB byte, chunkB int, nameB string) {
		entry := func(id string, kind byte, chunk int, name string) (vectorEntry, bool) {
			if id == "" {
				return vectorEntry{}, false
			}
			switch kind % 4 {
			case 0:
				return vectorEntry{NodeID: id, Kind: vectorEntryMain}, true
			case 1:
				if chunk < 1 {
					return vectorEntry{}, false
				}
				return vectorEntry{NodeID: id, Kind: vectorEntryChunk, Chunk: chunk}, true
			case 2:
				return vectorEntry{NodeID: id, Kind: vectorEntryNamed, Name: name}, true
			}
			return vectorEntry{NodeID: id, Kind: vectorEntryProperty, Name: name}, true
		}
		a, okA := entry(idA, kindA, chunkA, nameA)
		b, okB := entry(idB, kindB, chunkB, nameB)
		if !okA || !okB {
			return
		}
		keyA, keyB := vectorEntryKeyFor(a), vectorEntryKeyFor(b)
		decoded, ok := decodeVectorEntryKey(keyA)
		if !ok || decoded != a {
			t.Fatalf("%+v encodes to %q, which decodes to %+v (ok=%t)", a, keyA, decoded, ok)
		}
		if keyA == keyB && a != b {
			t.Fatalf("%+v and %+v share key %q", a, b, keyA)
		}
	})
}

// mustVectorEntryOwner decodes a raw vector-entry key a test got from an
// index (HNSW, candidate generators) into its owning node.
func mustVectorEntryOwner(t testing.TB, key string) string {
	t.Helper()
	owner, ok := vectorEntryOwner(key)
	require.True(t, ok, "vector-entry key %q has no owner", key)
	return owner
}
