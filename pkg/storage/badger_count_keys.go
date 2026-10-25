package storage

import (
	"encoding/binary"

	"github.com/orneryd/nornicdb/pkg/util"
)

// Count format 3 length-frames namespace identities as well as positional
// labels. Readiness is published by the retained authoritative rebuild owner.
const namedCountFormatVersion byte = 3

// countNamespaceKey builds a count-family prefix with an exact namespace
// extent. tailBytes is allocation capacity for the caller's remaining fields.
func countNamespaceKey(kind byte, namespace string, tailBytes int) []byte {
	key := make([]byte, 0, util.SafePreallocSum(2+binary.MaxVarintLen64, len(namespace), tailBytes))
	key = append(key, prefixMVCCMeta, kind)
	key = binary.AppendUvarint(key, uint64(len(namespace)))
	return append(key, namespace...)
}

// countNamespaceParts returns the exact namespace and the caller-owned tail.
// The final graph name or positional label/type fields keep their own extent.
func countNamespaceParts(key []byte) (namespace string, tail []byte, valid bool) {
	if len(key) < 3 || key[0] != prefixMVCCMeta {
		return "", nil, false
	}
	length, used := binary.Uvarint(key[2:])
	if used <= 0 || length > uint64(len(key)-2-used) {
		return "", nil, false
	}
	end := 2 + used + int(length)
	return string(key[2+used : end]), key[end:], true
}
