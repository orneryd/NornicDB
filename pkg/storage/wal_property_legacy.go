package storage

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// Lossless recovery sidecars contain named after-images. For an unstamped
// foreign legacy record they can supply property meaning without guessing the
// source dictionary. Only the property payload is re-encoded; other physical
// metadata, including the separately owned endpoint-ID representation, stays
// under its existing owner.
func (b *BadgerEngine) rebindWALNamedProperties(txn *badger.Txn, data *WALBufferedCommitData) (map[string][]byte, error) {
	properties := make(map[string]map[string]any)
	for _, op := range data.Ops {
		switch op.Type {
		case OpCreateNode, OpUpdateNode:
			if op.Node != nil {
				properties[string(nodeKey(op.Node.ID))] = op.Node.Properties
			}
		case OpCreateEdge, OpUpdateEdge:
			if op.Edge != nil {
				properties[string(edgeKey(op.Edge.ID))] = op.Edge.Properties
			}
		}
	}
	return rewriteWALPropertyPayloads(data.Writes, func(key, namespace string, payload []byte) ([]byte, error) {
		named, found := properties[key]
		count, _, err := readUvarint(payload)
		if err != nil {
			return nil, err
		}
		if !found || count != uint64(len(named)) {
			return nil, fmt.Errorf("wal: missing property name binding; legacy named after-image is incomplete for %q", key[1:])
		}
		return b.encodeTokenizedProperties(txn, namespace, named)
	})
}
