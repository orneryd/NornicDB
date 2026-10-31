package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// seedRawRecord encodes one node or edge record and stores it at its primary key
// without going through the public write path. Tests use it to place exactly the
// bytes a read or rebuild path must survive; the namespace comes from the record
// ID, so an unprefixed ID seeds the unprefixed namespace.
func seedRawRecord(t *testing.T, engine *BadgerEngine, record any) {
	t.Helper()
	require.NoError(t, engine.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
		var (
			key  []byte
			body []byte
			err  error
		)
		switch value := record.(type) {
		case *Node:
			namespace, _, _ := ParseDatabasePrefix(string(value.ID))
			body, _, err = engine.encodeNodeInTxn(txn, namespace, value)
			key = nodeKey(value.ID)
		case *Edge:
			namespace, _, _ := ParseDatabasePrefix(string(value.ID))
			body, err = engine.encodeEdgeInTxn(txn, namespace, value)
			key = edgeKey(value.ID)
		default:
			t.Fatalf("seedRawRecord: unsupported record type %T", record)
		}
		if err != nil {
			return err
		}
		return txn.Set(key, body)
	}))
}
