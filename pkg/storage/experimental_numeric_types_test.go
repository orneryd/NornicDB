package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestExperimentalCompressedNumericTypes(t *testing.T) {
	experimentalStorageFlag(t, true)
	engine := createTestBadgerEngine(t)
	numbers := make([]int64, 4096)
	floats := make([]float64, 4096)
	patterns := []uint64{0, 1 << 63, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000001, 0x7ff8000000000002, 1}
	for i := range numbers {
		hash := sha256.Sum256([]byte(fmt.Sprintf("documentation/numeric/%d", i)))
		numbers[i] = int64(hash[0] % 64)
		floats[i] = math.Float64frombits(patterns[i%len(patterns)])
	}
	_, err := engine.CreateNode(context.Background(), &Node{ID: "test:numeric", Properties: map[string]any{"numbers": numbers, "floats": floats}})
	require.NoError(t, err)
	require.NoError(t, engine.withView(func(tx *badger.Txn) error {
		item, e := tx.Get(nodeKey("test:numeric"))
		if e != nil {
			return e
		}
		return item.Value(func(body []byte) error {
			envelope, e := SplitNodeEnvelope(body)
			if e != nil {
				return e
			}
			reader := bytes.NewReader(envelope.properties)
			count, e := binary.ReadUvarint(reader)
			if e != nil {
				return e
			}
			decoder := msgpack.NewDecoder(reader)
			modes := map[byte]bool{}
			for i := uint64(0); i < count; i++ {
				if _, e = binary.ReadUvarint(reader); e != nil {
					return e
				}
				raw, e := decoder.DecodeRaw()
				if e != nil {
					return e
				}
				require.True(t, isExperimentalProperty(raw))
				valueReader := bytes.NewReader(raw)
				header := msgpack.NewDecoder(valueReader)
				_, _, e = header.DecodeExtHeader()
				if e != nil {
					return e
				}
				mode, e := valueReader.ReadByte()
				if e != nil {
					return e
				}
				modes[mode] = true
			}
			require.True(t, modes[1], "float values exercise independently compressed typed MessagePack")
			require.True(t, modes[2], "integer values exercise the fork's packed column reader")
			node, e := engine.decodeNodeProjected(tx, "test", body, nil)
			if e != nil {
				return e
			}
			require.Equal(t, numbers, node.Properties["numbers"])
			actual := node.Properties["floats"].([]float64)
			require.Len(t, actual, len(floats))
			for i, v := range floats {
				require.Equal(t, math.Float64bits(v), math.Float64bits(actual[i]), "float bits at row %d", i)
			}
			return nil
		})
	}))
}
