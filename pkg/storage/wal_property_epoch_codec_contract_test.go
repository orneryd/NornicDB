package storage

import (
	"hash/crc32"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWALPropertyEpochCodecByteContract(t *testing.T) {
	for _, test := range []struct {
		name     string
		frame    walPropertyEpochFrame
		data     string
		header   string
		checksum uint32
	}{
		{
			name: "unsealed null authority",
			frame: walPropertyEpochFrame{
				Format: 1, Through: "ffffffffffffffff", Last: "ffffffffffffffff",
				through: 7, last: 8,
			},
			data:     `{"nornic_property_epoch":1,"authority":null,"through":"ffffffffffffffff","last":"ffffffffffffffff"}`,
			header:   `{"seq":                   7,"ts":"1970-01-01T00:00:00Z","op":"checkpoint","data":null,"checksum":3015392351}`,
			checksum: 3015392351,
		},
		{
			name: "finite boundary spelling",
			frame: walPropertyEpochFrame{
				Format: 1, Through: "000000000000004A", Last: "0000000000000001",
				through: 7, last: 8,
			},
			data:     `{"nornic_property_epoch":1,"authority":null,"through":"000000000000004A","last":"0000000000000001"}`,
			header:   `{"seq":                   7,"ts":"1970-01-01T00:00:00Z","op":"checkpoint","data":null,"checksum": 497951736}`,
			checksum: 497951736,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Literal witnesses pin field order, exported boundary spellings and
			// fixed-width header spaces independently of the product encoder.
			// The numeric read caches intentionally disagree with those strings.
			require.Len(t, test.header, 108)
			require.Len(t, test.data, 99)
			require.Equal(t, test.checksum, crc32.Checksum([]byte(test.data), crc32.MakeTable(crc32.Castagnoli)))
			expected := append([]byte{108, 0, 0, 0}, []byte(test.header)...)
			expected = append(expected, 99, 0, 0, 0)
			expected = append(expected, test.data...)

			payload, err := encodeWALPropertyEpochFrame(test.frame, 7, time.Unix(0, 0).UTC())
			require.NoError(t, err)
			require.Equal(t, expected, payload)
		})
	}
}

func TestWALPropertyEpochCodecOverflowContract(t *testing.T) {
	const overflow = "10000000000000000"
	for _, test := range []struct {
		name     string
		data     string
		expected walPropertyEpochFrame
		error    string
	}{
		{
			name: "byte boundary precedes invalid sequence",
			data: `{"nornic_property_epoch":1,"authority":null,"through":"10000000000000000","last":"not-hex"}`,
			expected: walPropertyEpochFrame{
				Format: 1, Through: overflow, Last: "not-hex", through: ^uint64(0),
			},
			error: `wal: invalid property epoch byte boundary: strconv.ParseUint: parsing "10000000000000000": value out of range`,
		},
		{
			name: "sequence boundary precedes framing rule",
			data: `{"nornic_property_epoch":1,"authority":null,"through":"000000000000004A","last":"10000000000000000"}`,
			expected: walPropertyEpochFrame{
				Format: 1, Through: "000000000000004A", Last: overflow, through: 74, last: ^uint64(0),
			},
			error: `wal: invalid property epoch sequence boundary: strconv.ParseUint: parsing "10000000000000000": value out of range`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			frame, err := decodeWALPropertyEpochFrame(WALEntry{
				Operation: OpCheckpoint, Data: []byte(test.data), framingVersion: walFormatVersionV3,
			})
			require.EqualError(t, err, test.error)
			var numeric *strconv.NumError
			require.ErrorAs(t, err, &numeric)
			require.Equal(t, "ParseUint", numeric.Func)
			require.Equal(t, overflow, numeric.Num)
			require.ErrorIs(t, numeric.Err, strconv.ErrRange)
			require.ErrorIs(t, err, strconv.ErrRange)
			require.Equal(t, test.expected, frame)
		})
	}
}
