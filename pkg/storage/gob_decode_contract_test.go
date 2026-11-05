package storage

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/orneryd/nornicdb/internal/gobcodec"
	"github.com/stretchr/testify/require"
)

func TestHistoricalGobDecodeFramingContracts(t *testing.T) {
	want := struct{ Name string }{Name: "historical"}
	var encoded bytes.Buffer
	require.NoError(t, gob.NewEncoder(&encoded).Encode(want))
	withTrailing := append(append([]byte(nil), encoded.Bytes()...), 0)
	header := append([]byte(serializationMagic), serializationVersion, serializerIDGob)
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"headerless body", withTrailing},
		{"headered body", append(header, withTrailing...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got struct{ Name string }
			require.NoError(t, decodeValue(tc.data, &got))
			require.Equal(t, want, got)
		})
	}
	var migrated struct{ Name string }
	require.NoError(t, decodeV4Value(encoded.Bytes(), &migrated, false))
	require.Equal(t, want, migrated)
	require.ErrorContains(t, decodeV4Value(withTrailing, &migrated, false), "trailing historical gob payload")
}

func TestV4NodeDeletionRejectsTrailingHistoricalGob(t *testing.T) {
	var encoded bytes.Buffer
	require.NoError(t, gob.NewEncoder(&encoded).Encode(struct{ NodeID string }{NodeID: "test:n"}))
	converted, err := TranscodeV4EntityChange(EntityDeleteNode, encoded.Bytes())
	require.NoError(t, err)
	require.Equal(t, []byte("test:n"), converted)
	_, err = TranscodeV4EntityChange(EntityDeleteNode, append(bytes.Clone(encoded.Bytes()), 0))
	require.ErrorIs(t, err, gobcodec.ErrTrailing)
	converted, err = TranscodeV4EntityChange(EntityDeleteNode, []byte("test:n"))
	require.NoError(t, err)
	require.Equal(t, []byte("test:n"), converted)
}
