package gobcodec

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

type failingReader struct {
	data []byte
	err  error
}

func TestDecodeAllowTrailingLeavesRemainingStream(t *testing.T) {
	var encoded bytes.Buffer
	require.NoError(t, gob.NewEncoder(&encoded).Encode("historical"))
	tail := []byte{0xab, 0xcd}
	source := bytes.NewReader(append(bytes.Clone(encoded.Bytes()), tail...))
	var got string
	require.NoError(t, Decode(source, &got, AllowTrailing))
	require.Equal(t, "historical", got)
	remaining, err := io.ReadAll(source)
	require.NoError(t, err)
	require.Equal(t, tail, remaining)
}

func (r *failingReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

func TestDecodeHistoricalFraming(t *testing.T) {
	want := struct{ Value string }{Value: "historical"}
	var encoded bytes.Buffer
	require.NoError(t, gob.NewEncoder(&encoded).Encode(want))
	for _, tail := range [][]byte{nil, {0}, {0xff}} {
		input := append(bytes.Clone(encoded.Bytes()), tail...)
		for _, policy := range []TrailingPolicy{AllowTrailing, RejectTrailing} {
			var got struct{ Value string }
			err := Decode(bytes.NewReader(input), &got, policy)
			if len(tail) > 0 && policy == RejectTrailing {
				require.ErrorIs(t, err, ErrTrailing)
			} else {
				require.NoError(t, err)
				require.Equal(t, want, got)
			}
		}
	}
	var got struct{ Value string }
	require.Error(t, Decode(bytes.NewReader([]byte("invalid")), &got, RejectTrailing))
	require.Error(t, Decode(bytes.NewReader(encoded.Bytes()), &got, TrailingPolicy(99)))
	sentinel := errors.New("reader failed")
	require.ErrorIs(t, Decode(&failingReader{data: bytes.Clone(encoded.Bytes()), err: sentinel}, &got, RejectTrailing), sentinel)
}
