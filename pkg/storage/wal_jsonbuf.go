package storage

import (
	"bytes"

	"github.com/orneryd/nornicdb/internal/jsontext"
	"sync"
)

var walJSONBufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

// marshalJSONCompact writes v as JSON into buf without a trailing newline and
// returns the resulting byte slice.
//
// It uses json.Encoder to reuse the provided buffer across calls (reducing
// allocations versus json.Marshal) and trims the newline that Encoder adds.
func marshalJSONCompact(buf *bytes.Buffer, v any) ([]byte, error) {
	buf.Reset()
	enc := jsontext.NewEncoder(buf)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	b := buf.Bytes()
	if len(b) > 0 && b[len(b)-1] == '\n' {
		buf.Truncate(len(b) - 1)
	}
	return buf.Bytes(), nil
}
