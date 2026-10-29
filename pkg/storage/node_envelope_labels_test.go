package storage

// Author: #1667 implementation lane (Codex). This release-compatible test also
// runs unchanged as a test-only overlay on d663; its JSON is the byte oracle.

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeEnvelopeLabelCompatibility(t *testing.T) {
	type result struct {
		Name   string   `json:"name"`
		Labels []string `json:"labels"`
		Error  string   `json:"error"`
	}
	type sample struct {
		name string
		body []byte
	}
	samples := []sample{
		{"empty", nil}, {"wrong-format", []byte{0}},
		{"missing-length", []byte{nodeFormatTokenizedV1}},
		{"short-length", []byte{nodeFormatTokenizedV1, 0xff}},
		{"ten-continuations", append([]byte{nodeFormatTokenizedV1}, bytes.Repeat([]byte{0xff}, 10)...)},
		{"overflow", append(append([]byte{nodeFormatTokenizedV1}, bytes.Repeat([]byte{0xff}, 9)...), 2)},
		{"short-properties", []byte{nodeFormatTokenizedV1, 10, 0}},
		{"missing-metadata", []byte{nodeFormatTokenizedV1, 0}},
		{"invalid-metadata", []byte{nodeFormatTokenizedV1, 0, 0xc1}},
	}
	for _, labels := range []any{nil, []string{}, []string{"Person"}, []string{"", "Person", "Person", "\u00e9\u754c"}, 42} {
		// A struct fixes metadata order; property bytes are deliberately opaque.
		metadata, err := encodeValue(struct {
			Labels any
			Other  []any
		}{labels, []any{[]byte{0, 255}, map[string]any{"nested": true}}})
		require.NoError(t, err)
		body := binary.AppendUvarint([]byte{nodeFormatTokenizedV1}, 3)
		body = append(body, 0xff, 0xc1, 0)
		body = append(body, metadata...)
		name, err := json.Marshal(labels)
		require.NoError(t, err)
		samples = append(samples, sample{"labels=" + string(name), body})
		if want, ok := labels.([]string); ok {
			got, err := decodeStoredNodeLabels(body)
			require.NoError(t, err)
			require.Equal(t, want, got)
			var legacy bytes.Buffer
			require.NoError(t, gob.NewEncoder(&legacy).Encode(storedNodeLabels{Labels: want}))
			legacyBody := binary.AppendUvarint([]byte{nodeFormatTokenizedV1}, 0)
			legacyBody = append(legacyBody, legacy.Bytes()...)
			samples = append(samples, sample{"legacy=" + string(name), legacyBody})
		}
		for cut := 1; cut < len(metadata); cut++ {
			samples = append(samples, sample{"truncated=" + string(name) + ":" + strconv.Itoa(cut), body[:len(body)-cut]})
		}
	}
	var results []result
	for _, sample := range samples {
		labels, err := decodeStoredNodeLabels(sample.body)
		row := result{Name: sample.name, Labels: labels}
		if err != nil {
			row.Error = err.Error()
		}
		results = append(results, row)
	}
	oracle, err := json.Marshal(results)
	require.NoError(t, err)
	t.Log("LABEL-COMPATIBILITY-JSON " + string(oracle))
}
