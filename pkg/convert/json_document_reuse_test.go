package convert

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A complete document owns its input lifetime, unlike a caller-owned stream.
// Repeated documents must not pay for a fresh streaming buffer on every call.
func TestJSONDocumentDecodeAllocationBudget(t *testing.T) {
	raw := []byte(`{"integer":9007199254740993,"whole":2.0,"nested":{"value":18446744073709551615}}`)
	var result map[string]any
	document := testing.AllocsPerRun(100, func() {
		result = nil
		if err := PropertyJSON.Unmarshal(raw, &result); err != nil {
			panic(err)
		}
	})
	stream := testing.AllocsPerRun(100, func() {
		result = nil
		if err := PropertyJSON.NewDecoder(bytes.NewReader(raw)).Decode(&result); err != nil {
			panic(err)
		}
	})
	require.LessOrEqual(t, document, stream-2, "complete-document decoding must reuse its stream setup")
	require.Equal(t, int64(9007199254740993), result["integer"])
	require.Equal(t, float64(2), result["whole"])
	require.Equal(t, uint64(math.MaxUint64), result["nested"].(map[string]any)["value"])
}

func TestJSONDocumentDecoderReuse(t *testing.T) {
	// Include scalars ending exactly at a read-buffer boundary, mixed target
	// types, and trailing whitespace: a prior document must never supply data,
	// policy, or an EOF state to the next document.
	for round := 0; round < 3; round++ {
		for _, size := range []int{1, 510, 511, 512, 513, 1535, 1536, 4096} {
			for _, raw := range []string{`9007199254740993`, `18446744073709551615`, `2.0`, `-0.0`, `5e-324`, `null`, `true`, `"` + strings.Repeat("x", size) + `"`, fmt.Sprintf(`{"v":9007199254740993,"pad":"%s"}`, strings.Repeat("x", size))} {
				for _, input := range []string{raw, raw + " \n\t"} {
					var want, got any
					require.NoError(t, PropertyJSON.NewDecoder(strings.NewReader(raw)).Decode(&want))
					require.NoError(t, PropertyJSON.Unmarshal([]byte(input), &got))
					require.Equal(t, want, got)
					if floating, ok := want.(float64); ok {
						require.Equal(t, math.Float64bits(floating), math.Float64bits(got.(float64)))
					}
				}
			}
		}
		for _, raw := range []string{`1 2`, `{}[]`, `{"v":`, ``, `true false`, `18446744073709551616`} {
			var got any
			require.Error(t, PropertyJSON.Unmarshal([]byte(raw), &got))
		}
		var external, persisted any
		require.NoError(t, CypherInputJSON.Unmarshal([]byte(`18446744073709551615`), &external))
		require.IsType(t, float64(0), external)
		require.NoError(t, PropertyJSON.Unmarshal([]byte(`18446744073709551615`), &persisted))
		require.Equal(t, uint64(math.MaxUint64), persisted)
		var wrongType int
		require.Error(t, PropertyJSON.Unmarshal([]byte(`"text"`), &wrongType))
		var mixed struct {
			Value any `json:"value"`
			Count int `json:"count"`
		}
		require.Error(t, PropertyJSON.Unmarshal([]byte(`{"value":9007199254740993,"count":"text"}`), &mixed))
		require.NoError(t, PropertyJSON.Unmarshal([]byte(`{"value":18446744073709551615,"count":2}`), &mixed))
		require.Equal(t, uint64(math.MaxUint64), mixed.Value)
		require.Equal(t, 2, mixed.Count)
		decoder := PropertyJSON.NewDecoder(strings.NewReader(`2.0 9007199254740993`))
		require.NoError(t, decoder.Decode(&external))
		require.Equal(t, float64(2), external)
		require.NoError(t, decoder.Decode(&persisted))
		require.Equal(t, int64(9007199254740993), persisted)
		require.ErrorIs(t, decoder.Decode(&persisted), io.EOF)
	}
}

func TestJSONDocumentDecoderConcurrentDomains(t *testing.T) {
	for worker := 0; worker < 8; worker++ {
		t.Run(fmt.Sprint(worker), func(t *testing.T) {
			t.Parallel()
			for index := 0; index < 50; index++ {
				var persisted, external any
				require.NoError(t, PropertyJSON.Unmarshal([]byte(`18446744073709551615`), &persisted))
				require.Equal(t, uint64(math.MaxUint64), persisted)
				require.NoError(t, CypherInputJSON.Unmarshal([]byte(`18446744073709551615`), &external))
				require.IsType(t, float64(0), external)
			}
		})
	}
}
