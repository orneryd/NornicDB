package cypher

import (
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSymbolicUnicodeMapKeysPublic(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			executor := NewStorageExecutor(newTestMemoryEngine(t))
			for _, tc := range []struct{ written, key string }{
				{"`a\\u0062`", "ab"},
				{"`a\\uD800`", "a" + string([]byte{0xed, 0xa0, 0x80})},
				{"`a\\uDC00`", "a" + string([]byte{0xed, 0xb0, 0x80})},
				{"`a\\u005cu0062`", "a\\u0062"},
				{"`a\\uD83D\\uDE00`", "a😀"},
				{"`a\\U00000062`", "a\\U00000062"},
				{"`a\\n`", "a\\n"},
				{"`a\\\\u0062`", "a\\\\u0062"},
				{"`a``b`", "a`b"},
				{"`a  b`", "a  b"},
			} {
				t.Run(tc.written, func(t *testing.T) {
					result, err := executor.Execute(t.Context(), "RETURN {"+tc.written+":1} AS m", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{map[string]interface{}{tc.key: int64(1)}}}, result.Rows)
				})
			}
			for _, written := range []string{"`a\\uZZZZ`", "`a\\u006`", "`a\\u0060b`"} {
				t.Run("malformed "+written, func(t *testing.T) {
					_, err := executor.Execute(t.Context(), "RETURN {"+written+":1} AS m", nil)
					require.Error(t, err)
				})
			}
		})
	}
}
