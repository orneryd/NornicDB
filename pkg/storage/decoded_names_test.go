package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodedNameCompositeSchema(t *testing.T) {
	for _, name := range []string{" ", "  ", "\t"} {
		require.Equal(t, []string{name}, anyToStringSlice([]string{"", name}))
		require.Equal(t, []string{name}, anyToStringSlice([]interface{}{"", name, nil}))
		engine := newTestMemoryEngine(t)
		require.NoError(t, engine.GetSchema().AddFulltextIndex("decoded", []string{name}, []string{name}))
		composite := NewCompositeEngine(map[string]Engine{"db1": engine}, map[string]string{"db1": "db1"}, map[string]string{"db1": "read_write"})
		indexes := composite.GetSchema().GetIndexes()
		found := false
		for _, index := range indexes {
			entry := index.(map[string]interface{})
			if entry["name"] == "decoded" {
				found = true
				require.Equal(t, []string{name}, entry["labels"])
				require.Equal(t, []string{name}, entry["properties"])
			}
		}
		require.True(t, found)
	}
}
