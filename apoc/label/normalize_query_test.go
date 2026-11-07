package label_test

import (
	"testing"

	"github.com/orneryd/nornicdb/apoc"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNormalizeRegisteredQueryPreservesUnicode(t *testing.T) {
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
	previousLookup := cypher.PluginFunctionLookup
	t.Cleanup(func() { cypher.PluginFunctionLookup = previousLookup })
	cypher.PluginFunctionLookup = func(name string) (*cypher.PluginFunctionHandler, bool) {
		if info, ok := apoc.GetInfo(name); ok {
			return info.Handler, true
		}
		if previousLookup != nil {
			return previousLookup(name)
		}
		return nil, false
	}
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			exec := cypher.NewStorageExecutor(base)
			result, err := exec.Execute(t.Context(), "RETURN apoc.label.normalize($label) AS normalized", map[string]interface{}{"label": " élan漢字 "})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"Élan漢字"}}, result.Rows)
		})
	}
}
