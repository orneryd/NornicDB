package adminimport

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNumericWireSchemaSidecarRoundTrip(t *testing.T) {
	for _, format := range []struct {
		name, header, token string
		want                interface{}
	}{
		{"v1_large_float", `"version":1,`, "1152921504606847000", float64(1 << 60)},
		{"v1_out_of_range_float", `"version":1,`, "100000000000000000000", float64(1e20)},
		{"v2_exact_integer", `"version":2,`, "9007199254740993", int64(9007199254740993)},
	} {
		t.Run(format.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "schema.json")
			raw := []byte(fmt.Sprintf(`{%s"constraints":[{"name":"value_domain","type":"DOMAIN","label":"Value","properties":["v"],"allowed_values":[%s]}]}`, format.header, format.token))
			require.NoError(t, os.WriteFile(path, raw, 0600))
			for stage := 0; stage < 2; stage++ {
				engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
				require.NoError(t, applySchemaDefinition(engine, path))
				require.Equal(t, []interface{}{format.want}, engine.GetSchema().ExportDefinition().Constraints[0].AllowedValues)
				require.NoError(t, exportNornicSchemaDefinition(path, engine.GetSchema()))
			}
		})
	}
}
