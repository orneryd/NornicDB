// Close-out lane L8: helper copied from the pin branch's
// zz_closeout_l8_owner_test.go (which uses pin-only internals) so the
// round-6 owner step runs on v1.5.1. Test-only, not for merge.
package cypher

import (
	"encoding/json"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
)

func l8Owner(t *testing.T, lead, step string, fields map[string]any) {
	t.Helper()
	fields["lead"], fields["step"], fields["parser"] = lead, step, config.GetParserType()
	data, _ := json.Marshal(fields)
	t.Logf("L8_OWNER %s", data)
}
