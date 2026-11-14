package nornicdb

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCypherRejectsUnpersistableSchemaKeysBeforeAcknowledgement(t *testing.T) {
	name := strings.Repeat("K", 70000)
	for _, query := range []struct{ kind, statement string }{
		{"label", "CREATE (:`" + name + "` {value:1})"},
		{"relationship_type", "CREATE (:A)-[:`" + name + "`]->(:B)"},
		{"property_name", "CREATE (:A {`" + name + "`:1})"},
	} {
		for _, async := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/async=%v", query.kind, async), func(t *testing.T) {
				cfg := DefaultConfig()
				cfg.Database.AsyncWritesEnabled = async
				cfg.Database.StrictDurability = !async
				cfg.Database.WALAutoCompactionEnabled = false
				cfg.Memory.DecayEnabled = false
				cfg.Memory.AutoLinksEnabled = false
				cfg.Memory.EmbeddingEnabled = false
				root := filepath.Join(t.TempDir(), "data")
				db, err := Open(root, cfg)
				require.NoError(t, err)
				t.Cleanup(func() { _ = db.Close() })
				_, err = db.ExecuteCypher(t.Context(), query.statement, nil)
				require.Error(t, err, "a statement that cannot be persisted must not be acknowledged")
				count, err := db.GetStorage().NodeCount(t.Context())
				require.NoError(t, err)
				require.Zero(t, count, "rejected relationship writes cannot leave their endpoints")
				require.NoError(t, db.Close())
				reopened, err := Open(root, cfg)
				require.NoError(t, err, "the rejected statement must not poison journal recovery")
				t.Cleanup(func() { _ = reopened.Close() })
				count, err = reopened.GetStorage().NodeCount(t.Context())
				require.NoError(t, err)
				require.Zero(t, count)
			})
		}
	}
}
