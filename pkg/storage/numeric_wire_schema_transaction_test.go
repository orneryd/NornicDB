package storage

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func TestNumericWireSchemaTransactionRewrite(t *testing.T) {
	for _, format := range []struct {
		name, header, token string
		want, rejected      interface{}
	}{
		{"v0_large_float", "", "1152921504606847000", float64(1 << 60), int64(1152921504606847000)},
		{"v0_out_of_range_float", "", "100000000000000000000", float64(1e20), "100000000000000000000"},
		{"v1_large_float", `"version":1,`, "1152921504606847000", float64(1 << 60), int64(1152921504606847000)},
		{"v1_out_of_range_float", `"version":1,`, "100000000000000000000", float64(1e20), "100000000000000000000"},
		{"v2_exact_integer", `"version":2,`, "9007199254740993", int64(9007199254740993), int64(9007199254740992)},
	} {
		for _, mutation := range []string{"native_backup", "schema", "knowledge_policy"} {
			t.Run(format.name+"/"+mutation, func(t *testing.T) {
				dir := t.TempDir()
				open := func(path string) *BadgerEngine {
					engine, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(path))
					require.NoError(t, err)
					return engine
				}
				engine := open(dir)
				raw := []byte(fmt.Sprintf(`{%s"constraints":[{"name":"value_domain","type":"DOMAIN","entity_type":"NODE","label":"Value","properties":["v"],"allowed_values":[%s]}]}`, format.header, format.token))
				require.NoError(t, engine.withUpdate(updateRequest{}, func(txn *badger.Txn) error { return txn.Set(schemaKey("test"), raw) }))
				require.NoError(t, engine.Close())
				engine = open(dir)
				check := func(engine *BadgerEngine, stage string) {
					t.Helper()
					require.Equal(t, []interface{}{format.want}, engine.GetSchemaForNamespace("test").ExportDefinition().Constraints[0].AllowedValues)
					require.NoError(t, writeNumericWireSchemaValue(engine, "node", false, "test:"+stage+"-accepted", format.want))
					var violation *ConstraintViolationError
					require.ErrorAs(t, writeNumericWireSchemaValue(engine, "node", false, "test:"+stage+"-rejected", format.rejected), &violation)
					require.Equal(t, ConstraintDomain, violation.Type)
				}
				check(engine, "original")
				if mutation != "native_backup" {
					tx, err := engine.BeginTransaction()
					require.NoError(t, err)
					defer tx.Rollback()
					require.NoError(t, tx.SetNamespace("test"))
					if mutation == "schema" {
						view, err := tx.Schema(OrdinarySchemaChanges)
						require.NoError(t, err)
						require.NoError(t, view.AddPropertyIndex("other_index", "Other", []string{"v"}))
						require.NoError(t, tx.StageSchemaChanges())
					} else {
						view, err := tx.Schema(KnowledgePolicyChanges)
						require.NoError(t, err)
						require.NoError(t, view.CreatePromotionProfile(validPromoProfile("other_profile")))
					}
					require.NoError(t, tx.Commit())
					var envelope struct {
						Version int `json:"version"`
					}
					require.NoError(t, engine.withView(func(txn *badger.Txn) error {
						item, err := txn.Get(schemaKey("test"))
						if err != nil {
							return err
						}
						return item.Value(func(raw []byte) error { return json.Unmarshal(raw, &envelope) })
					}))
					require.Equal(t, 2, envelope.Version)
				}
				require.NoError(t, engine.Close())
				engine = open(dir)
				check(engine, "restart")
				if mutation == "schema" {
					require.True(t, engine.GetSchemaForNamespace("test").MaintainsPropertyIndex("Other", "v"))
				} else if mutation == "knowledge_policy" {
					require.Len(t, engine.GetSchemaForNamespace("test").ExportDefinition().PromotionProfiles, 1)
				}
				backup := filepath.Join(t.TempDir(), "schema.backup")
				require.NoError(t, engine.Backup(backup))
				restoredDir := t.TempDir()
				restored := open(restoredDir)
				require.NoError(t, restored.Restore(backup))
				check(restored, "restored")
				require.NoError(t, restored.Close())
				restored = open(restoredDir)
				check(restored, "restored-restart")
			})
		}
	}
}
