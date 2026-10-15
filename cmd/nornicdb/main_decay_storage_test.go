package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestDecayCommandsReadAccessThroughWALStorage(t *testing.T) {
	for _, name := range []string{"stats", "suppress"} {
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			cfg := nornicdb.DefaultConfig()
			cfg.Database.DataDir = directory
			cfg.Memory.DecayEnabled = true
			db, err := nornicdb.Open(directory, cfg)
			require.NoError(t, err)
			// Close setup storage before the command reopens it, including on a
			// failed setup assertion.
			t.Cleanup(func() { _ = db.Close() })
			base := unwrapBadgerEngine(db.GetBaseStorageForManager())
			require.NotNil(t, base)
			schema := db.GetStorage().GetSchema()
			require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "recent_access", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromLastAccessed, Enabled: true, DecayEnabled: true}))
			require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "recent_doc", TargetLabels: []string{"Doc"}, ProfileRef: "recent_access"}))
			now := time.Now()
			old := now.Add(-72 * time.Hour)
			_, err = db.GetStorage().CreateNode(&storage.Node{ID: "recent:id", Labels: []string{"Doc"}, Properties: map[string]interface{}{"body": "still visible"}, CreatedAt: old, UpdatedAt: old})
			require.NoError(t, err)
			require.NoError(t, base.PutAccessMeta("nornic:recent:id", &knowledgepolicy.AccessMetaEntry{TargetID: "nornic:recent:id", TargetScope: knowledgepolicy.ScopeNode, Fixed: knowledgepolicy.AccessMetaFixedFields{AccessCount: 3, LastAccessedAt: now.UnixNano()}}))
			require.NoError(t, db.Close())

			var output bytes.Buffer
			cmd := &cobra.Command{Use: "decay " + name}
			cmd.SetContext(context.Background())
			manager, err := localization.NewManager([]language.Tag{language.AmericanEnglish}, nil)
			require.NoError(t, err)
			setCommandLocalizer(cmd, manager)
			cmd.SetOut(&output)
			cmd.SetErr(&output)
			cmd.Flags().String("data-dir", directory, "")
			cmd.Flags().Float64("threshold", 0.1, "")
			if name == "stats" {
				require.NoError(t, runDecayStats(cmd, nil), output.String())
				require.Contains(t, output.String(), "1.0000", "recent access must determine the reported average")
			} else {
				require.NoError(t, runDecaySuppress(cmd, nil), output.String())
				require.Contains(t, output.String(), "Above threshold:      1")
				require.Contains(t, output.String(), "Newly suppressed:     0")
			}
		})
	}
}
