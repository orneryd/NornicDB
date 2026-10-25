//go:build cyphernames

package cypher

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// This opt-in lab subject checks the real UI compiler and production build at
// the supported Node floor, alongside the narrower consumer harness.
func TestDecodedNameUIBuild(t *testing.T) {
	root, err := filepath.Abs("../..")
	require.NoError(t, err)
	command := exec.Command("python3", filepath.Join(root, "internal/testutil/cyphernames/run_ui_identity.py"), "--build-ui")
	output, err := command.CombinedOutput()
	t.Log(string(output))
	require.NoError(t, err)
}

// The public query outputs feed the actual browser component, API record
// builders and editor callbacks. Emitted statements then execute here.
func TestDecodedNameUIIdentity(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "r5_ui")
	executor := NewStorageExecutor(store)
	ctx := context.Background()
	results := []map[string]interface{}{}
	for _, query := range []string{"RETURN 7 AS `__proto__`", "UNWIND [1,2] AS n RETURN 0 AS `__gridId`", "RETURN 1 AS `nornicSelect`, 2 AS `__nodeId`, 3 AS `__nodeData`, 4 AS `constructor`, 5 AS ` `, 6 AS `\t`, 7 AS `a.b`", "RETURN 11 AS `a.b`, {b:99} AS a, 12 AS `constructor`, 13 AS `prototype`, 14 AS plain"} {
		result, err := executor.Execute(ctx, query, nil)
		require.NoError(t, err)
		data := []map[string]interface{}{}
		for _, row := range result.Rows {
			data = append(data, map[string]interface{}{"row": row, "meta": []interface{}{}})
		}
		results = append(results, map[string]interface{}{"columns": result.Columns, "data": data})
	}
	for i, labels := range [][]string{{"a:b"}, {"A", "B"}, {" "}, {"a`b"}} {
		binding := knowledgepolicy.DecayProfileBinding{Name: []string{"ui_colon", "ui_tuple", "ui_space", "ui_backtick"}[i], TargetLabels: labels, NoDecay: true, PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "property.name` ", NoDecay: true}}}
		require.NoError(t, store.GetSchema().CreateDecayProfileBinding(binding))
	}
	metadata, err := executor.Execute(ctx, "CALL kp.profiles()", nil)
	require.NoError(t, err)
	bindings := []map[string]interface{}{}
	for _, row := range metadata.Rows {
		binding := map[string]interface{}{}
		for i, column := range metadata.Columns {
			binding[column] = row[i]
		}
		bindings = append(bindings, binding)
	}
	root, err := filepath.Abs("../..")
	require.NoError(t, err)
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	manager, err := multidb.NewDatabaseManager(ctx, base, nil)
	if err != nil {
		require.NoError(t, base.Close())
	}
	require.NoError(t, err)
	fixtureowner.OwnChecked(t, manager)
	admin := NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
	admin.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	for _, query := range []string{"CREATE DATABASE role", "CREATE DATABASE role_"} {
		_, err = admin.Execute(ctx, query, nil)
		require.NoError(t, err)
	}
	databases, err := admin.Execute(ctx, "SHOW DATABASES", nil)
	require.NoError(t, err)
	databaseNames := []string{}
	for i, column := range databases.Columns {
		if column == "name" {
			for _, row := range databases.Rows {
				databaseNames = append(databaseNames, row[i].(string))
			}
		}
	}
	require.Contains(t, databaseNames, "role")
	nodeResult, err := executor.Execute(ctx, "CREATE (n:R5UI {value:7}) RETURN n, 0 AS `__nodeData`, 0 AS `__nodeId`, 0 AS `__gridId`, 9 AS `nornicSelect`", nil)
	require.NoError(t, err)
	input, err := json.Marshal(map[string]interface{}{"results": results, "bindings": bindings, "databaseNames": databaseNames,
		"nodeResult": map[string]interface{}{"columns": nodeResult.Columns, "data": []map[string]interface{}{{"row": nodeResult.Rows[0], "meta": []interface{}{}}}},
	})
	require.NoError(t, err)
	file := filepath.Join(t.TempDir(), "ui-input.json")
	require.NoError(t, os.WriteFile(file, input, 0600))
	command := exec.Command("python3", filepath.Join(root, "internal/testutil/cyphernames/run_ui_identity.py"), file)
	output, runErr := command.Output()
	if exit, ok := runErr.(*exec.ExitError); ok {
		t.Log(string(exit.Stderr))
	}
	var observed struct {
		Commands []struct {
			Kind, Query, Name, Key, EdgeType string
			Labels                           []string
			Parameters                       map[string]interface{}
		}
		Failures     []struct{ Name, Message string }
		Observations []string
	}
	require.NoError(t, json.Unmarshal(output, &observed), string(output))
	for _, failure := range observed.Failures {
		t.Errorf("UI consumer %s: %s", failure.Name, failure.Message)
	}
	require.NoError(t, runErr)
	require.NotEmpty(t, observed.Observations)
	t.Logf("UI semantic observations (%d): %v", len(observed.Observations), observed.Observations)
	_, err = executor.Execute(ctx, "CREATE DECAY PROFILE ui_anchor OPTIONS {function:'exponential',scope:'NODE',scoreFrom:'CUSTOM',scoreFromProperty:'old'}", nil)
	require.NoError(t, err)
	for _, statement := range observed.Commands {
		t.Run(statement.Kind+statement.Name+statement.Key, func(t *testing.T) {
			if statement.Kind == "resolve" {
				return
			} // Dedicated list/edge public consumer test verifies exact target lookup.
			_, err := executor.Execute(ctx, statement.Query, statement.Parameters)
			require.NoError(t, err, statement.Query)
			bundles, actualBindings := store.GetSchema().ShowDecayProfiles()
			if statement.Kind == "anchor" || statement.Kind == "grid-anchor" {
				for _, bundle := range bundles {
					if bundle.Name == "ui_anchor" {
						require.Equal(t, statement.Key, bundle.ScoreFromProperty)
					}
				}
			}
			if statement.Kind == "binding" {
				found := false
				for _, binding := range actualBindings {
					if binding.Name == statement.Name {
						found = true
						require.Equal(t, statement.Labels, binding.TargetLabels)
						require.Equal(t, statement.EdgeType, binding.TargetEdgeType)
						require.Equal(t, "property.name` ", binding.PropertyRules[0].PropertyPath)
					}
				}
				require.True(t, found)
			}
		})
	}
}
