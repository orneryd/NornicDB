package cypher

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/internal/contextslot"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCourseChangeCredentialOutput(t *testing.T) {
	executor := newCourseChangeCredentialFixture(t)
	var logs bytes.Buffer
	executor.SetLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
	executor.SetSlowQueryThreshold(time.Nanosecond)
	const secret = "probe741928"
	for _, tail := range []string{
		"'probe741928' probe741928", `"probe741928" probe741928`, "`probe741928` probe741928",
		"probe741928 probe741928", "PASSWORD 'probe741928' probe741928",
		"FROM probe741928 TO probe629174", "'probe741928", "`probe741928",
	} {
		t.Run(fmt.Sprint(len(tail), "_", strings.IndexAny(tail, "'\"`")), func(t *testing.T) {
			query := "CREATE COMPOSITE DATABASE review_c ALIAS r FOR DATABASE nornic AT 'http://example.invalid' USER u PASSWORD " + tail
			_, err := executor.Execute(context.Background(), query, nil)
			require.Error(t, err)
			outputs := []string{err.Error(), logs.String()}
			var localized *localization.LocalizedError
			if errors.As(err, &localized) {
				outputs = append(outputs, localized.Message.Fallback, fmt.Sprint(localized.Message.Data))
			}
			for _, output := range outputs {
				if strings.Contains(output, secret) || strings.Contains(output, "probe629174") {
					t.Error("credential reached public diagnostic; details withheld")
				}
			}
		})
	}
}

func TestCourseChangeUseConsumers(t *testing.T) {
	for _, body := range []string{"CREATE (:CourseChange)", "CALL dbms.listConfig()", "CALL db.index.vector.createNodeIndex('v', 'N', 'v', 3, 'cosine')"} {
		want := QueryPermissionRequirements(t.Context(), "USE g "+body, RawSource)
		require.Equal(t, want, QueryPermissionRequirements(t.Context(), "USE GRAPH g "+body, RawSource), body)
		require.Equal(t, RegisteredProceduresForCalls(t.Context(), "USE g "+body, RawSource), RegisteredProceduresForCalls(t.Context(), "USE GRAPH g "+body, RawSource), body)
	}
}

func TestCourseChangeGraphNamedGraph(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	defer manager.Close()
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "graph"}))
	graph, err := manager.GetStorage("graph")
	require.NoError(t, err)
	_, err = graph.CreateNode(&storage.Node{ID: "course-change", Labels: []string{"Probe"}})
	require.NoError(t, err)
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	result, err := executor.Execute(t.Context(), "USE graph MATCH (n) RETURN n", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
}
func TestCourseChangeCredentialReaderProduct(t *testing.T) {
	executor := newCourseChangeCredentialFixture(t)
	for index, value := range []struct{ raw, decoded string }{
		{"'probe741928'", "probe741928"}, {`"probe741928"`, "probe741928"}, {"`probe741928`", "probe741928"},
		{"probe741928", "probe741928"}, {`'probe74\u0031928'`, "probe741928"}, {"`probe``741928`", "probe`741928"},
	} {
		ctx := contextslot.Ensure[cyphertext.CredentialSet](t.Context())
		query := "CREATE COMPOSITE DATABASE reader_product ALIAS r FOR DATABASE nornic AT 'http://example.invalid' USER u PASSWORD " + value.raw + " trailing"
		_, err := executor.executeCompositeCommand(ctx, query, compositeCreateCommand)
		require.Error(t, err)
		found := false
		for _, span := range contextslot.Get[cyphertext.CredentialSet](ctx).Spans {
			if span.Password && span.Raw == value.raw && span.Value == value.decoded && span.End-span.Start == len(value.raw) {
				found = true
			}
		}
		require.True(t, found, "case %d: reader must retain the raw span and its exact decoded value; details withheld", index)
	}
}

// newCourseChangeCredentialFixture preserves manager-before-owned-engines cleanup.
func newCourseChangeCredentialFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	t.Cleanup(func() { _ = manager.Close() })
	return executor
}
