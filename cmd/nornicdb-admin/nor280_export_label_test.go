package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/adminimport"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

// The real offline command must expose the exporter failure, stable exit class,
// and localized delimiter guidance without leaving a successful CSV package.
func TestNOR280ExportCommandRejectsUnrepresentableLabels(t *testing.T) {
	for _, tc := range []struct{ name, label, delimiter string }{
		{"default", "A;B", ";"},
		{"custom", "A|B", "|"},
		{"surrounding whitespace", "A\n", ";"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir, out := t.TempDir(), filepath.Join(t.TempDir(), "export")
			engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dataDir))
			require.NoError(t, err)
			fixtureowner.Own(t, engine)
			ns := storage.NewNamespacedEngine(engine, "selected")
			_, err = ns.CreateNode(t.Context(), &storage.Node{ID: "one", Labels: []string{tc.label}})
			require.NoError(t, err)
			require.NoError(t, engine.Close(), "the offline command opens the completed source database itself")
			manager, err := localization.NewManager([]language.Tag{language.EuropeanSpanish}, nil)
			require.NoError(t, err)
			root := newTestAdminRoot(manager)
			root.SilenceErrors, root.SilenceUsage = true, true
			output := new(bytes.Buffer)
			root.SetOut(output)
			root.SetErr(output)
			root.SetArgs([]string{"database", "export", "neo4j-csv", "selected", "--data-dir", dataDir, "--to-path", out, "--array-delimiter", tc.delimiter})
			err = root.Execute()
			require.Error(t, err)
			var exportErr *adminimport.Error
			require.ErrorAs(t, err, &exportErr)
			require.Equal(t, adminimport.ExitUnsupported, exitCodeForError(err))
			for _, tag := range []language.Tag{language.AmericanEnglish, language.EuropeanSpanish, language.MustParse("en-XA")} {
				renderer, buildErr := localization.NewManager([]language.Tag{tag}, nil)
				require.NoError(t, buildErr)
				rendered := renderCommandError(renderer, err)
				require.Contains(t, rendered, strconv.Quote(tc.label))
				require.NotContains(t, rendered, "\n", "quoted data must keep the diagnostic on one line")
				if strings.Contains(tc.label, tc.delimiter) {
					require.Contains(t, rendered, "--array-delimiter")
				}
				if tag == language.EuropeanSpanish {
					require.Contains(t, rendered, "etiqueta")
				}
				if tag == language.MustParse("en-XA") {
					require.True(t, strings.HasPrefix(rendered, "[!! "))
				}
			}
			entries, readErr := os.ReadDir(out)
			require.NoError(t, readErr)
			require.Empty(t, entries)
		})
	}
}
