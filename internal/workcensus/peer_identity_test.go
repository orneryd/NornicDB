//go:build cgo

package workcensus

// Author: #1667 implementation lane (Codex). Shared language extraction must
// retain anonymous owner/occurrence identities through padded source controls.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPeerAnonymousOccurrenceIdentity(t *testing.T) {
	for _, language := range []string{"javascript", "typescript", "tsx"} {
		t.Run(language, func(t *testing.T) {
			dir := t.TempDir()
			first := "consume(function(){Observe(ctx)});"
			raw := first + "\n" + first + "\n"
			path := filepath.Join(dir, "peer.source")
			if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			c := Config{Dir: dir, Documents: []Document{{Path: "peer.source", Language: language}}, Selectors: []Selector{{Kind: "function", Node: "function", Reachability: "all"}}}
			base := mustExtract(t, c)
			if len(base.Members) != 2 {
				t.Fatal("anonymous peer population", len(base.Members))
			}
			retained := base.Members[0]
			for _, m := range base.Members {
				if m.Offset > retained.Offset {
					retained = m
				}
			}
			c.Overlay = map[string][]byte{path: []byte(strings.Repeat(" ", len(first)) + "\n" + first + "\n")}
			actual := mustExtract(t, c)
			if len(actual.Members) != 1 || actual.Members[0].ID != retained.ID || retained.SiteID == "" || actual.Members[0].SiteID != retained.SiteID {
				t.Fatal("anonymous peer identity reassigned after another owner removal")
			}
		})
	}
}
