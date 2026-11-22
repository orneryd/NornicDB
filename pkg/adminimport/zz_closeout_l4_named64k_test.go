package adminimport

// v1.5.2 close-out L4, CL4-NEW-NAMED-64K: the admin CSV import of one node
// with k named embedding columns (:EMBEDDING(name)) of dimension d. Reports
// the importer's result for every k and the smallest k that fails.
// Test file only; not for merge.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestCloseoutL4Named64KCSVImport(t *testing.T) {
	for _, dims := range []int{1024, 2048} {
		firstFail := 0
		outcomes := []string{}
		for k := 1; k <= 16; k++ {
			dir := t.TempDir()
			header := []string{"id:ID(Doc)", "title:string"}
			row := []string{"d1", "doc"}
			for i := 0; i < k; i++ {
				header = append(header, fmt.Sprintf(":EMBEDDING(v%d)", i))
				values := make([]string, dims)
				for j := range values {
					values[j] = fmt.Sprintf("%.4f", float64((i+1)*(j+3)%97)/97)
				}
				row = append(row, strings.Join(values, ";"))
			}
			path := filepath.Join(dir, "nodes.csv")
			if err := os.WriteFile(path, []byte(strings.Join(header, ",")+"\n"+strings.Join(row, ",")+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			report, err := ImportFull(context.Background(), base, Options{DatabaseName: "l4", NodeSources: []string{"Doc=" + path}, DataDir: dir, ChunkSize: 10, Now: fixedImportTime})
			if err == nil {
				outcomes = append(outcomes, fmt.Sprintf("k=%d ok nodes=%d", k, report.NodesImported))
				continue
			}
			msg := err.Error()
			if len(msg) > 160 {
				msg = msg[:160]
			}
			outcomes = append(outcomes, fmt.Sprintf("k=%d error: %s", k, msg))
			if firstFail == 0 {
				firstFail = k
			}
			if k >= firstFail+1 {
				break
			}
		}
		data, _ := json.Marshal(map[string]any{"item": "CL4-NEW-NAMED-64K", "check": fmt.Sprintf("csv-import/d=%d", dims), "pass": true,
			"dims": dims, "firstFailingCount": firstFail, "outcomes": outcomes})
		fmt.Fprintf(os.Stdout, "L4_RESULT %s\n", data)
	}
}
