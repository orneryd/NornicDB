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
	"github.com/orneryd/nornicdb/pkg/cypher"
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
			base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
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

// Rows before the failing one: written or rolled back? The CSV holds three
// small rows (one 2,048-d named vector) and a fourth with seven, imported in
// one batch (ChunkSize 10) and row by row (ChunkSize 1). Read back by node
// reads and a Cypher count.
func TestCloseoutL4Named64KCSVImportRowsBeforeError(t *testing.T) {
	const dims = 2048
	vector := func(i int) string {
		values := make([]string, dims)
		for j := range values {
			values[j] = fmt.Sprintf("%.4f", float64((i+1)*(j+3)%97)/97)
		}
		return strings.Join(values, ";")
	}
	for _, chunk := range []int{10, 1} {
		dir := t.TempDir()
		header := []string{"id:ID(Doc)", "title:string"}
		for i := 0; i < 7; i++ {
			header = append(header, fmt.Sprintf(":EMBEDDING(v%d)", i))
		}
		rows := []string{strings.Join(header, ",")}
		for r, id := range []string{"small1", "small2", "small3", "big"} {
			cells := []string{id, "doc"}
			for i := 0; i < 7; i++ {
				if r < 3 && i > 0 {
					cells = append(cells, "")
				} else {
					cells = append(cells, vector(i))
				}
			}
			rows = append(rows, strings.Join(cells, ","))
		}
		path := filepath.Join(dir, "nodes.csv")
		if err := os.WriteFile(path, []byte(strings.Join(rows, "\n")+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
		report, importErr := ImportFull(context.Background(), base, Options{DatabaseName: "l4", NodeSources: []string{"Doc=" + path}, DataDir: dir, ChunkSize: chunk, Now: fixedImportTime})
		engine := storage.NewNamespacedEngine(base, "l4")
		present := []string{}
		for _, id := range []string{"small1", "small2", "small3", "big"} {
			if node, err := engine.GetNode(context.Background(), storage.NodeID(id)); err == nil && node != nil {
				present = append(present, id)
			}
		}
		cy, cyErr := cypher.NewStorageExecutor(engine).Execute(context.Background(), "MATCH (n) RETURN count(n)", nil)
		cyCount := "?"
		if cy != nil && len(cy.Rows) == 1 {
			cyCount = fmt.Sprint(cy.Rows[0][0])
		}
		msg := fmt.Sprint(importErr)
		if len(msg) > 160 {
			msg = msg[:160]
		}
		data, _ := json.Marshal(map[string]any{"item": "CL4-NEW-NAMED-64K", "check": fmt.Sprintf("csv-rows-before-error/chunk=%d", chunk), "pass": true,
			"importError": msg, "reportNodesImported": report.NodesImported, "nodesPresent": present, "cypherCount": cyCount, "cypherError": fmt.Sprint(cyErr)})
		fmt.Fprintf(os.Stdout, "L4_RESULT %s\n", data)
	}
}
