package fabric

import (
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

func TestFabricBraceConsumerProperty(t *testing.T) {
	gaps := []string{" ", "\t", "\n", " /* { : , } */ ", " // { }\n"}
	for _, gap := range gaps {
		for _, entries := range []string{"k: v", "k: v, other: v", "`k-x`: v", "a$b: v", "bad", "1bad: v", "k:", "k: v,", ",k: v", "k: v,,other: v", "k v", "'k': v", "\"k\": v"} {
			spec := strings.ReplaceAll(entries, " ", gap)
			query := "WITH 7 AS v RETURN {" + gap + spec + gap + "} AS out"
			t.Run(query, func(t *testing.T) {
				oracle := cypherantlr.Validate(query) == nil
				input := cyphertext.NormalizeSource(query)
				owned := cypherantlr.OriginalBraceAnalysis(input, false, nil).Admission() == nil
				mapSource := input.Window(len("WITH 7 AS v RETURN "), len(query)-len(" AS out"))
				rows, accepted := projectInputRowsAsMaps(&ResultStream{Columns: []string{"v"}, Rows: [][]interface{}{{int64(7)}}}, mapSource)
				if owned != oracle || accepted != oracle {
					t.Fatalf("owner=%v Fabric=%v ANTLR=%v", owned, accepted, oracle)
				}
				if accepted && len(rows) != 1 {
					t.Fatalf("rows=%v", rows)
				}
			})
		}
		for _, template := range []string{"CALL%s{ RETURN {k: 1} AS out } RETURN out", "CALL%s()%s{ RETURN 1 AS out } RETURN out", "WITH {} AS call RETURN call%s{.k} AS out"} {
			query := strings.ReplaceAll(template, "%s", gap)
			t.Run(fmt.Sprint(query), func(t *testing.T) {
				if err := cypherantlr.Validate(query); err != nil {
					t.Fatal(err)
				}
				blocks, err := extractTopLevelCallBlocks(query, nil)
				if err != nil {
					t.Fatal(err)
				}
				bodies := 0
				for _, record := range cypherantlr.OriginalBraceAnalysis(cyphertext.NormalizeSource(query), false, nil).Records() {
					if record.Role == cypherantlr.SubqueryBrace {
						bodies++
					}
				}
				if len(blocks) != bodies {
					t.Fatalf("Fabric=%d owner=%d", len(blocks), bodies)
				}
			})
		}
	}
}
