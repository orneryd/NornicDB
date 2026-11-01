// Post-run shape mapping uses the exact engine commit's public redaction/hash functions.
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"os"
	"path/filepath"
	"sort"
)

func main() {
	if len(os.Args) != 3 {
		panic("trace directory and output required")
	}
	files, e := filepath.Glob(os.Args[1] + "/queries-*.jsonl")
	if e != nil {
		panic(e)
	}
	groups := map[string]map[string]bool{}
	exact := map[string]bool{}
	count := 0
	for _, p := range files {
		f, e := os.Open(p)
		if e != nil {
			panic(e)
		}
		s := bufio.NewScanner(f)
		s.Buffer(make([]byte, 65536), 16*1024*1024)
		for s.Scan() {
			var q struct {
				Query string `json:"query"`
				Sha   string `json:"query_sha256"`
			}
			if e = json.Unmarshal(s.Bytes(), &q); e != nil {
				panic(e)
			}
			raw := fmt.Sprintf("%x", sha256.Sum256([]byte(q.Query)))
			if raw != q.Sha {
				panic("query hash mismatch")
			}
			shape := cypher.StatementShapeHash(cypher.RedactLiterals(q.Query))
			if groups[shape] == nil {
				groups[shape] = map[string]bool{}
			}
			groups[shape][raw] = true
			exact[raw] = true
			count++
		}
		if e = s.Err(); e != nil {
			panic(e)
		}
		f.Close()
	}
	rows := []map[string]any{}
	ambiguous := 0
	keys := []string{}
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, shape := range keys {
		hashes := []string{}
		for h := range groups[shape] {
			hashes = append(hashes, h)
		}
		sort.Strings(hashes)
		if len(hashes) > 1 {
			ambiguous++
		}
		rows = append(rows, map[string]any{"shapeHash": shape, "exactSha256": hashes, "unambiguous": len(hashes) == 1})
	}
	b, e := json.MarshalIndent(map[string]any{"traceSamples": count, "exactQueries": len(exact), "ambiguousShapes": ambiguous, "rows": rows, "attributionRule": "Attribute an engine shape only when the mapping is unambiguous; EXPLAIN/PROFILE plan hashes and unobserved shapes are not mapped. Client trace timing is separate from engine timing."}, "", "  ")
	if e != nil {
		panic(e)
	}
	if e = os.WriteFile(os.Args[2], append(b, '\n'), 0644); e != nil {
		panic(e)
	}
	if count == 0 {
		panic("no client query traces captured")
	}
}
