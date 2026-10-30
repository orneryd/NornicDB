// convert_search_index_to_msgpack converts persisted search index files from gob to msgpack in place.
//
// Usage:
//
//	# Back up first, then run (default: data/test/search)
//	cp -r data/test/search data/test/search.bak
//	go run ./scripts/convert_search_index_to_msgpack --dir data/test/search
//
// After conversion, restart NornicDB; it will load the msgpack files and you won't need to rebuild indexes.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/orneryd/nornicdb/pkg/search"
)

func main() {
	dir := flag.String("dir", "data/test/search", "Search index root (e.g. data/test/search)")
	flag.Parse()

	if *dir == "" {
		fmt.Fprintln(os.Stderr, "missing --dir")
		flag.Usage()
		os.Exit(2)
	}

	if err := search.ConvertSearchIndexDirFromGobToMsgpack(*dir); err != nil {
		fmt.Fprintf(os.Stderr, "conversion failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Done. Restart NornicDB to use the converted indexes.")
}
