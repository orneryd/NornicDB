// Command inventory emits a deterministic expanded openCypher TCK inventory.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/orneryd/nornicdb/testing/cypher/tck"
)

func main() {
	features := flag.String("features", "testing/cypher/tck/testdata/opencypher/features", "TCK features root")
	check := flag.String("check", "", "compare generated inventory with this file")
	flag.Parse()

	inv, err := tck.BuildInventory(*features)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var output bytes.Buffer
	enc := json.NewEncoder(&output)
	enc.SetIndent("", "  ")
	if err := enc.Encode(inv); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *check != "" {
		expected, err := os.ReadFile(*check)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if !bytes.Equal(expected, output.Bytes()) {
			fmt.Fprintf(os.Stderr, "TCK inventory differs from %s; review the corpus provenance before updating the baseline\n", *check)
			os.Exit(1)
		}
		return
	}
	if _, err := os.Stdout.Write(output.Bytes()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
