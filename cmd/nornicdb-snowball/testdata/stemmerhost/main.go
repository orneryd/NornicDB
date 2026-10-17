// Command stemmerhost is a test --target for nornicdb-snowball. It answers
// `stemmerhost stemmer check [--timeout D] <manifest>` the way
// `nornicdb stemmer check` does, through the same stemmer.CheckManifest, and
// like nornicdb it links golang.org/x/text/unicode/norm. The package tests
// build it with different build settings to see which plugins it accepts.
//
// STEMMERHOST_MODE simulates other targets: "old" (a nornicdb without the
// stemmer command), "crash" and "hang".
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/orneryd/nornicdb/pkg/search/stemmer"
	_ "golang.org/x/text/unicode/norm"
)

func main() {
	switch os.Getenv("STEMMERHOST_MODE") {
	case "old":
		fmt.Fprintln(os.Stderr, `Error: unknown command "stemmer" for "nornicdb"`)
		os.Exit(1)
	case "crash":
		panic("stemmerhost: simulated crash")
	case "hang":
		time.Sleep(time.Hour)
	}
	if len(os.Args) < 3 || os.Args[1] != "stemmer" || os.Args[2] != "check" {
		fmt.Fprintln(os.Stderr, "usage: stemmerhost stemmer check [--timeout D] <manifest>")
		os.Exit(2)
	}
	flags := flag.NewFlagSet("check", flag.ExitOnError)
	timeout := flags.Duration("timeout", time.Minute, "")
	_ = flags.Parse(os.Args[3:])
	registration, err := stemmer.CheckManifest(flags.Arg(0), *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "stemmer plugin %s does not load: %v\n", flags.Arg(0), err)
		os.Exit(1)
	}
	fmt.Printf("Stemmer plugin %s %s loads: %s\n", registration.ID, registration.Version, registration.Path)
}
