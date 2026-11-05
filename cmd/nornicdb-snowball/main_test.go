package main

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search/stemmer"
)

// TestMain lets the test binary serve as a --target: run as
// `<test binary> stemmer check [--timeout D] <manifest>`, it checks the plugin
// with stemmer.CheckManifest, as `nornicdb stemmer check` does, and exits.
func TestMain(m *testing.M) {
	if len(os.Args) > 2 && os.Args[1] == "stemmer" && os.Args[2] == "check" {
		flags := flag.NewFlagSet("check", flag.ExitOnError)
		timeout := flags.Duration("timeout", time.Minute, "")
		_ = flags.Parse(os.Args[3:])
		registration, err := stemmer.CheckManifest(flags.Arg(0), *timeout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "stemmer plugin %s does not load: %v\n", flags.Arg(0), err)
			os.Exit(1)
		}
		fmt.Fprintln(os.Stdout, stemmer.CheckResultLine(registration))
		os.Exit(0)
	}
	os.Exit(m.Run())
}
