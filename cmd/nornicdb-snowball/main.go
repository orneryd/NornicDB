package main

import (
	"fmt"
	"io"
	"os"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		fmt.Fprintln(stdout, "usage: nornicdb-snowball package --language <name> --id <plugin.id> --version <version> --module <dir> --source <file.go> --output <plugin.so> [--target <nornicdb binary>]")
		return 0
	}
	switch args[0] {
	case "package":
		opts, err := parsePackageArgs(args[1:])
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		if err := packageSnowball(opts); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		if opts.Target == "" {
			fmt.Fprintf(stdout, "packaged %s (unverified: no --target was given, so no nornicdb binary has checked that it loads; run `nornicdb stemmer check %s` with the server binary)\n", opts.ID, manifestPath(opts.Output))
			return 0
		}
		fmt.Fprintf(stdout, "packaged %s (verified: %s loads it)\n", opts.ID, opts.Target)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n", args[0])
		return 2
	}
}
