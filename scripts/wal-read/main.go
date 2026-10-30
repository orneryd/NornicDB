// Command wal-read prints a NornicDB WAL directory as one JSON object per
// visited entry, for reading a framed, mixed WAL stream by hand.
//
// A WAL directory (see pkg/storage) is not one compressed archive: it is a set
// of sealed segments plus the active wal.log, each a sequence of
// length-prefixed frames whose payload may be JSON control records or typed
// (optionally zstd-compressed) records. This command therefore never treats a
// file as a single zstd stream and never shells out to a compressor: it walks
// the directory through storage.VisitWALEntriesAfterFromDir, the same reader
// recovery and db.txlog use, and renders each entry's payload with
// storage.EntryPayloadJSON, the WAL's typed display owner.
//
// Usage:
//
//	go run ./scripts/wal-read -dir /path/to/wal [-from N] [-to N]
//
// Each output line is one JSON object with seq, timestamp (RFC 3339Nano, UTC),
// database, kind and payload. payload is the entry's JSON value, not a
// JSON-escaped string. Bounds are inclusive: -from is the lowest sequence to
// print (default 1) and -to the highest (default 0, no upper bound). Entries
// may be out of sequence on disk, so a sequence above -to never stops the
// scan; it is only skipped.
//
// The command is read-only: it opens no WAL for writing, creates no
// directory, and never invents a transaction id for an entry that has none.
// This scope holds only published storage API, so it observes exactly what a
// recovery reader observes. That reader reports a torn end-of-file as an
// incomplete write and stops without an error; correcting or masking that
// limit belongs to pkg/storage, not here.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// defaultFromSeq is the lowest WAL sequence number: sequences start at 1.
const defaultFromSeq = 1

// bounds are the validated inclusive sequence bounds of a read. A max of 0
// means no upper bound; scanAfter is what the WAL visitor is asked for, so a
// read without -from does not assume the WAL starts at 1.
type bounds struct {
	max       uint64
	scanAfter uint64
}

// record is one output line: a WAL entry as the reader sees it.
type record struct {
	Seq       uint64          `json:"seq"`
	Timestamp string          `json:"timestamp"`
	Database  string          `json:"database"`
	Kind      string          `json:"kind"`
	Payload   json.RawMessage `json:"payload"`
}

// retained returns whether an entry's sequence is inside the inclusive bounds,
// scanAfter already having excluded everything below the lower bound.
func (b bounds) retained(seq uint64) bool {
	return seq > b.scanAfter && (b.max == 0 || seq <= b.max)
}

// errUsage reports a rejected argument list; the run then exits with
// exitUsage.
var errUsage = errors.New("invalid arguments")

// exit codes. Usage errors are separated from read and conversion failures so
// a script can tell them apart.
const (
	exitOK = 0
	// exitRead covers directory, WAL read, conversion and output failures.
	exitRead = 1
	// exitUsage covers an invalid argument or missing directory.
	exitUsage = 2
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run executes the command and returns its exit code.
func run(args []string, stdout io.Writer, stderr io.Writer) int {
	options, err := parseArgs(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		fmt.Fprintf(stderr, "wal-read: %v\n", err)
		return exitUsage
	}
	if err := requireDirectory(options.dir); err != nil {
		fmt.Fprintf(stderr, "wal-read: %v\n", err)
		return exitUsage
	}
	if err := writeEntries(options.dir, options.bounds, stdout); err != nil {
		fmt.Fprintf(stderr, "wal-read: %v\n", err)
		return exitRead
	}
	return exitOK
}

// requireDirectory reports a missing path or a path that is not a directory,
// so an empty WAL is never confused with a wrong or absent -dir.
func requireDirectory(dir string) error {
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%q is not a directory", dir)
	}
	return nil
}

// options are the validated command arguments.
type options struct {
	dir    string
	bounds bounds
}

// parseArgs parses and validates the command line. -h is reported as
// flag.ErrHelp after its usage text has been written.
func parseArgs(args []string, stderr io.Writer) (options, error) {
	flags := flag.NewFlagSet("wal-read", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dir := flags.String("dir", "", "existing WAL directory to read (segments/ and wal.log)")
	from := flags.Int64("from", defaultFromSeq, "lowest WAL sequence to print (inclusive)")
	to := flags.Int64("to", 0, "highest WAL sequence to print (inclusive; 0 for no upper bound)")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "usage: go run ./scripts/wal-read -dir /path/to/wal [-from N] [-to N]")
		fmt.Fprintln(stderr, "prints one JSON object per WAL entry: seq, timestamp, database, kind, payload")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			flags.Usage()
			return options{}, flag.ErrHelp
		}
		return options{}, fmt.Errorf("%w: %v", errUsage, err)
	}
	if flags.NArg() > 0 {
		return options{}, fmt.Errorf("%w: unexpected argument %q", errUsage, flags.Arg(0))
	}
	if *dir == "" {
		return options{}, fmt.Errorf("%w: -dir is required", errUsage)
	}
	span, err := parseBounds(*from, *to)
	if err != nil {
		return options{}, err
	}
	return options{dir: filepath.Clean(*dir), bounds: span}, nil
}

// parseBounds validates inclusive sequence bounds. -from is at least 1, -to is
// non-negative, and an upper bound below the lower bound is rejected rather
// than silently printing nothing.
func parseBounds(from, to int64) (bounds, error) {
	if from < defaultFromSeq {
		return bounds{}, fmt.Errorf("%w: -from must be at least %d, got %d", errUsage, defaultFromSeq, from)
	}
	if to < 0 {
		return bounds{}, fmt.Errorf("%w: -to must not be negative, got %d", errUsage, to)
	}
	if to > 0 && to < from {
		return bounds{}, fmt.Errorf("%w: -to %d is below -from %d", errUsage, to, from)
	}
	// The visitor reports entries after a sequence, so ask it for everything
	// before the lower bound; retained() drops those.
	return bounds{max: uint64(to), scanAfter: uint64(from - 1)}, nil
}

// writeEntries writes one JSON line per WAL entry of dir inside the bounds.
// The visitor streams entries, and each line is encoded as it is visited, so
// memory stays bounded by one entry rather than by the WAL length. A payload
// that cannot be rendered fails the read instead of producing a partial line.
func writeEntries(dir string, span bounds, out io.Writer) error {
	buffered := bufio.NewWriterSize(out, 64*1024)
	encoder := json.NewEncoder(buffered)
	encoder.SetEscapeHTML(false)
	err := storage.VisitWALEntriesAfterFromDir(dir, span.scanAfter, func(entry storage.WALEntry) error {
		if !span.retained(entry.Sequence) {
			return nil
		}
		line, err := renderEntry(entry)
		if err != nil {
			return err
		}
		return encoder.Encode(line)
	})
	if err != nil {
		return err
	}
	return buffered.Flush()
}

// renderEntry renders one visited entry as an output record. The payload comes
// from storage.EntryPayloadJSON, the WAL's typed display owner; there is no
// second decoder here.
func renderEntry(entry storage.WALEntry) (record, error) {
	payload, err := storage.EntryPayloadJSON(entry)
	if err != nil {
		return record{}, err
	}
	line := record{
		Seq:       entry.Sequence,
		Timestamp: formatTimestamp(entry.Timestamp),
		Database:  entry.Database,
		Kind:      string(entry.Operation),
		Payload:   json.RawMessage(payload),
	}
	return line, nil
}

// formatTimestamp renders an entry time as RFC 3339Nano in UTC. RFC 3339
// spells years with four digits, so the zero time (and any other year outside
// 0–9999) is rendered as the empty string rather than a value no RFC 3339
// reader accepts.
func formatTimestamp(when time.Time) string {
	if when.IsZero() {
		return ""
	}
	utc := when.UTC()
	year := utc.Year()
	if year < 0 || year > 9999 {
		return ""
	}
	return utc.Format(time.RFC3339Nano)
}
