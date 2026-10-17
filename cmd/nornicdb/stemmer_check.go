package main

import (
	"errors"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/search/stemmer"
	"github.com/spf13/cobra"
)

// defaultStemmerCheckTimeout bounds `nornicdb stemmer check` when --timeout is
// not given.
const defaultStemmerCheckTimeout = time.Minute

// newStemmerCommand returns `nornicdb stemmer` with its `check` subcommand.
//
// `stemmer check <manifest>` loads one stemmer plugin exactly as startup does
// (stemmer.CheckManifest runs stemmer.LoadManifest, the per-plugin step of
// startup's LoadDirBestEffort) and nothing else: it reads no configuration,
// opens no data directory, starts no listeners and makes no network calls.
// nornicdb-snowball runs it against the target server binary, so a packaged
// plugin is reported usable only when that binary's own loader accepts it.
func newStemmerCommand(text func(localization.Message) string) *cobra.Command {
	stemmerCmd := &cobra.Command{
		Use:   "stemmer",
		Short: text(localization.NornicDBCLIStemmerShort()),
	}
	checkCmd := &cobra.Command{
		Use:   "check <manifest.stemmer.json>",
		Short: text(localization.NornicDBCLIStemmerCheckShort()),
		Args:  cobra.ExactArgs(1),
		RunE:  runStemmerCheck,
	}
	checkCmd.Flags().Duration("timeout", defaultStemmerCheckTimeout, text(localization.NornicDBCLIFlagStemmerCheckTimeout()))
	stemmerCmd.AddCommand(checkCmd)
	return stemmerCmd
}

func runStemmerCheck(cmd *cobra.Command, args []string) error {
	manifestPath := args[0]
	timeout, err := cmd.Flags().GetDuration("timeout")
	if err != nil {
		return err
	}
	registration, err := stemmer.CheckManifest(manifestPath, timeout)
	if errors.Is(err, stemmer.ErrCheckTimedOut) {
		return newCommandError(cmd, localization.NornicDBCLIStemmerCheckTimedOut(manifestPath, timeout.String()), err)
	}
	if err != nil {
		return newCommandError(cmd, localization.NornicDBCLIStemmerCheckFailed(manifestPath, err), err)
	}
	commandPrintln(cmd, localization.NornicDBCLIStemmerCheckOK(registration.ID, registration.Version, registration.Path))
	// The result line is a protocol for nornicdb-snowball, not prose, so it is
	// not localized.
	cmd.Println(stemmer.CheckResultLine(registration))
	return nil
}
