package localization

import "fmt"

const (
	MessageNornicDBCLIStemmerShort            MessageID = "nornicdbcli.stemmer.short"
	MessageNornicDBCLIStemmerCheckShort       MessageID = "nornicdbcli.stemmer.check.short"
	MessageNornicDBCLIFlagStemmerCheckTimeout MessageID = "nornicdbcli.flag.stemmer_check_timeout"
	MessageNornicDBCLIStemmerCheckOK          MessageID = "nornicdbcli.stemmer.check.ok"
	MessageNornicDBCLIStemmerCheckFailed      MessageID = "nornicdbcli.stemmer.check_failed"
	MessageNornicDBCLIStemmerCheckTimedOut    MessageID = "nornicdbcli.stemmer.check_timed_out"
)

// NornicDBCLIStemmerShort is the `nornicdb stemmer` command summary.
func NornicDBCLIStemmerShort() Message {
	return nornicDBCLIText(MessageNornicDBCLIStemmerShort, "Stemmer plugin operations")
}

// NornicDBCLIStemmerCheckShort is the `nornicdb stemmer check` command summary.
func NornicDBCLIStemmerCheckShort() Message {
	return nornicDBCLIText(MessageNornicDBCLIStemmerCheckShort, "Check that a stemmer plugin loads into this binary, as it would at startup")
}

// NornicDBCLIFlagStemmerCheckTimeout is the `stemmer check --timeout` flag help.
func NornicDBCLIFlagStemmerCheckTimeout() Message {
	return nornicDBCLIText(MessageNornicDBCLIFlagStemmerCheckTimeout, "Maximum time to load the plugin")
}

// NornicDBCLIStemmerCheckOK reports a plugin that loaded and registered.
func NornicDBCLIStemmerCheckOK(id, version, path string) Message {
	return cliOutput(MessageNornicDBCLIStemmerCheckOK, "Stemmer plugin "+id+" "+version+" loads: "+path, map[string]any{"ID": id, "Version": version, "Path": path})
}

// NornicDBCLIStemmerCheckFailed reports a plugin that this binary refuses.
// Cause is an untranslated loader diagnostic.
func NornicDBCLIStemmerCheckFailed(manifest string, cause error) Message {
	return cliOperationError(MessageNornicDBCLIStemmerCheckFailed, fmt.Sprintf("stemmer plugin %s does not load: %v", manifest, cause), cause, map[string]any{"Manifest": manifest})
}

// NornicDBCLIStemmerCheckTimedOut reports a plugin whose loading did not finish.
func NornicDBCLIStemmerCheckTimedOut(manifest, timeout string) Message {
	return cliOutput(MessageNornicDBCLIStemmerCheckTimedOut, "stemmer plugin "+manifest+" did not finish loading within "+timeout, map[string]any{"Manifest": manifest, "Timeout": timeout})
}
