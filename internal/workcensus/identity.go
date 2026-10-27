package workcensus

import "fmt"

// One occurrence constructor for typed Go, syntax peers and lexical peers.
// Extent and node kind distinguish nested syntax sharing a start. Ownership is
// validated separately; another owner's removal cannot rename this occurrence.
func sourceOccurrenceID(file, kind, node string, start, end int) string {
	return fmt.Sprintf("%s/%s:%s@%d:%d", file, kind, node, start, end)
}
