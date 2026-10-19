package search

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Live writers go through RefreshNode/RefreshEdge, which read storage after
// reserving their generation. A payload mutation decides what to index from
// an earlier read that a newer write can outdate (#1482), so outside this
// package only the offline eval sink may use one. Storage and adminimport
// have their own property-index IndexNode/RemoveNode, a different interface.
func TestLiveIndexWritersRefreshFromStorage(t *testing.T) {
	payload := regexp.MustCompile(`\.(IndexNode|RemoveNode|IndexNodeWithMutation|RemoveNodeWithMutation|IndexEdge|RemoveEdge|IndexRelationship|RemoveRelationship)\(`)
	allowed := map[string]bool{"eval/ir/nornic_sink.go": true}
	skipped := []string{"search/", "storage/", "adminimport/"}
	var found []string
	require.NoError(t, filepath.WalkDir("..", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		rel, err := filepath.Rel("..", path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		for _, prefix := range skipped {
			if strings.HasPrefix(rel, prefix) {
				return nil
			}
		}
		if allowed[rel] {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for number, line := range strings.Split(string(source), "\n") {
			if !strings.HasPrefix(strings.TrimSpace(line), "//") && payload.MatchString(line) {
				found = append(found, fmt.Sprintf("%s:%d: %s", rel, number+1, strings.TrimSpace(line)))
			}
		}
		return nil
	}))
	require.Empty(t, found, "live search writers must call RefreshNode/RefreshEdge")
}
