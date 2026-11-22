package server

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// lostackReplayJournal replays walDir alone (no snapshot) with the product's
// journal reader and returns the R37L names it yields, with the replay result.
func lostackReplayJournal(walDir string) (map[string]int, string, error) {
	ctx := context.Background()
	engine, result, err := storage.RecoverFromWALWithResult(ctx, walDir, "")
	summary := fmt.Sprintf("{applied:%d skipped:%d failed:%d errors:%d}", result.Applied, result.Skipped, result.Failed, len(result.Errors))
	if len(result.Errors) > 0 {
		summary += fmt.Sprintf(" first-error=%+v", result.Errors[0])
	}
	names := map[string]int{}
	if engine == nil {
		return names, summary, err
	}
	defer engine.Close()
	nodes, listErr := engine.AllNodes(ctx)
	if err == nil {
		err = listErr
	}
	for _, node := range nodes {
		for _, label := range node.Labels {
			if label == "R37L" {
				names[fmt.Sprint(node.Properties["name"])]++
			}
		}
	}
	return names, summary, err
}
