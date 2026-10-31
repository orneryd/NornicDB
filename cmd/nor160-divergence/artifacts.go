// Author: Codex (implementation sub-agent), 2026-10-26.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// The receipt is written after both reports and published by one directory rename.
// Sizes detect truncated deliveries without rereading hundreds of MB per cache hit.
func completeArtifact(dir, key, tool, input string) (Receipt, error) {
	var r Receipt
	data, err := os.ReadFile(filepath.Join(dir, "receipt.json"))
	if err != nil {
		return r, err
	}
	if err = json.Unmarshal(data, &r); err != nil {
		return r, err
	}
	if r.Key != key || r.ToolDigest != tool || r.TreeDigest != input || len(r.Counts) == 0 || len(r.IDDigest) != len(r.Counts) {
		return r, fmt.Errorf("receipt does not identify a complete matching analysis")
	}
	for name, size := range map[string]int64{"report.json": r.ReportJSONBytes, "report.md": r.ReportMarkdownBytes} {
		if size <= 0 {
			return r, fmt.Errorf("receipt has no completed size for %s", name)
		}
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			return r, err
		}
		if !info.Mode().IsRegular() || info.Size() != size {
			return r, fmt.Errorf("%s is incomplete: expected %d bytes, found %d", name, size, info.Size())
		}
	}
	return r, nil
}
