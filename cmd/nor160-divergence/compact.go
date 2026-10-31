// NOR-160 lab vendoring (lane, 31 Oct): a lab job returns only its output,
// and a full report (hundreds of MB) is over the result client's limit, so
// -print-report streams the completed report and prints one compact line per
// finding, plus the receipt.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

func printCompactReport(out string) error {
	var reportPath, receiptPath string
	_ = filepath.WalkDir(out, func(path string, entry os.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			switch entry.Name() {
			case "report.json":
				reportPath = path
			case "receipt.json":
				receiptPath = path
			}
		}
		return nil
	})
	if receiptPath != "" {
		data, err := os.ReadFile(receiptPath)
		if err != nil {
			return err
		}
		fmt.Printf("NOR160_DIVERGENCE_RECEIPT %s\n", compactJSON(data))
	}
	if reportPath == "" {
		return fmt.Errorf("no report.json under %s", out)
	}
	file, err := os.Open(reportPath)
	if err != nil {
		return err
	}
	defer file.Close()
	dec := json.NewDecoder(file)
	if err := expectDelim(dec, '{'); err != nil {
		return err
	}
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return err
		}
		if key != "sections" {
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return err
			}
			if key == "options" || key == "owners_sha256" || key == "commit" {
				fmt.Printf("NOR160_DIVERGENCE_META %v %s\n", key, compactJSON(skip))
			}
			continue
		}
		if err := expectDelim(dec, '['); err != nil {
			return err
		}
		for dec.More() {
			if err := printSection(dec); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return err
		}
	}
	return nil
}

func printSection(dec *json.Decoder) error {
	if err := expectDelim(dec, '{'); err != nil {
		return err
	}
	fmt.Println("NOR160_DIVERGENCE_SECTION_BEGIN")
	var name string
	var count json.RawMessage
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return err
		}
		switch key {
		case "name":
			if err := dec.Decode(&name); err != nil {
				return err
			}
		case "count":
			if err := dec.Decode(&count); err != nil {
				return err
			}
		case "findings":
			if err := expectDelim(dec, '['); err != nil {
				return err
			}
			for dec.More() {
				var finding map[string]any
				if err := dec.Decode(&finding); err != nil {
					return err
				}
				line, err := compactFinding(finding)
				if err != nil {
					return err
				}
				fmt.Printf("NOR160_DIVERGENCE_FINDING %s\n", line)
			}
			if _, err := dec.Token(); err != nil {
				return err
			}
		default:
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return err
			}
		}
	}
	if _, err := dec.Token(); err != nil {
		return err
	}
	fmt.Printf("NOR160_DIVERGENCE_SECTION_END %s %s\n", name, count)
	return nil
}

// compactFinding keeps kind, subject and every site (function, role, file,
// line) and adds a digest of the finding without id/line/column and with its
// sites sorted, the normalization of the earlier delta script.
func compactFinding(finding map[string]any) ([]byte, error) {
	normalized := stripPositions(finding).(map[string]any)
	if sites, ok := normalized["sites"].([]any); ok {
		keys := make([]string, len(sites))
		for i, site := range sites {
			data, _ := json.Marshal(site)
			keys[i] = string(data)
		}
		sort.Strings(keys)
		sorted := make([]any, len(keys))
		for i, key := range keys {
			var value any
			_ = json.Unmarshal([]byte(key), &value)
			sorted[i] = value
		}
		normalized["sites"] = sorted
	}
	data, err := json.Marshal(normalized)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(data)
	var sites []map[string]any
	if raw, ok := finding["sites"].([]any); ok {
		for _, site := range raw {
			if m, ok := site.(map[string]any); ok {
				sites = append(sites, map[string]any{"function": m["function"], "role": m["role"], "file": m["file"], "line": m["line"]})
			}
		}
	}
	return json.Marshal(map[string]any{"kind": finding["kind"], "subject": finding["subject"], "sites": sites, "digest": hex.EncodeToString(sum[:])})
}

func stripPositions(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			if key == "id" || key == "line" || key == "column" {
				continue
			}
			out[key] = stripPositions(item)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = stripPositions(item)
		}
		return out
	}
	return value
}

func expectDelim(dec *json.Decoder, want json.Delim) error {
	token, err := dec.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); !ok || delim != want {
		return fmt.Errorf("report: expected %q, got %v", want, token)
	}
	return nil
}

func compactJSON(data []byte) string {
	var value any
	if json.Unmarshal(data, &value) != nil {
		return string(data)
	}
	out, _ := json.Marshal(value)
	return string(out)
}

var _ = io.EOF
