package main

// Close-out lane L8 (v1.5.2 verification campaign, 2026-11-22): the
// option-flag part of hunt lead L-H9-018. For every documented flag, the
// command line it appears on is rebuilt from the page (continuation lines
// joined); when that command is the nornicdb binary, the real command tree
// (registerServerCommands, as main builds it) is asked for the subcommand
// and the flag. Test-only, not for merge.

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cli"
	"github.com/orneryd/nornicdb/pkg/localization"
)

func l8CommandLine(lines []string, n int) string {
	i := n - 1
	for i > 0 && strings.HasSuffix(strings.TrimSpace(lines[i-1]), "\\") {
		i--
	}
	parts := []string{}
	for j := i; j < len(lines); j++ {
		text := strings.TrimSpace(lines[j])
		parts = append(parts, strings.TrimSuffix(text, "\\"))
		if !strings.HasSuffix(text, "\\") {
			break
		}
	}
	return strings.Join(parts, " ")
}

func TestL8_H9_018_CLIFlags(t *testing.T) {
	root := cli.NewRoot("nornicdb", nil, localization.NornicDBCLIRootShort(), localization.NornicDBCLIRootLong())
	root.SetContext(context.WithValue(context.Background(), commandLocalizerContextKey{}, &commandLocalization{}))
	registerServerCommands(root, nil)
	file, err := os.Open(filepath.Join("..", "..", "pkg", "server", "testdata", "l8", "h9-leads-raw-r1.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	counts := map[string]int{}
	for scanner.Scan() {
		cols := strings.Split(scanner.Text(), "\t")
		if len(cols) < 5 || cols[2] != "option-flag" {
			continue
		}
		item, name, documented := cols[1], cols[3], cols[4]
		where := documented[strings.LastIndex(documented, "=")+1:]
		parts := strings.Split(where, ":")
		data, _ := os.ReadFile(filepath.Join("..", "..", parts[0]))
		lines := strings.Split(string(data), "\n")
		n := 0
		for _, c := range parts[1] {
			n = n*10 + int(c-'0')
		}
		command := ""
		if n > 0 && n <= len(lines) {
			command = l8CommandLine(lines, n)
		}
		tokens := strings.Fields(command)
		for len(tokens) > 0 && strings.Contains(tokens[0], "=") && !strings.HasPrefix(tokens[0], "-") {
			tokens = tokens[1:]
		}
		for len(tokens) > 0 && (tokens[0] == "sudo") {
			tokens = tokens[1:]
		}
		first := ""
		if len(tokens) > 0 {
			first = filepath.Base(strings.ReplaceAll(tokens[0], "\\", "/"))
		}
		fields := map[string]any{"lead": "L-H9-018", "item": item, "flag": name, "documented": documented, "command": command, "program": first}
		class := ""
		if first == "nornicdb" || first == "nornicdb.exe" {
			path := []string{}
			for _, tok := range tokens[1:] {
				if strings.HasPrefix(tok, "-") {
					break
				}
				path = append(path, tok)
			}
			cmd, rest, findErr := root.Find(path)
			flag := strings.SplitN(strings.TrimPrefix(name, "--"), "=", 2)[0]
			found := false
			if cmd != nil {
				found = cmd.Flags().Lookup(flag) != nil || cmd.InheritedFlags().Lookup(flag) != nil || cmd.PersistentFlags().Lookup(flag) != nil
				fields["resolved_command"] = cmd.CommandPath()
			}
			fields["rest"], fields["find_err"] = rest, findErrString(findErr)
			if found {
				class = "nornicdb flag exists"
			} else {
				class = "nornicdb flag missing"
			}
		} else {
			class = "flag of another program"
		}
		fields["class"] = class
		counts[class]++
		line, _ := json.Marshal(fields)
		t.Logf("L8_CLI %s", line)
	}
	line, _ := json.Marshal(map[string]any{"lead": "L-H9-018", "counts": counts})
	t.Logf("L8_CLI_SUMMARY %s", line)
}

func findErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
