package server

// Hunt ground H9: documentation and release notes. The population
// (testdata/hunt/h9-population.tsv, built by
// sig/handoff/v152-verification/hunt/tools/build_h9.py) lists every documented
// option, procedure, endpoint, step and 1.5.0-1.5.2 changelog entry.
//
// This harness settles the mechanical part; the oracle is the documentation
// itself:
//   - option-env:  the documented NORNICDB_* name is read by the code (a string
//                  literal in a non-test Go file of the repository);
//   - option-yaml: the key's last segment is a yaml struct tag in the code;
//   - option-flag: the flag name is a string literal in cmd/;
//   - procedure:   the name is listed by SHOW PROCEDURES or SHOW FUNCTIONS on a
//                  server in the default configuration;
//   - endpoint:    the route answers something other than 404/405 to the
//                  documented method (placeholders filled with "nornic").
// Steps and changelog entries are done by hand by the hunter, as written.
//
// Run: go test ./pkg/server -run '^TestHuntH9DocumentedSurface$' -count=1 -timeout=30m -v

import (
	"bufio"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

type huntH9Item struct{ ID, Kind, Name, Where string }

func huntH9Load(t *testing.T) []huntH9Item {
	file, err := os.Open(filepath.Join("testdata", "hunt", "h9-population.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var items []huntH9Item
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for first := true; scanner.Scan(); first = false {
		if first {
			continue
		}
		cols := strings.Split(scanner.Text(), "\t")
		if len(cols) >= 4 {
			items = append(items, huntH9Item{cols[0], cols[1], cols[2], cols[3]})
		}
	}
	return items
}

// huntH9Source collects the string literals and yaml tags of the repository's
// non-test Go files (repo root = ../..).
func huntH9Source(t *testing.T) (literals map[string]bool, cmdLiterals map[string]bool, yamlTags map[string]bool) {
	literals, cmdLiterals, yamlTags = map[string]bool{}, map[string]bool{}, map[string]bool{}
	tagRE := regexp.MustCompile(`yaml:"([^",]+)`)
	root := filepath.Join("..", "..")
	fset := token.NewFileSet()
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			name := d.Name()
			if name == "node_modules" || name == ".git" || name == "testdata" || name == "ui" || name == "docs" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return nil
		}
		inCmd := strings.Contains(filepath.ToSlash(path), "/cmd/")
		ast.Inspect(file, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				if value, err := strconv.Unquote(lit.Value); err == nil {
					literals[value] = true
					if inCmd {
						cmdLiterals[value] = true
					}
					for _, m := range tagRE.FindAllStringSubmatch(value, -1) {
						yamlTags[m[1]] = true
					}
				}
			}
			if field, ok := n.(*ast.Field); ok && field.Tag != nil {
				for _, m := range tagRE.FindAllStringSubmatch(field.Tag.Value, -1) {
					yamlTags[m[1]] = true
				}
			}
			return true
		})
		return nil
	})
	return
}

func TestHuntH9DocumentedSurface(t *testing.T) {
	items := huntH9Load(t)
	literals, cmdLiterals, yamlTags := huntH9Source(t)
	base, err := os.MkdirTemp(huntEnv("HUNT_TMP", os.TempDir()), "hunt-h9-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(base)
	h := huntOpen(t, filepath.Join(base, "store"), huntOptions{QueryCache: true})
	defer h.Close(t)
	registered := map[string]bool{}
	for _, q := range []string{"SHOW PROCEDURES YIELD name RETURN name", "SHOW FUNCTIONS YIELD name RETURN name"} {
		outcome := h.Run(routeBoltAuto, q, nil, time.Minute)
		if outcome.Failed() {
			t.Logf("%s: %s %s", q, outcome.Code, outcome.Message)
		}
		for _, row := range outcome.Rows {
			if name, ok := row[0].(string); ok {
				registered[strings.ToLower(name)] = true
			}
		}
	}
	placeholder := regexp.MustCompile(`\{[^}]*\}|<[^>]*>|:[A-Za-z_]+`)
	checked, manual, leads := map[string]int{}, map[string]int{}, 0
	lead := func(item huntH9Item, verdict string, detail map[string]any) {
		leads++
		entry := map[string]any{"ground": "H9", "item": item.ID, "kind": item.Kind, "name": item.Name, "documented": item.Where, "verdict": verdict}
		for k, v := range detail {
			entry[k] = v
		}
		huntLead(t, entry)
	}
	for _, item := range items {
		switch item.Kind {
		case "option-env":
			checked[item.Kind]++
			if !literals[item.Name] {
				lead(item, "documented environment variable is not read by any code", nil)
			}
		case "option-yaml":
			checked[item.Kind]++
			parts := strings.Split(item.Name, ".")
			if !yamlTags[parts[len(parts)-1]] {
				lead(item, "documented configuration key is not a yaml key of the configuration", nil)
			}
		case "option-flag":
			checked[item.Kind]++
			name := strings.TrimPrefix(item.Name, "--")
			if !cmdLiterals[name] && !cmdLiterals["--"+name] && !cmdLiterals["-"+name] {
				lead(item, "documented command-line flag is not defined in cmd/", nil)
			}
		case "procedure":
			checked[item.Kind]++
			if !registered[strings.ToLower(item.Name)] {
				lead(item, "documented procedure or function is not registered in the default configuration", nil)
			}
		case "endpoint":
			checked[item.Kind]++
			fields := strings.Fields(item.Name)
			method, path := fields[0], placeholder.ReplaceAllString(fields[1], "nornic")
			request, err := http.NewRequest(method, h.http.URL+path, strings.NewReader("{}"))
			if err != nil {
				lead(item, "documented endpoint is not a valid request", map[string]any{"error": err.Error()})
				continue
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", h.token)
			client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
			response, err := client.Do(request)
			if err != nil {
				lead(item, "documented endpoint did not answer", map[string]any{"error": err.Error()})
				continue
			}
			_ = response.Body.Close()
			if response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusMethodNotAllowed {
				lead(item, "documented endpoint is not routed", map[string]any{"status": response.StatusCode, "requested": method + " " + path})
			}
		default:
			manual[item.Kind]++
		}
	}
	huntCoverage(t, map[string]any{"ground": "H9", "checkedAutomatically": checked, "leftForTheHunter": manual, "leads": leads,
		"registeredProcedures": len(registered)})
}
