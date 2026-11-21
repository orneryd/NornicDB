package config

// Close-out lane L8 (v1.5.2 verification campaign, 2026-11-22): hunt leads
// L-H9-015 (negative cache size accepted), L-H9-016 (documented environment
// variables read by no code) and the option-yaml and option-flag parts of
// L-H9-018, decided item by item by a run. Test-only, not for merge.
//
// Run: go test ./pkg/config -run '^TestL8' -count=1 -timeout=15m -v

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func l8cLog(t *testing.T, kind string, fields map[string]any) {
	t.Helper()
	data, _ := json.Marshal(fields)
	t.Logf("%s %s", kind, data)
}

// ---------------------------------------------------------------- L-H9-015
func TestL8_H9_015_NegativeQueryCacheSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "negative.yaml")
	if err := os.WriteFile(path, []byte("memory:\n  query_cache_size: -5\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var decoded YAMLConfig
	raw, _ := os.ReadFile(path)
	decodeErr := yaml.Unmarshal(raw, &decoded)
	cfg, err := LoadFromFile(path)
	loaded, validateErr := -1, error(nil)
	if err == nil {
		loaded = cfg.Memory.QueryCacheSize
		validateErr = cfg.Validate()
	}
	t.Setenv("NORNICDB_QUERY_CACHE_SIZE", "-5")
	envCfg := LoadFromEnv()
	l8cLog(t, "L8_OWNER", map[string]any{"lead": "L-H9-015", "yaml_decoded_query_cache_size": decoded.Memory.QueryCacheSize, "decode_err": fmt.Sprint(decodeErr),
		"LoadFromFile_err": fmt.Sprint(err), "loaded_query_cache_size": loaded, "Validate_err": fmt.Sprint(validateErr), "env_-5_loaded": envCfg.Memory.QueryCacheSize, "env_-5_Validate": fmt.Sprint(envCfg.Validate())})
	if err == nil && validateErr == nil {
		t.Errorf("memory.query_cache_size: -5 accepted (loaded %d, no error); configuration.md:1188 says negative cache sizes are rejected", loaded)
	}
}

func TestL8_H9_015_Control(t *testing.T) {
	path := filepath.Join(t.TempDir(), "positive.yaml")
	if err := os.WriteFile(path, []byte("memory:\n  query_cache_size: 5\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFromFile(path)
	if err != nil || cfg.Memory.QueryCacheSize != 5 {
		t.Errorf("query_cache_size: 5 -> %v %v", cfg, err)
	}
}

// ---------------------------------------------------------------- shared
type l8cRaw struct{ Item, Kind, Name, Documented string }

func l8cRows(t *testing.T, kind string) []l8cRaw {
	t.Helper()
	file, err := os.Open(filepath.Join("..", "server", "testdata", "l8", "h9-leads-raw-r1.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	var rows []l8cRaw
	for first := true; scanner.Scan(); first = false {
		if first {
			continue
		}
		cols := strings.Split(scanner.Text(), "\t")
		if len(cols) >= 5 && cols[2] == kind {
			rows = append(rows, l8cRaw{cols[1], cols[2], cols[3], cols[4]})
		}
	}
	return rows
}

func l8cWhere(documented string) (string, int) {
	where := documented
	if i := strings.LastIndex(where, "="); i >= 0 {
		where = where[i+1:]
	}
	parts := strings.Split(where, ":")
	if len(parts) < 2 {
		return where, 0
	}
	var n int
	fmt.Sscanf(parts[1], "%d", &n)
	return parts[0], n
}

func l8cDocLines(path string) []string {
	data, err := os.ReadFile(filepath.Join("..", "..", path))
	if err != nil {
		return nil
	}
	return strings.Split(string(data), "\n")
}

// l8cEqual compares two values ignoring func, chan and unsafe pointer fields.
func l8cEqual(a, b reflect.Value, depth int) bool {
	if depth > 40 {
		return true
	}
	if a.Kind() != b.Kind() {
		return false
	}
	switch a.Kind() {
	case reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return true
	case reflect.Pointer, reflect.Interface:
		if a.IsNil() || b.IsNil() {
			return a.IsNil() == b.IsNil()
		}
		return l8cEqual(a.Elem(), b.Elem(), depth+1)
	case reflect.Struct:
		for i := 0; i < a.NumField(); i++ {
			if !l8cEqual(a.Field(i), b.Field(i), depth+1) {
				return false
			}
		}
		return true
	case reflect.Slice, reflect.Array:
		if a.Kind() == reflect.Slice && (a.IsNil() != b.IsNil()) {
			return false
		}
		if a.Len() != b.Len() {
			return false
		}
		for i := 0; i < a.Len(); i++ {
			if !l8cEqual(a.Index(i), b.Index(i), depth+1) {
				return false
			}
		}
		return true
	case reflect.Map:
		if a.Len() != b.Len() {
			return false
		}
		for _, key := range a.MapKeys() {
			bv := b.MapIndex(key)
			if !bv.IsValid() || !l8cEqual(a.MapIndex(key), bv, depth+1) {
				return false
			}
		}
		return true
	default:
		if a.CanInterface() && b.CanInterface() {
			return reflect.DeepEqual(a.Interface(), b.Interface())
		}
		return fmt.Sprint(a) == fmt.Sprint(b)
	}
}

func l8cConfigEqual(a, b *Config) bool { return l8cEqual(reflect.ValueOf(a), reflect.ValueOf(b), 0) }

// l8cScan lists the repository files (outside docs/ and Markdown) that hold
// text, split into test and non-test files.
func l8cScan(needles []string) map[string][2][]string {
	hits := map[string][2][]string{}
	root := filepath.Join("..", "..")
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "docs", "vendor", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(rel, ".md") || strings.HasPrefix(d.Name(), "zz_closeout_l8") || strings.HasPrefix(d.Name(), "zz_hunt_") {
			return nil
		}
		info, err := d.Info()
		if err != nil || info.Size() > 4<<20 {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		text := string(data)
		for _, needle := range needles {
			if strings.Contains(text, needle) {
				entry := hits[needle]
				if strings.HasSuffix(rel, "_test.go") {
					entry[1] = append(entry[1], rel)
				} else {
					entry[0] = append(entry[0], rel)
				}
				hits[needle] = entry
			}
		}
		return nil
	})
	return hits
}

func l8cFirst(list []string, n int) []string {
	sort.Strings(list)
	if len(list) > n {
		return append(list[:n:n], fmt.Sprintf("... %d more", len(list)-n))
	}
	return list
}

// ---------------------------------------------------------------- L-H9-016
func TestL8_H9_016_DocumentedEnvironmentVariables(t *testing.T) {
	baseline := LoadFromEnv()
	if !l8cConfigEqual(baseline, LoadFromEnv()) {
		t.Fatal("control: two loads of the same environment differ; comparison unusable")
	}
	t.Setenv("NORNICDB_QUERY_CACHE_SIZE", "77")
	if l8cConfigEqual(baseline, LoadFromEnv()) {
		t.Fatal("control: NORNICDB_QUERY_CACHE_SIZE=77 changed nothing; comparison unusable")
	}
	os.Unsetenv("NORNICDB_QUERY_CACHE_SIZE")
	rows := l8cRows(t, "option-env")
	needles := []string{}
	for _, row := range rows {
		needles = append(needles, row.Name)
	}
	hits := l8cScan(needles)
	counts := map[string]int{}
	for _, row := range rows {
		changed := []string{}
		if !strings.HasSuffix(row.Name, "_") {
			for _, value := range []string{"7777", "true", "false", "l8probe"} {
				func() {
					defer func() {
						if r := recover(); r != nil {
							changed = append(changed, value+":panic")
						}
					}()
					previous, had := os.LookupEnv(row.Name)
					os.Setenv(row.Name, value)
					cfg := LoadFromEnv()
					if had {
						os.Setenv(row.Name, previous)
					} else {
						os.Unsetenv(row.Name)
					}
					if !l8cConfigEqual(baseline, cfg) {
						changed = append(changed, value)
					}
				}()
			}
		}
		code, tests := hits[row.Name][0], hits[row.Name][1]
		class := ""
		switch {
		case len(changed) > 0:
			class = "read by LoadFromEnv"
		case len(code) > 0:
			class = "read outside LoadFromEnv (non-test file names it)"
		case len(tests) > 0:
			class = "named only by test files"
		default:
			class = "named by no file outside docs"
		}
		counts[class]++
		doc, line := l8cWhere(row.Documented)
		docText := ""
		if lines := l8cDocLines(doc); line > 0 && line <= len(lines) {
			docText = strings.TrimSpace(lines[line-1])
		}
		l8cLog(t, "L8_ENV", map[string]any{"lead": "L-H9-016", "item": row.Item, "name": row.Name, "documented": row.Documented, "doc_line": docText, "class": class, "LoadFromEnv_changed_by": changed, "code_files": l8cFirst(code, 6), "test_files": l8cFirst(tests, 4)})
	}
	l8cLog(t, "L8_ENV_SUMMARY", map[string]any{"lead": "L-H9-016", "rows": len(rows), "counts": counts})
}

// ---------------------------------------------------------------- L-H9-018 option-yaml
var l8cKeyRE = regexp.MustCompile(`^(\s*)(?:-\s+)?([A-Za-z_][A-Za-z0-9_.\-]*)\s*:`)

func l8cSections() map[string]bool {
	sections := map[string]bool{}
	typ := reflect.TypeOf(YAMLConfig{})
	for i := 0; i < typ.NumField(); i++ {
		tag := strings.Split(typ.Field(i).Tag.Get("yaml"), ",")[0]
		if tag != "" && tag != "-" {
			sections[tag] = true
		}
	}
	return sections
}

// l8cBlock returns the fenced block around line (1-based): its language,
// its lines, and the line's index in it.
func l8cBlock(lines []string, line int) (string, []string, int) {
	start, end := -1, -1
	for i := line - 1; i >= 0; i-- {
		if strings.HasPrefix(strings.TrimSpace(lines[i]), "```") {
			start = i
			break
		}
	}
	if start < 0 {
		return "", nil, -1
	}
	// The opening fence must be an opening one: count fences above it.
	fences := 0
	for i := 0; i < start; i++ {
		if strings.HasPrefix(strings.TrimSpace(lines[i]), "```") {
			fences++
		}
	}
	if fences%2 == 1 {
		return "", nil, -1
	}
	for i := line; i < len(lines); i++ {
		if strings.HasPrefix(strings.TrimSpace(lines[i]), "```") {
			end = i
			break
		}
	}
	if end < 0 {
		end = len(lines)
	}
	lang := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(lines[start]), "```"))
	return lang, lines[start+1 : end], line - 1 - (start + 1)
}

func l8cPath(block []string, at int) []string {
	if at < 0 || at >= len(block) {
		return nil
	}
	m := l8cKeyRE.FindStringSubmatch(block[at])
	if m == nil {
		return nil
	}
	path := []string{m[2]}
	indent := len(m[1])
	for i := at - 1; i >= 0 && indent > 0; i-- {
		pm := l8cKeyRE.FindStringSubmatch(block[i])
		if pm == nil {
			continue
		}
		if len(pm[1]) < indent {
			rest := strings.TrimSpace(block[i][len(pm[0]):])
			if rest == "" || strings.HasPrefix(rest, "#") {
				path = append([]string{pm[2]}, path...)
				indent = len(pm[1])
			}
		}
	}
	return path
}

func l8cStrict(path []string) error {
	var b strings.Builder
	for depth, part := range path {
		b.WriteString(strings.Repeat("  ", depth))
		b.WriteString(part)
		if depth == len(path)-1 {
			b.WriteString(": null\n")
		} else {
			b.WriteString(":\n")
		}
	}
	decoder := yaml.NewDecoder(strings.NewReader(b.String()))
	decoder.KnownFields(true)
	var into YAMLConfig
	err := decoder.Decode(&into)
	if err != nil && err.Error() == "EOF" {
		return nil
	}
	return err
}

func TestL8_H9_018_YAMLOptions(t *testing.T) {
	sections := l8cSections()
	counts := map[string]int{}
	for _, row := range l8cRows(t, "option-yaml") {
		doc, line := l8cWhere(row.Documented)
		lines := l8cDocLines(doc)
		lang, block, at := "", []string(nil), -1
		if line > 0 && line <= len(lines) {
			lang, block, at = l8cBlock(lines, line)
		}
		top := []string{}
		configTop := false
		for _, l := range block {
			if m := l8cKeyRE.FindStringSubmatch(l); m != nil && m[1] == "" {
				top = append(top, m[2])
				if sections[m[2]] {
					configTop = true
				}
			}
		}
		path := l8cPath(block, at)
		class, strictErr := "", ""
		switch {
		case block == nil:
			class = "not inside a fenced block"
		case !configTop:
			class = "block is not a NornicDB configuration file (no top-level key is a config section)"
		default:
			if err := l8cStrict(path); err != nil {
				class, strictErr = "config block: documented key path refused by the schema", err.Error()
			} else {
				class = "config block: documented key path accepted by the schema"
			}
		}
		counts[class]++
		docText := ""
		if line > 0 && line <= len(lines) {
			docText = strings.TrimSpace(lines[line-1])
		}
		if len(strictErr) > 300 {
			strictErr = strictErr[:300]
		}
		l8cLog(t, "L8_YAML", map[string]any{"lead": "L-H9-018", "item": row.Item, "name": row.Name, "documented": row.Documented, "lang": lang, "top_keys": l8cFirst(top, 8), "path": strings.Join(path, "."), "doc_line": docText, "class": class, "strict_error": strictErr})
	}
	l8cLog(t, "L8_YAML_SUMMARY", map[string]any{"lead": "L-H9-018", "counts": counts})
}

// ---------------------------------------------------------------- L-H9-018 option-flag
var l8cNornicCommandRE = regexp.MustCompile(`(^|[\s/"'\x60])nornicdb(\.exe)?(\s|$)`)

func TestL8_H9_018_Flags(t *testing.T) {
	rows := l8cRows(t, "option-flag")
	needles := []string{}
	for _, row := range rows {
		needles = append(needles, `"`+strings.TrimPrefix(row.Name, "--")+`"`)
	}
	hits := l8cScan(needles)
	counts := map[string]int{}
	for _, row := range rows {
		doc, line := l8cWhere(row.Documented)
		lines := l8cDocLines(doc)
		docText := ""
		if line > 0 && line <= len(lines) {
			docText = strings.TrimSpace(lines[line-1])
		}
		command := strings.Fields(docText)
		first := ""
		if len(command) > 0 {
			first = command[0]
		}
		nornic := l8cNornicCommandRE.MatchString(docText) && !strings.HasPrefix(first, "docker") && !strings.HasPrefix(first, "gpg")
		cmdFiles := []string{}
		for _, f := range hits[`"`+strings.TrimPrefix(row.Name, "--")+`"`][0] {
			if strings.HasPrefix(f, "cmd/") {
				cmdFiles = append(cmdFiles, f)
			}
		}
		class := ""
		switch {
		case !nornic:
			class = "flag of another command on that line"
		case len(cmdFiles) > 0:
			class = "nornicdb command line; a cmd/ file defines the name"
		default:
			class = "nornicdb command line; no cmd/ file defines the name"
		}
		counts[class]++
		l8cLog(t, "L8_FLAG", map[string]any{"lead": "L-H9-018", "item": row.Item, "name": row.Name, "documented": row.Documented, "doc_line": docText, "first_word": first, "class": class, "cmd_files": l8cFirst(cmdFiles, 5)})
	}
	l8cLog(t, "L8_FLAG_SUMMARY", map[string]any{"lead": "L-H9-018", "counts": counts})
}
