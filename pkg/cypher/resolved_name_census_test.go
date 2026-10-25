package cypher

import (
	"bufio"
	"go/ast"
	"go/constant"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// nameSplitters are the library calls that split text on a separator, keyed
// by the index of their separator argument (-1: every use is recorded), and
// resolvedTokenNames, the one reader of label and relationship-type names
// given as values. A use of a splitter whose separator is not a constant, or
// holds a name delimiter (| or :), and every use of the reader, is a ledger
// record classified by the text it reads. A resolved label or type name is
// never split, so no class admits one: a new record fails until it is shown
// to read some other kind of text.
var nameSplitters = map[string]int{
	"strings.Split": 1, "strings.SplitN": 1, "strings.SplitAfter": 1, "strings.SplitAfterN": 1,
	"strings.SplitSeq": 1, "strings.SplitAfterSeq": 1, "strings.Cut": 1,
	"strings.FieldsFunc": -1, "strings.FieldsFuncSeq": -1,
	"bytes.Split": 1, "bytes.SplitN": 1, "bytes.SplitAfter": 1, "bytes.Cut": 1, "bytes.FieldsFunc": -1,
	"cypher.resolvedTokenNames": -1,
}

// The kinds of text a name-delimiter split reads.
var nameSplitClasses = map[string]bool{
	"name-reader":     true, // resolvedTokenNames: names given as values, read literally
	"element-id":      true, // element and entity IDs (4:db:id, kind:id) and storage keys
	"query-text":      true, // Cypher or DDL text, before names are resolved
	"apoc-filter":     true, // APOC's documented relationship and label filter syntax
	"import-format":   true, // Neo4j admin import headers and property lists
	"credentials":     true, // user:password pairs
	"configuration":   true, // configuration, environment and system file values
	"internal-key":    true, // keys this code builds itself
	"procedure-text":  true, // procedure signatures (name :: TYPE)
	"natural-text":    true, // free text and identifiers split into words
	"string-function": true, // string functions splitting the caller's data on its separator
}

func resolvedNameCensus(t *testing.T, overlay map[string][]string) map[string]int {
	t.Helper()
	bare := map[string]bool{}
	for name := range nameSplitters {
		bare[name[strings.LastIndexByte(name, '.')+1:]] = true
	}
	records := map[string]int{}
	for _, pkg := range typeCheckCensusPackages(t, overlay, func(name string) bool { return bare[name] }) {
		for _, file := range pkg.files {
			for _, decl := range file.Decls {
				enclosing := censusEnclosing(pkg.info, decl)
				calls := map[*ast.Ident]*ast.CallExpr{}
				ast.Inspect(decl, func(node ast.Node) bool {
					if call, ok := node.(*ast.CallExpr); ok {
						switch fun := call.Fun.(type) {
						case *ast.Ident:
							calls[fun] = call
						case *ast.SelectorExpr:
							calls[fun.Sel] = call
						}
					}
					return true
				})
				ast.Inspect(decl, func(node ast.Node) bool {
					ident, ok := node.(*ast.Ident)
					if !ok {
						return true
					}
					function, ok := pkg.info.Uses[ident].(*types.Func)
					if !ok {
						return true
					}
					name := relationshipOwnerQualified(function)
					separator, splitter := nameSplitters[name]
					if !splitter {
						return true
					}
					if call := calls[ident]; call != nil && separator >= 0 && separator < len(call.Args) {
						if value := pkg.info.Types[call.Args[separator]].Value; value != nil && value.Kind() == constant.String &&
							!strings.ContainsAny(constant.StringVal(value), "|:") {
							return true
						}
					}
					records[pkg.names[file]+"|"+enclosing+"|"+name]++
					return true
				})
			}
		}
	}
	return records
}

func resolvedNameLedger(t *testing.T) map[string]string {
	t.Helper()
	ledger, err := os.Open(filepath.Join("testdata", "resolved_name_census.txt"))
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	classified := map[string]string{}
	scanner := bufio.NewScanner(ledger)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "|")
		if len(fields) != 4 || !nameSplitClasses[fields[3]] {
			t.Fatalf("malformed ledger line %q", line)
		}
		classified[strings.Join(fields[:3], "|")] = fields[3]
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return classified
}

func resolvedNameLedgerDiff(census map[string]int, classified map[string]string) (missing, stale []string) {
	for record := range census {
		if classified[record] == "" {
			missing = append(missing, record+"|?")
		}
	}
	for record := range classified {
		if census[record] == 0 {
			stale = append(stale, record)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	return missing, stale
}

// Every module place that splits text on a name delimiter reads some kind of
// text other than a resolved name, and names given as values go through the
// one literal-name reader.
func TestResolvedNameCensusLedger(t *testing.T) {
	census := resolvedNameCensus(t, nil)
	missing, stale := resolvedNameLedgerDiff(census, resolvedNameLedger(t))
	if len(missing)+len(stale) > 0 {
		t.Fatalf("name-delimiter census differs from testdata/resolved_name_census.txt\nunclassified (%d):\n%s\nno longer present (%d):\n%s",
			len(missing), strings.Join(missing, "\n"), len(stale), strings.Join(stale, "\n"))
	}
	t.Logf("CENSUS records=%d", len(census))
}

// A planted split of a name on | or :, through a call, a function value or
// FieldsFunc, is a new unclassified record; a split on another separator is
// not recorded.
func TestResolvedNameCensusFindsPlantedSplits(t *testing.T) {
	census := resolvedNameCensus(t, map[string][]string{relationshipOwnerModule + "pkg/cypher": {`package cypher

import "strings"

func plantedPipeSplit(name string) []string { return strings.Split(name, "|") }

func plantedColonCut(name string) (string, string, bool) { return strings.Cut(name, ":") }

func plantedFieldsSplit(name string) []string {
	return strings.FieldsFunc(name, func(r rune) bool { return r == '|' })
}

func plantedVariableSeparator(name, separator string) []string { return strings.Split(name, separator) }

var plantedSplitValue = strings.SplitN

func plantedCommaSplit(name string) []string { return strings.Split(name, ",") }
`}})
	missing, _ := resolvedNameLedgerDiff(census, resolvedNameLedger(t))
	got := strings.Join(missing, "\n")
	for _, record := range []string{
		"pkg/cypher/overlay_0.go|cypher.plantedPipeSplit|strings.Split|?",
		"pkg/cypher/overlay_0.go|cypher.plantedColonCut|strings.Cut|?",
		"pkg/cypher/overlay_0.go|cypher.plantedFieldsSplit|strings.FieldsFunc|?",
		"pkg/cypher/overlay_0.go|cypher.plantedVariableSeparator|strings.Split|?",
		"pkg/cypher/overlay_0.go|<package>|strings.SplitN|?",
	} {
		if !strings.Contains(got, record) {
			t.Errorf("census did not report %s; unclassified:\n%s", record, got)
		}
	}
	if strings.Contains(got, "plantedCommaSplit") {
		t.Errorf("a split on a comma was recorded:\n%s", got)
	}
}
