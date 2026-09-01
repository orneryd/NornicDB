// Command localization_inventory inventories human-readable text emitted by
// maintained NornicDB core production packages.
package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

var productionRoots = []string{"cmd/nornicdb", "cmd/nornicdb-admin", "pkg", "resolvers", "ui"}

var nativeStringPattern = regexp.MustCompile(`"(?:\\.|[^"\\])*"`)
var printfPlaceholderPattern = regexp.MustCompile(`%(?:\[[0-9]+\])?[#0+\- ']*(?:[0-9]+|\*)?(?:\.(?:[0-9]+|\*))?(?:\[[0-9]+\])?[vTtbcdoOxXUeEfFgGspwq]`)
var templatePlaceholderPattern = regexp.MustCompile(`{{\s*\.([[:alnum:]_]+)[^}]*}}`)

type occurrence struct {
	Audience string
	Channel  string
	Package  string
	File     string
	Line     int
	Callee   string
	Text     string
	Dynamic  bool
	Review   string
}

type scanner struct {
	root             string
	fset             *token.FileSet
	occurrences      []occurrence
	seen             map[string]struct{}
	scannedFiles     int
	packages         map[string]struct{}
	skippedGenerated int
	nativeFiles      int
	cgoPreambles     int
}

func main() {
	root := flag.String("root", ".", "repository root")
	out := flag.String("out", "", "CSV output path (stdout when empty)")
	check := flag.Bool("check", false, "fail when generated inventory differs from -out")
	duplicatesOut := flag.String("duplicates-out", "", "optional exact duplicate candidate CSV")
	normalizedOut := flag.String("normalized-out", "", "optional normalized duplicate candidate CSV")
	nearOut := flag.String("near-out", "", "optional token-similarity candidate CSV")
	flag.Parse()
	if *check && *out == "" {
		fatalf("-check requires -out")
	}

	absRoot, err := filepath.Abs(*root)
	if err != nil {
		fatalf("resolve repository root: %v", err)
	}
	s := &scanner{
		root: absRoot, fset: token.NewFileSet(), seen: make(map[string]struct{}),
		packages: make(map[string]struct{}),
	}
	if err := s.scan(); err != nil {
		fatalf("scan production packages: %v", err)
	}
	s.sort()

	var inventory bytes.Buffer
	if err := s.writeCSV(&inventory); err != nil {
		fatalf("render inventory: %v", err)
	}
	if *check {
		current, err := os.ReadFile(*out)
		if err != nil {
			fatalf("read inventory for check: %v", err)
		}
		if !bytes.Equal(current, inventory.Bytes()) {
			fatalf("inventory drift: run go run ./scripts/localization_inventory -out %s", *out)
		}
	} else if *out != "" {
		if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
			fatalf("create output directory: %v", err)
		}
		if err := os.WriteFile(*out, inventory.Bytes(), 0o644); err != nil {
			fatalf("write inventory: %v", err)
		}
	} else if _, err := os.Stdout.Write(inventory.Bytes()); err != nil {
		fatalf("write inventory: %v", err)
	}
	if *duplicatesOut != "" {
		checkOrWriteReport(*duplicatesOut, *check, s.writeDuplicatesCSV)
	}
	if *normalizedOut != "" {
		checkOrWriteReport(*normalizedOut, *check, s.writeNormalizedCandidatesCSV)
	}
	if *nearOut != "" {
		checkOrWriteReport(*nearOut, *check, s.writeNearCandidatesCSV)
	}
	if *out != "" {
		s.writeSummary(os.Stderr)
	}
}

func checkOrWriteReport(path string, check bool, write func(io.Writer) error) {
	var output bytes.Buffer
	if err := write(&output); err != nil {
		fatalf("render report: %v", err)
	}
	if check {
		current, err := os.ReadFile(path)
		if err != nil {
			fatalf("read report for check: %v", err)
		}
		if !bytes.Equal(current, output.Bytes()) {
			fatalf("report drift: regenerate %s", path)
		}
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		fatalf("create report output directory: %v", err)
	}
	if err := os.WriteFile(path, output.Bytes(), 0o644); err != nil {
		fatalf("write report: %v", err)
	}
}

func (s *scanner) scan() error {
	if s.seen == nil {
		s.seen = make(map[string]struct{})
	}
	if s.packages == nil {
		s.packages = make(map[string]struct{})
	}
	for _, root := range productionRoots {
		path := filepath.Join(s.root, root)
		if _, err := os.Stat(path); errorsIsNotExist(err) {
			continue
		}
		if err := filepath.WalkDir(path, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				if shouldSkipDir(path, entry.Name()) {
					return filepath.SkipDir
				}
				return nil
			}
			if isNativeSource(path) {
				return s.scanNativeFile(path)
			}
			if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			return s.scanFile(path)
		}); err != nil {
			return err
		}
	}
	return nil
}

func errorsIsNotExist(err error) bool {
	return err != nil && os.IsNotExist(err)
}

func shouldSkipDir(path, name string) bool {
	if name == "vendor" || name == "testdata" || name == "node_modules" || name == "gen" {
		return true
	}
	return filepath.Base(filepath.Dir(path)) == "pkg" && name == "cypher" && strings.Contains(filepath.ToSlash(path), "/generated/")
}

func isNativeSource(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".c", ".cc", ".cpp", ".cxx", ".h", ".hpp", ".m", ".mm":
		return true
	default:
		return false
	}
}

func (s *scanner) scanFile(path string) error {
	source, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if bytes.Contains(source[:min(len(source), 2048)], []byte("Code generated")) {
		s.skippedGenerated++
		return nil
	}
	file, err := parser.ParseFile(s.fset, path, source, parser.SkipObjectResolution)
	if err != nil {
		return err
	}
	imports := importAliases(file)
	consts := stringConstants(file)
	rel, err := filepath.Rel(s.root, path)
	if err != nil {
		return err
	}
	rel = filepath.ToSlash(rel)
	pkgPath := filepath.ToSlash(filepath.Dir(rel))
	s.scannedFiles++
	s.packages[pkgPath] = struct{}{}
	s.scanCGOPreamble(pkgPath, rel, source)

	errorReturns := make(map[token.Pos]struct{})
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "Error" || function.Body == nil {
			continue
		}
		ast.Inspect(function.Body, func(child ast.Node) bool {
			if ret, ok := child.(*ast.ReturnStmt); ok {
				for _, result := range ret.Results {
					errorReturns[result.Pos()] = struct{}{}
					s.addExpr(pkgPath, rel, "client", "error-method", "Error return", result, consts)
				}
			}
			return true
		})
	}

	ast.Inspect(file, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.CallExpr:
			s.scanCall(pkgPath, rel, typed, imports, consts)
		case *ast.CompositeLit:
			s.scanComposite(pkgPath, rel, typed, consts)
		case *ast.AssignStmt:
			s.scanAssignment(pkgPath, rel, typed, consts)
		case *ast.ReturnStmt:
			for _, result := range typed.Results {
				if _, isErrorReturn := errorReturns[result.Pos()]; isErrorReturn {
					continue
				}
				if isHumanTextExpr(result, consts) {
					s.addExpr(pkgPath, rel, "client", "return-value", "return", result, consts)
				}
			}
		case *ast.BasicLit:
			s.scanEmbeddedJSON(pkgPath, rel, typed)
		}
		return true
	})
	return nil
}

func (s *scanner) scanNativeFile(path string) error {
	source, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(s.root, path)
	if err != nil {
		return err
	}
	rel = filepath.ToSlash(rel)
	pkgPath := filepath.ToSlash(filepath.Dir(rel))
	s.nativeFiles++
	s.scanNativeText(pkgPath, rel, source, 1)
	return nil
}

func (s *scanner) scanCGOPreamble(pkg, file string, source []byte) {
	importIndex := bytes.Index(source, []byte(`import "C"`))
	if importIndex < 0 {
		return
	}
	start := bytes.LastIndex(source[:importIndex], []byte("/*"))
	end := bytes.LastIndex(source[:importIndex], []byte("*/"))
	if start < 0 || end <= start {
		return
	}
	s.cgoPreambles++
	startLine := bytes.Count(source[:start], []byte("\n")) + 1
	s.scanNativeText(pkg, file, source[start+2:end], startLine)
}

func (s *scanner) scanNativeText(pkg, file string, source []byte, startLine int) {
	for index, rawLine := range bytes.Split(source, []byte("\n")) {
		line := string(rawLine)
		channel := ""
		switch {
		case strings.Contains(line, "set_error(") || strings.Contains(line, "snprintf(") || strings.Contains(line, "throw "):
			channel = "native-error"
		case strings.Contains(line, "fprintf(") || strings.Contains(line, "printf(") || strings.Contains(line, "NSLog("):
			channel = "native-log"
		case strings.Contains(line, "return "):
			channel = "native-return"
		default:
			continue
		}
		for _, quoted := range nativeStringPattern.FindAllString(line, -1) {
			text, err := strconv.Unquote(quoted)
			if err != nil || !looksHuman(text) {
				continue
			}
			audience := "client"
			review := "localize"
			if channel == "native-log" {
				audience = "operator"
				review = "policy"
			}
			s.addAtLine(pkg, file, audience, channel, "native", startLine+index, text, strings.Contains(text, "%"), review)
		}
	}
}

func (s *scanner) scanCall(pkg, file string, call *ast.CallExpr, imports map[string]string, consts map[string]string) {
	callee := callName(call.Fun)
	base := calleeBase(callee)
	qualified := importedCallee(callee, imports)

	switch {
	case qualified == "fmt.Errorf":
		s.addFormatArg(pkg, file, "client", "error", qualified, call.Args, 0, consts)
	case qualified == "errors.New":
		s.addArg(pkg, file, "client", "error", qualified, call.Args, 0, consts)
	case qualified == "google.golang.org/grpc/status.Error":
		s.addArg(pkg, file, "client", "grpc", qualified, call.Args, 1, consts)
	case qualified == "google.golang.org/grpc/status.Errorf":
		s.addFormatArg(pkg, file, "client", "grpc", qualified, call.Args, 1, consts)
	case qualified == "google.golang.org/grpc/status.New":
		s.addArg(pkg, file, "client", "grpc", qualified, call.Args, 1, consts)
	case qualified == "google.golang.org/grpc/status.Newf":
		s.addFormatArg(pkg, file, "client", "grpc", qualified, call.Args, 1, consts)
	case qualified == "net/http.Error":
		s.addArg(pkg, file, "client", "http", qualified, call.Args, 1, consts)
	case qualified == "fmt.Printf":
		s.addFormatArg(pkg, file, "cli", "cli", qualified, call.Args, 0, consts)
	case qualified == "fmt.Print", qualified == "fmt.Println":
		s.addArg(pkg, file, "cli", "cli", qualified, call.Args, 0, consts)
	case qualified == "fmt.Fprintf":
		s.addFormatArg(pkg, file, "cli", "cli", qualified, call.Args, 1, consts)
	case qualified == "fmt.Fprint", qualified == "fmt.Fprintln":
		s.addArg(pkg, file, "cli", "cli", qualified, call.Args, 1, consts)
	case qualified == "log.Printf", qualified == "log.Fatalf", qualified == "log.Panicf":
		s.addFormatArg(pkg, file, "operator", "log", qualified, call.Args, 0, consts)
	case qualified == "log.Print", qualified == "log.Println",
		qualified == "log.Fatal", qualified == "log.Fatalln",
		qualified == "log.Panic", qualified == "log.Panicln":
		s.addArg(pkg, file, "operator", "log", qualified, call.Args, 0, consts)
	case qualified == "log/slog.Debug", qualified == "log/slog.Info", qualified == "log/slog.Warn", qualified == "log/slog.Error":
		s.addArg(pkg, file, "operator", "log", qualified, call.Args, 0, consts)
	case qualified == "io.WriteString":
		s.addArg(pkg, file, "client", "wire-output", qualified, call.Args, 1, consts)
	case strings.HasPrefix(qualified, "flag.") || strings.HasPrefix(qualified, "github.com/spf13/pflag."):
		s.addArg(pkg, file, "cli", "metadata", qualified, call.Args, len(call.Args)-1, consts)
	case base == "Log" || base == "LogAttrs":
		messageIndex := 1
		if len(call.Args) >= 4 {
			messageIndex = 2
		}
		s.addArg(pkg, file, "operator", "log", callee, call.Args, messageIndex, consts)
	case base == "Debug" || base == "Debugf" || base == "Info" || base == "Infof" || base == "Warn" || base == "Warnf" || base == "Warning" || base == "Warningf" || base == "Error" || base == "Errorf" || base == "Fatal" || base == "Fatalf":
		if len(call.Args) > 0 {
			if strings.HasSuffix(base, "f") {
				s.addFormatArg(pkg, file, "operator", "log", callee, call.Args, 0, consts)
			} else {
				s.addArg(pkg, file, "operator", "log", callee, call.Args, 0, consts)
			}
		}
	case base == "panic":
		s.addArg(pkg, file, "operator", "panic", callee, call.Args, 0, consts)
	case strings.Contains(strings.ToLower(base), "failure"):
		s.addArg(pkg, file, "client", "bolt", callee, call.Args, len(call.Args)-1, consts)
	case base == "writeError":
		s.addArg(pkg, file, "client", "http", callee, call.Args, len(call.Args)-2, consts)
	case base == "writeNeo4jError":
		s.addArg(pkg, file, "client", "http", callee, call.Args, len(call.Args)-1, consts)
	case base == "writeJSONRPCError":
		s.addArg(pkg, file, "client", "jsonrpc", callee, call.Args, len(call.Args)-2, consts)
	case strings.Contains(strings.ToLower(base), "register"):
		for _, arg := range call.Args {
			if isHumanTextExpr(arg, consts) {
				s.addExpr(pkg, file, "client", "metadata", callee, arg, consts)
			}
		}
	}
}

func (s *scanner) scanComposite(pkg, file string, literal *ast.CompositeLit, consts map[string]string) {
	for _, element := range literal.Elts {
		pair, ok := element.(*ast.KeyValueExpr)
		if !ok || !isMessageField(fieldName(pair.Key)) {
			continue
		}
		s.addExpr(pkg, file, "client", "response-field", fieldName(pair.Key), pair.Value, consts)
	}
}

func (s *scanner) scanAssignment(pkg, file string, assignment *ast.AssignStmt, consts map[string]string) {
	for index, left := range assignment.Lhs {
		if index >= len(assignment.Rhs) || !isMessageField(fieldName(left)) {
			continue
		}
		s.addExpr(pkg, file, "client", "response-field", fieldName(left), assignment.Rhs[index], consts)
	}
}

func (s *scanner) scanEmbeddedJSON(pkg, file string, literal *ast.BasicLit) {
	if literal.Kind != token.STRING {
		return
	}
	value, err := strconv.Unquote(literal.Value)
	if err != nil || (!strings.HasPrefix(strings.TrimSpace(value), "{") && !strings.HasPrefix(strings.TrimSpace(value), "[")) {
		return
	}
	var decoded any
	if json.Unmarshal([]byte(value), &decoded) != nil {
		return
	}
	var walk func(any, string)
	walk = func(current any, key string) {
		switch typed := current.(type) {
		case map[string]any:
			for childKey, child := range typed {
				walk(child, childKey)
			}
		case []any:
			for _, child := range typed {
				walk(child, key)
			}
		case string:
			if isMessageField(key) && looksHuman(typed) {
				s.add(pkg, file, "client", "embedded-json", key, literal.Pos(), typed, false, true)
			}
		}
	}
	walk(decoded, "")
}

func (s *scanner) addArg(pkg, file, audience, channel, callee string, args []ast.Expr, index int, consts map[string]string) {
	if index < 0 || index >= len(args) {
		return
	}
	s.addExpr(pkg, file, audience, channel, callee, args[index], consts)
}

func (s *scanner) addFormatArg(pkg, file, audience, channel, callee string, args []ast.Expr, index int, consts map[string]string) {
	if index < 0 || index >= len(args) {
		return
	}
	text, dynamic, resolved := expressionText(args[index], consts)
	dynamic = dynamic || len(args) > index+1
	if text == "" || (!dynamic && !looksHuman(text)) {
		return
	}
	s.add(pkg, file, audience, channel, callee, args[index].Pos(), text, dynamic, resolved)
}

func (s *scanner) addExpr(pkg, file, audience, channel, callee string, expr ast.Expr, consts map[string]string) {
	text, dynamic, resolved := expressionText(expr, consts)
	if text == "" || (!dynamic && !looksHuman(text)) {
		return
	}
	s.add(pkg, file, audience, channel, callee, expr.Pos(), text, dynamic, resolved)
}

func (s *scanner) add(pkg, file, audience, channel, callee string, pos token.Pos, text string, dynamic, resolved bool) {
	position := s.fset.Position(pos)
	review := "localize"
	if audience == "operator" {
		review = "policy"
	} else if !resolved {
		review = "trace-source"
	}
	s.addAtLine(pkg, file, audience, channel, callee, position.Line, text, dynamic, review)
}

func (s *scanner) addAtLine(pkg, file, audience, channel, callee string, line int, text string, dynamic bool, review string) {
	key := fmt.Sprintf("%s:%d:%s:%s:%s", file, line, channel, callee, text)
	if _, exists := s.seen[key]; exists {
		return
	}
	s.seen[key] = struct{}{}
	s.occurrences = append(s.occurrences, occurrence{
		Audience: audience, Channel: channel, Package: pkg, File: file,
		Line: line, Callee: callee, Text: text, Dynamic: dynamic, Review: review,
	})
}

func (s *scanner) sort() {
	sort.Slice(s.occurrences, func(i, j int) bool {
		left, right := s.occurrences[i], s.occurrences[j]
		if left.File != right.File {
			return left.File < right.File
		}
		if left.Line != right.Line {
			return left.Line < right.Line
		}
		if left.Channel != right.Channel {
			return left.Channel < right.Channel
		}
		return left.Text < right.Text
	})
}

func (s *scanner) writeCSV(out io.Writer) error {
	writer := csv.NewWriter(out)
	defer writer.Flush()
	if err := writer.Write([]string{"id", "audience", "channel", "package", "file", "line", "callee_or_field", "text_or_expression", "dynamic", "review"}); err != nil {
		return err
	}
	for index, item := range s.occurrences {
		if err := writer.Write([]string{
			fmt.Sprintf("MSG-%05d", index+1), item.Audience, item.Channel, item.Package,
			item.File, strconv.Itoa(item.Line), item.Callee, item.Text,
			strconv.FormatBool(item.Dynamic), item.Review,
		}); err != nil {
			return err
		}
	}
	return writer.Error()
}

func (s *scanner) writeDuplicatesCSV(out io.Writer) error {
	groups := s.reviewGroups(false)
	writer := csv.NewWriter(out)
	defer writer.Flush()
	if err := writer.Write([]string{"audience", "channel", "placeholder_schema", "occurrences", "packages", "text_or_template", "locations"}); err != nil {
		return err
	}
	for _, group := range groups {
		if err := writer.Write([]string{
			group.audience, group.channel, group.schema, strconv.Itoa(len(group.locations)),
			strings.Join(sortedKeys(group.packages), ";"), strings.Join(sortedKeys(group.texts), " | "),
			strings.Join(group.locations, ";"),
		}); err != nil {
			return err
		}
	}
	return writer.Error()
}

func (s *scanner) writeNormalizedCandidatesCSV(out io.Writer) error {
	groups := s.reviewGroups(true)
	writer := csv.NewWriter(out)
	defer writer.Flush()
	if err := writer.Write([]string{"audience", "channel", "placeholder_schema", "occurrences", "packages", "normalized_text", "variants", "locations"}); err != nil {
		return err
	}
	for _, group := range groups {
		if err := writer.Write([]string{
			group.audience, group.channel, group.schema, strconv.Itoa(len(group.locations)),
			strings.Join(sortedKeys(group.packages), ";"), group.normalized,
			strings.Join(sortedKeys(group.texts), " | "), strings.Join(group.locations, ";"),
		}); err != nil {
			return err
		}
	}
	return writer.Error()
}

type nearGroup struct {
	audience   string
	channel    string
	schema     string
	normalized string
	texts      map[string]struct{}
	packages   map[string]struct{}
	locations  []string
	tokens     map[string]struct{}
}

type nearCandidate struct {
	left  *nearGroup
	right *nearGroup
	score float64
}

func (s *scanner) writeNearCandidatesCSV(out io.Writer) error {
	buckets := make(map[string]map[string]*nearGroup)
	for _, item := range s.occurrences {
		if item.Review != "localize" || strings.TrimSpace(item.Text) == "" {
			continue
		}
		normalized := normalizeCandidate(item.Text)
		schema := placeholderSchema(item.Text)
		bucketKey := item.Audience + "\x00" + item.Channel + "\x00" + schema
		if buckets[bucketKey] == nil {
			buckets[bucketKey] = make(map[string]*nearGroup)
		}
		group := buckets[bucketKey][normalized]
		if group == nil {
			group = &nearGroup{
				audience: item.Audience, channel: item.Channel, schema: schema, normalized: normalized,
				texts: make(map[string]struct{}), packages: make(map[string]struct{}), tokens: tokenSet(normalized),
			}
			buckets[bucketKey][normalized] = group
		}
		group.texts[item.Text] = struct{}{}
		group.packages[item.Package] = struct{}{}
		group.locations = append(group.locations, fmt.Sprintf("%s:%d", item.File, item.Line))
	}

	const minimumSimilarity = 0.75
	candidates := make([]nearCandidate, 0)
	for _, groupsByText := range buckets {
		groups := make([]*nearGroup, 0, len(groupsByText))
		for _, group := range groupsByText {
			if len(group.tokens) >= 3 {
				sort.Strings(group.locations)
				groups = append(groups, group)
			}
		}
		sort.Slice(groups, func(i, j int) bool { return groups[i].normalized < groups[j].normalized })
		for leftIndex := 0; leftIndex < len(groups); leftIndex++ {
			for rightIndex := leftIndex + 1; rightIndex < len(groups); rightIndex++ {
				score := jaccardSimilarity(groups[leftIndex].tokens, groups[rightIndex].tokens)
				if score >= minimumSimilarity && score < 1 {
					candidates = append(candidates, nearCandidate{left: groups[leftIndex], right: groups[rightIndex], score: score})
				}
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		if candidates[i].left.audience != candidates[j].left.audience {
			return candidates[i].left.audience < candidates[j].left.audience
		}
		if candidates[i].left.channel != candidates[j].left.channel {
			return candidates[i].left.channel < candidates[j].left.channel
		}
		if candidates[i].left.schema != candidates[j].left.schema {
			return candidates[i].left.schema < candidates[j].left.schema
		}
		if candidates[i].left.normalized != candidates[j].left.normalized {
			return candidates[i].left.normalized < candidates[j].left.normalized
		}
		return candidates[i].right.normalized < candidates[j].right.normalized
	})

	writer := csv.NewWriter(out)
	defer writer.Flush()
	if err := writer.Write([]string{"audience", "channel", "placeholder_schema", "similarity", "left_text", "right_text", "packages", "locations"}); err != nil {
		return err
	}
	for _, candidate := range candidates {
		packages := make(map[string]struct{})
		for value := range candidate.left.packages {
			packages[value] = struct{}{}
		}
		for value := range candidate.right.packages {
			packages[value] = struct{}{}
		}
		locations := append(append([]string(nil), candidate.left.locations...), candidate.right.locations...)
		sort.Strings(locations)
		if err := writer.Write([]string{
			candidate.left.audience, candidate.left.channel, candidate.left.schema,
			strconv.FormatFloat(candidate.score, 'f', 3, 64),
			strings.Join(sortedKeys(candidate.left.texts), " | "),
			strings.Join(sortedKeys(candidate.right.texts), " | "),
			strings.Join(sortedKeys(packages), ";"), strings.Join(locations, ";"),
		}); err != nil {
			return err
		}
	}
	return writer.Error()
}

func tokenSet(text string) map[string]struct{} {
	tokens := make(map[string]struct{})
	for _, token := range strings.Fields(text) {
		tokens[token] = struct{}{}
	}
	return tokens
}

func jaccardSimilarity(left, right map[string]struct{}) float64 {
	intersection := 0
	for token := range left {
		if _, exists := right[token]; exists {
			intersection++
		}
	}
	union := len(left) + len(right) - intersection
	if union == 0 {
		return 0
	}
	return float64(intersection) / float64(union)
}

type reviewGroup struct {
	audience   string
	channel    string
	schema     string
	normalized string
	locations  []string
	packages   map[string]struct{}
	texts      map[string]struct{}
}

func (s *scanner) reviewGroups(normalized bool) []*reviewGroup {
	groups := make(map[string]*reviewGroup)
	for _, item := range s.occurrences {
		if item.Review != "localize" || strings.TrimSpace(item.Text) == "" {
			continue
		}
		schema := placeholderSchema(item.Text)
		groupText := item.Text
		if normalized {
			groupText = normalizeCandidate(item.Text)
		}
		key := item.Audience + "\x00" + item.Channel + "\x00" + schema + "\x00" + groupText
		group := groups[key]
		if group == nil {
			group = &reviewGroup{
				audience: item.Audience, channel: item.Channel, schema: schema, normalized: groupText,
				packages: make(map[string]struct{}), texts: make(map[string]struct{}),
			}
			groups[key] = group
		}
		group.locations = append(group.locations, fmt.Sprintf("%s:%d", item.File, item.Line))
		group.packages[item.Package] = struct{}{}
		group.texts[item.Text] = struct{}{}
	}
	duplicates := make([]*reviewGroup, 0, len(groups))
	for _, group := range groups {
		if len(group.locations) > 1 && (!normalized || len(group.texts) > 1) {
			sort.Strings(group.locations)
			duplicates = append(duplicates, group)
		}
	}
	sort.Slice(duplicates, func(i, j int) bool {
		if len(duplicates[i].locations) != len(duplicates[j].locations) {
			return len(duplicates[i].locations) > len(duplicates[j].locations)
		}
		if duplicates[i].audience != duplicates[j].audience {
			return duplicates[i].audience < duplicates[j].audience
		}
		if duplicates[i].channel != duplicates[j].channel {
			return duplicates[i].channel < duplicates[j].channel
		}
		if duplicates[i].schema != duplicates[j].schema {
			return duplicates[i].schema < duplicates[j].schema
		}
		if duplicates[i].normalized != duplicates[j].normalized {
			return duplicates[i].normalized < duplicates[j].normalized
		}
		return strings.Join(sortedKeys(duplicates[i].texts), "\x00") < strings.Join(sortedKeys(duplicates[j].texts), "\x00")
	})
	return duplicates
}

func placeholderSchema(text string) string {
	placeholders := make([]string, 0)
	for _, match := range templatePlaceholderPattern.FindAllStringSubmatch(text, -1) {
		placeholders = append(placeholders, "named:"+match[1])
	}
	for _, match := range printfPlaceholderPattern.FindAllString(text, -1) {
		verb := match[len(match)-1]
		kind := "value"
		switch {
		case strings.ContainsRune("sqxX", rune(verb)):
			kind = "string"
		case strings.ContainsRune("bcdOoU", rune(verb)):
			kind = "integer"
		case strings.ContainsRune("eEfFgG", rune(verb)):
			kind = "float"
		case verb == 't':
			kind = "boolean"
		case verb == 'p':
			kind = "pointer"
		case verb == 'w':
			kind = "error"
		}
		placeholders = append(placeholders, kind)
	}
	if len(placeholders) == 0 {
		return "-"
	}
	return strings.Join(placeholders, ",")
}

func normalizeCandidate(text string) string {
	text = templatePlaceholderPattern.ReplaceAllString(text, " placeholder ")
	text = printfPlaceholderPattern.ReplaceAllString(text, " placeholder ")
	text = strings.Map(func(value rune) rune {
		if unicode.IsLetter(value) || unicode.IsNumber(value) || unicode.IsSpace(value) {
			return unicode.ToLower(value)
		}
		return ' '
	}, text)
	return strings.Join(strings.Fields(text), " ")
}

func sortedKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for value := range values {
		keys = append(keys, value)
	}
	sort.Strings(keys)
	return keys
}

func (s *scanner) writeSummary(out *os.File) {
	counts := make(map[string]int)
	audiences := make(map[string]int)
	reviews := make(map[string]int)
	packageCounts := make(map[string]int)
	unique := make(map[string]struct{})
	files := make(map[string]struct{})
	packages := make(map[string]struct{})
	dynamic := 0
	for _, item := range s.occurrences {
		counts[item.Channel]++
		audiences[item.Audience]++
		reviews[item.Review]++
		packageCounts[item.Package]++
		unique[item.Text] = struct{}{}
		files[item.File] = struct{}{}
		packages[item.Package] = struct{}{}
		if item.Dynamic {
			dynamic++
		}
	}
	channels := make([]string, 0, len(counts))
	for channel := range counts {
		channels = append(channels, channel)
	}
	sort.Strings(channels)
	fmt.Fprintf(out, "inventory occurrences: %d\n", len(s.occurrences))
	fmt.Fprintf(out, "unique text/templates: %d\n", len(unique))
	fmt.Fprintf(out, "dynamic occurrences: %d\n", dynamic)
	fmt.Fprintf(out, "static occurrences: %d\n", len(s.occurrences)-dynamic)
	fmt.Fprintf(out, "files with occurrences: %d\n", len(files))
	fmt.Fprintf(out, "packages with occurrences: %d\n", len(packages))
	fmt.Fprintf(out, "production files scanned: %d\n", s.scannedFiles)
	fmt.Fprintf(out, "production packages scanned: %d\n", len(s.packages))
	fmt.Fprintf(out, "generated files skipped: %d\n", s.skippedGenerated)
	fmt.Fprintf(out, "native files scanned: %d\n", s.nativeFiles)
	fmt.Fprintf(out, "cgo preambles scanned: %d\n", s.cgoPreambles)
	for _, channel := range channels {
		fmt.Fprintf(out, "  %s: %d\n", channel, counts[channel])
	}
	writeSortedCounts(out, "audience", audiences, 0)
	writeSortedCounts(out, "review", reviews, 0)
	writeSortedCounts(out, "top packages", packageCounts, 15)
}

func writeSortedCounts(out *os.File, heading string, counts map[string]int, limit int) {
	type count struct {
		name  string
		value int
	}
	items := make([]count, 0, len(counts))
	for name, value := range counts {
		items = append(items, count{name: name, value: value})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].value != items[j].value {
			return items[i].value > items[j].value
		}
		return items[i].name < items[j].name
	})
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	fmt.Fprintf(out, "%s:\n", heading)
	for _, item := range items {
		fmt.Fprintf(out, "  %s: %d\n", item.name, item.value)
	}
}

func importAliases(file *ast.File) map[string]string {
	aliases := make(map[string]string)
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		name := filepath.Base(path)
		if spec.Name != nil && spec.Name.Name != "_" && spec.Name.Name != "." {
			name = spec.Name.Name
		}
		aliases[name] = path
	}
	return aliases
}

func stringConstants(file *ast.File) map[string]string {
	constants := make(map[string]string)
	for _, declaration := range file.Decls {
		generic, ok := declaration.(*ast.GenDecl)
		if !ok || generic.Tok != token.CONST {
			continue
		}
		for _, spec := range generic.Specs {
			values, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for index, name := range values.Names {
				if index >= len(values.Values) {
					continue
				}
				if value, ok := staticString(values.Values[index], constants); ok {
					constants[name.Name] = value
				}
			}
		}
	}
	return constants
}

func expressionText(expr ast.Expr, consts map[string]string) (string, bool, bool) {
	if value, ok := staticString(expr, consts); ok {
		return value, false, true
	}
	if call, ok := expr.(*ast.CallExpr); ok {
		base := calleeBase(callName(call.Fun))
		if (base == "Sprintf" || base == "Errorf") && len(call.Args) > 0 {
			if value, ok := staticString(call.Args[0], consts); ok {
				return value, true, true
			}
		}
	}
	var buffer bytes.Buffer
	if format.Node(&buffer, token.NewFileSet(), expr) == nil {
		return buffer.String(), true, false
	}
	return "<dynamic expression>", true, false
}

func staticString(expr ast.Expr, consts map[string]string) (string, bool) {
	switch typed := expr.(type) {
	case *ast.BasicLit:
		if typed.Kind != token.STRING {
			return "", false
		}
		value, err := strconv.Unquote(typed.Value)
		return value, err == nil
	case *ast.Ident:
		value, ok := consts[typed.Name]
		return value, ok
	case *ast.BinaryExpr:
		if typed.Op != token.ADD {
			return "", false
		}
		left, leftOK := staticString(typed.X, consts)
		right, rightOK := staticString(typed.Y, consts)
		return left + right, leftOK && rightOK
	case *ast.ParenExpr:
		return staticString(typed.X, consts)
	}
	return "", false
}

func isHumanTextExpr(expr ast.Expr, consts map[string]string) bool {
	if text, ok := staticString(expr, consts); ok {
		return looksHuman(text)
	}
	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) == 0 {
		return false
	}
	base := calleeBase(callName(call.Fun))
	if base != "Sprintf" && base != "Errorf" {
		return false
	}
	text, ok := staticString(call.Args[0], consts)
	return ok && looksHuman(text)
}

func looksHuman(text string) bool {
	text = strings.TrimSpace(text)
	if len(text) < 3 {
		return false
	}
	for _, r := range text {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			return true
		}
	}
	return false
}

func isMessageField(name string) bool {
	lower := strings.ToLower(name)
	switch lower {
	case "message", "error", "description", "reason", "detail", "details", "hint", "warning", "warnings", "summary", "title", "help":
		return true
	}
	for _, suffix := range []string{"message", "description", "reason", "detail", "details", "hint", "warning", "warnings", "summary", "title", "help"} {
		if strings.HasSuffix(lower, suffix) {
			return true
		}
	}
	return lower == "use" || lower == "short" || lower == "long" || lower == "example" || lower == "usage"
}

func fieldName(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.SelectorExpr:
		return typed.Sel.Name
	case *ast.BasicLit:
		if typed.Kind == token.STRING {
			value, _ := strconv.Unquote(typed.Value)
			return value
		}
	case *ast.IndexExpr:
		return fieldName(typed.Index)
	}
	return ""
}

func callName(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.SelectorExpr:
		prefix := callName(typed.X)
		if prefix == "" {
			return typed.Sel.Name
		}
		return prefix + "." + typed.Sel.Name
	case *ast.CallExpr:
		return callName(typed.Fun)
	case *ast.ParenExpr:
		return callName(typed.X)
	}
	return ""
}

func calleeBase(name string) string {
	if index := strings.LastIndex(name, "."); index >= 0 {
		return name[index+1:]
	}
	return name
}

func importedCallee(name string, imports map[string]string) string {
	parts := strings.Split(name, ".")
	if len(parts) != 2 {
		return name
	}
	if path, ok := imports[parts[0]]; ok {
		return path + "." + parts[1]
	}
	return name
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "localization inventory: "+format+"\n", args...)
	os.Exit(1)
}
