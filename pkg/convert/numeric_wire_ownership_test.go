package convert

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unicode"
)

type jsonDataPath struct {
	ReadDomain  string         `json:"read_domain,omitempty"`
	WriteDomain string         `json:"write_domain,omitempty"`
	Contract    string         `json:"contract"`
	Calls       map[string]int `json:"calls"`
}
type wireException struct {
	Reason string `json:"reason"`
	SHA256 string `json:"sha256"`
}

func numericWireRoot() string {
	_, here, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(here), "../.."))
}

func TestNumericWireOwnership(t *testing.T) {
	root := numericWireRoot()
	paths := map[string]jsonDataPath{}
	exceptions := map[string]wireException{}
	for name, target := range map[string]interface{}{"json_data_paths.json": &paths, "numeric_wire_exceptions.json": &exceptions} {
		raw, err := os.ReadFile(filepath.Join(root, "pkg/convert", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, target); err != nil {
			t.Fatal(err)
		}
	}
	for _, violation := range inspectNumericWire(root, paths, exceptions, true) {
		t.Error(violation)
	}
}

// inspectNumericWire scans every production Go package, not only the packages
// currently calling the owner. A raw reader in a new file has no implicit
// exemption. Genuine binary/schema/hash/protocol boundaries are frozen
// individually. Each path must use its declared domain entry points; adding or
// swapping a call changes that path's checked contract.
func inspectNumericWire(root string, paths map[string]jsonDataPath, exceptions map[string]wireException, complete bool) []string {
	var violations []string
	seenPaths, seenExceptions := map[string]bool{}, map[string]bool{}
	ownerFiles := map[string]bool{"pkg/convert/json.go": true, "pkg/convert/json_encode.go": true, "pkg/convert/json_domains.go": true, "pkg/convert/float_text.go": true}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".tn-operator", "node_modules", "vendor", "antlr":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		positions := token.NewFileSet()
		parsed, err := parser.ParseFile(positions, path, source, 0)
		if err != nil {
			return err
		}
		rawAliases, convertAlias := map[string]bool{}, ""
		rawAliases[".SchemaCodec"] = strings.HasPrefix(relative, "pkg/storage/")
		for _, imported := range parsed.Imports {
			importedPath, _ := strconv.Unquote(imported.Path.Value)
			alias := filepath.Base(importedPath)
			if imported.Name != nil {
				alias = imported.Name.Name
			}
			if importedPath == "encoding/json" || importedPath == "encoding/json/v2" || importedPath == "github.com/goccy/go-json" || importedPath == "github.com/json-iterator/go" {
				if alias == "." {
					violations = append(violations, relative+": dot-imported JSON bypasses ownership analysis")
				}
				rawAliases[alias] = true
			}
			if importedPath == "github.com/orneryd/nornicdb/pkg/convert" {
				convertAlias = alias
			}
			if importedPath == "github.com/orneryd/nornicdb/pkg/storage" {
				rawAliases[alias+".SchemaCodec"] = true
			}
			if importedPath == "strconv" {
				rawAliases[alias+".AppendNumber"] = true
			}
			if importedPath == "github.com/orneryd/nornicdb/pkg/cypher" {
				rawAliases[alias+".JSONNumbers"] = true
			}
		}
		if ownerFiles[relative] {
			return nil
		}
		if strings.HasPrefix(relative, "pkg/convert/") {
			privateWriter := false
			ast.Inspect(parsed, func(node ast.Node) bool {
				switch node := node.(type) {
				case *ast.Ident:
					switch node.Name {
					case "jsonWriterState", "jsonWriterStates", "preparedJSON",
						"appendJSONInteger", "appendJSONUnsigned", "appendJSONFloat",
						"jsonDocumentDecoder", "jsonDocumentDecoders",
						"newJSONDecoder", "unmarshalJSON", "newJSONEncoder", "marshalJSON", "marshalJSONIndent", "jsonNumbers", "prepareJSON",
						"jsonDecoder", "jsonNumberPolicy", "normalizeDecodedJSON", "jsonPropertyPolicy", "jsonInputPolicy":
						privateWriter = true
					}
				case *ast.CompositeLit:
					if name, ok := node.Type.(*ast.Ident); ok && name.Name == "JSONEncoder" {
						privateWriter = true
					}
				case *ast.CallExpr:
					if function, ok := node.Fun.(*ast.Ident); ok && function.Name == "new" && len(node.Args) == 1 {
						if name, ok := node.Args[0].(*ast.Ident); ok && name.Name == "JSONEncoder" {
							privateWriter = true
						}
					}
				}
				return true
			})
			if privateWriter {
				violations = append(violations, relative+": private codec accessed outside its domain owner")
			}
		}
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				raw, calls := numericJSONReferences(declaration, rawAliases, convertAlias)
				if raw || len(calls) > 0 {
					violations = append(violations, relative+": JSON entry point outside a classified function")
				}
				continue
			}
			if function.Body == nil {
				continue
			}
			key := relative + ":" + numericWireFunctionName(function)
			raw, calls := numericJSONReferences(function.Body, rawAliases, convertAlias)
			if raw {
				if numericSchemaStateReferences(function) {
					violations = append(violations, key+": schema state bypasses the schema codec")
				}
				start, end := positions.Position(function.Pos()).Offset, positions.Position(function.End()).Offset
				compact := strings.Map(func(char rune) rune {
					if unicode.IsSpace(char) {
						return -1
					}
					return char
				}, string(source[start:end]))
				digest := sha256.Sum256([]byte(compact))
				hash := hex.EncodeToString(digest[:])
				approved, ok := exceptions[key]
				seenExceptions[key] = true
				if !ok || approved.Reason == "" || approved.SHA256 != hash {
					violations = append(violations, fmt.Sprintf("NUMERIC_WIRE_EXCEPTION %s %s: raw JSON reader/writer outside the owner", key, hash))
				}
			}
			if len(calls) > 0 {
				expected, ok := paths[key]
				seenPaths[key] = true
				if !ok || expected.Contract == "" || !reflect.DeepEqual(expected.Calls, calls) {
					encoded, _ := json.Marshal(calls)
					violations = append(violations, fmt.Sprintf("NUMERIC_JSON_PATH %s %s: unclassified or wrong-domain JSON entry point", key, encoded))
				}
				for call := range calls {
					pieces := strings.SplitN(call, ".", 2)
					direction := expected.WriteDomain
					switch pieces[1] {
					case "Unmarshal", "UnmarshalLegacyState", "NewDecoder", "Numbers", "Decode":
						direction = expected.ReadDomain
					}
					if pieces[0] != direction {
						violations = append(violations, key+": "+call+" conflicts with its classified read/write domain")
					}
				}
			}
			if strings.HasPrefix(relative, "pkg/bolt/") {
				ast.Inspect(function.Body, func(node ast.Node) bool {
					if selector, ok := node.(*ast.SelectorExpr); ok && (selector.Sel.Name == "Float64bits" || selector.Sel.Name == "Float64frombits") {
						violations = append(violations, key+": Bolt number bypasses convert")
					}
					if literal, ok := node.(*ast.BasicLit); ok && literal.Kind == token.INT && strings.HasPrefix(strings.ToLower(literal.Value), "0x") {
						value, err := strconv.ParseUint(literal.Value, 0, 64)
						if err == nil && (value == 0xC1 || value >= 0xC8 && value <= 0xCB) {
							violations = append(violations, key+": numeric Bolt marker bypasses convert")
						}
					}
					return true
				})
			}
		}
		return nil
	})
	if err != nil {
		violations = append(violations, err.Error())
	}
	if complete {
		for key := range paths {
			if !seenPaths[key] {
				violations = append(violations, "missing classified JSON data path "+key)
			}
		}
		for key := range exceptions {
			if !seenExceptions[key] {
				violations = append(violations, "obsolete numeric wire exception "+key)
			}
		}
	}
	sort.Strings(violations)
	return violations
}

func numericWireFunctionName(function *ast.FuncDecl) string {
	name := function.Name.Name
	if function.Recv != nil && len(function.Recv.List) > 0 {
		receiver := function.Recv.List[0].Type
		if pointer, ok := receiver.(*ast.StarExpr); ok {
			receiver = pointer.X
		}
		if identifier, ok := receiver.(*ast.Ident); ok {
			name = identifier.Name + "." + name
		}
	}
	return name
}

// A typed schema still contains DOMAIN property values. A frozen raw-JSON
// exception must never exempt a whole schema definition or export from its owner.
func numericSchemaStateReferences(node ast.Node) bool {
	found := false
	ast.Inspect(node, func(node ast.Node) bool {
		if identifier, ok := node.(*ast.Ident); ok && (identifier.Name == "SchemaDefinition" || identifier.Name == "ExportDefinition") {
			found = true
		}
		return true
	})
	return found
}

func TestNumericSchemaRawStateExceptionRejected(t *testing.T) {
	for _, definition := range []string{
		`func bypass(data []byte) error { var def SchemaDefinition; return json.Unmarshal(data, &def) }`,
		`func bypass(def *SchemaDefinition) ([]byte,error) { return json.Marshal(def) }`,
		`func bypass(schema *SchemaManager) ([]byte,error) { return json.Marshal(schema.ExportDefinition()) }`,
	} {
		root := t.TempDir()
		path := "pkg/storage/badger_transaction_knowledge_schema.go"
		writeNumericWireFixture(t, root, path, "package storage\nimport \"encoding/json\"\n"+definition)
		compact := strings.Map(func(char rune) rune {
			if unicode.IsSpace(char) {
				return -1
			}
			return char
		}, definition)
		hash := sha256.Sum256([]byte(compact))
		exceptions := map[string]wireException{path + ":bypass": {Reason: "Typed schema", SHA256: hex.EncodeToString(hash[:])}}
		violations := inspectNumericWire(root, nil, exceptions, true)
		if !strings.Contains(strings.Join(violations, "\n"), "schema state bypasses the schema codec") {
			t.Fatalf("a typed-schema exception must not admit schema JSON: %v", violations)
		}
	}
}

func numericJSONReferences(node ast.Node, rawAliases map[string]bool, convertAlias string) (bool, map[string]int) {
	raw := false
	calls := map[string]int{}
	var parents []ast.Node
	ast.Inspect(node, func(node ast.Node) bool {
		if node == nil {
			parents = parents[:len(parents)-1]
			return true
		}
		var parent ast.Node
		if len(parents) > 0 {
			parent = parents[len(parents)-1]
		}
		parents = append(parents, node)
		if call, ok := node.(*ast.CallExpr); ok {
			name := ""
			switch function := call.Fun.(type) {
			case *ast.Ident:
				if rawAliases[".SchemaCodec"] {
					name = function.Name
				}
			case *ast.SelectorExpr:
				if owner, ok := function.X.(*ast.Ident); ok && rawAliases[owner.Name+".SchemaCodec"] {
					name = function.Sel.Name
				}
			}
			switch name {
			case "EncodeSchemaDefinition":
				calls["schema.Encode"]++
			case "DecodeSchemaDefinition":
				calls["schema.Decode"]++
			}
		}
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if owner, ok := selector.X.(*ast.Ident); ok && rawAliases[owner.Name] {
			switch selector.Sel.Name {
			case "Unmarshal", "NewDecoder", "Marshal", "MarshalIndent", "NewEncoder":
				raw = true
			}
		}
		if owner, ok := selector.X.(*ast.Ident); ok && rawAliases[owner.Name+".AppendNumber"] {
			switch selector.Sel.Name {
			case "AppendInt", "AppendUint", "AppendFloat":
				raw = true
			}
		}
		if owner, ok := selector.X.(*ast.Ident); ok && selector.Sel.Name == "JSONNumbers" && rawAliases[owner.Name+".JSONNumbers"] {
			calls["input.Numbers"]++
		}
		if owner, ok := selector.X.(*ast.Ident); ok && owner.Name == convertAlias && (selector.Sel.Name == "PropertyJSON" || selector.Sel.Name == "CypherInputJSON") {
			member, ok := parent.(*ast.SelectorExpr)
			if !ok || member.X != selector {
				calls["escaped."+selector.Sel.Name]++
			}
		}
		if domain, ok := selector.X.(*ast.SelectorExpr); ok {
			owner, ok := domain.X.(*ast.Ident)
			if ok && owner.Name == convertAlias {
				switch domain.Sel.Name {
				case "PropertyJSON":
					calls["property."+selector.Sel.Name]++
				case "CypherInputJSON":
					calls["input."+selector.Sel.Name]++
				}
			}
		}
		return true
	})
	return raw, calls
}
