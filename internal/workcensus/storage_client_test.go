package workcensus

// The first client supplies data. Production extraction has no NornicDB
// entrypoints, storage types, grammar names or class policy built into it.
const module = "github.com/orneryd/nornicdb/"

// storageConfig covers protocol, Fabric and Cypher statement entrypoints;
// storage is entered through their actual calls, including interface dispatch.
func storageConfig(dir string) Config {
	// Load every maintained package body. Managed-runtime implementations and
	// WAL constructors are dependency bodies of the statement entry packages;
	// leaving them export-only erases real preparation paths from VTA.
	c := Config{Dir: dir, Patterns: []string{"./..."}, BuildFlags: []string{"-tags=noui,nolocalllm"}, Packages: []string{module + "pkg/storage", module + "pkg/multidb", module + "pkg/replication"}}
	for p, names := range map[string][]string{
		"pkg/cypher":    {"Execute", "ExecuteWithParams", "executeInternal", "callProcedure", "executeProcedure", "evaluateFunctionWithContext", "evaluateRowExtensionFunction"},
		"pkg/cypher/fn": {"EvaluateFunction", "callFunction"},
		"pkg/fabric":    {"Execute", "ExecutePlan", "ExecuteFragment"},
		"pkg/server":    {"runRequestStatements", "handleCypher", "handleTransaction", "handleTransactionCommit", "handleAutoCommit"},
		"pkg/bolt":      {"handleRun", "handlePull", "handleCommit"},
	} {
		for _, n := range names {
			c.Roots = append(c.Roots, Root{Package: module + p, Name: n})
		}
	}
	c.Selectors = storageSelectors()
	c.IncludeSyntax = true
	c.Selectors = append(c.Selectors, Selector{Kind: "owner-call", Group: "owner-call", Node: "call"})
	c.ContextFields = []string{"walk", "ctx", "context", "labelContext", "checkpoint"}
	return c
}

func storageSelectors() []Selector {
	work := func(kind, node string) Selector { return Selector{Kind: kind, Node: node, Group: "work"} }
	call := func(kind, pkg string, names ...string) Selector {
		s := work(kind, "call")
		s.Packages = []string{pkg}
		s.Names = names
		return s
	}
	rules := []Selector{work("for", "for"), work("range", "range")}
	for _, name := range []string{"copy", "append"} {
		s := work(name, "call")
		s.Builtin = true
		s.Names = []string{name}
		rules = append(rules, s)
	}
	typed := work("entrance", "call")
	typed.PackagePrefixes = []string{module}
	typed.ResultTypes = []string{module + "pkg/storage.Node", module + "pkg/storage.NodeID", module + "pkg/storage.Edge", module + "pkg/storage.EdgeID"}
	typed.UnwrapResults = true
	rules = append(rules, typed)
	for _, entry := range []struct {
		kind  string
		names []string
	}{
		{"append", []string{"AppendValues", "AppendString"}}, {"copy", []string{"CopyValues", "CopyString"}}, {"slices.Grow", []string{"Grow"}},
		{"strings.WriteString", []string{"WriteBuilder"}}, {"strings.Write", []string{"WriteBuilderBytes"}}, {"strings.WriteByte", []string{"WriteBuilderByte"}}, {"strings.WriteRune", []string{"WriteBuilderRune"}}, {"strings.Grow", []string{"GrowBuilder"}},
		{"bytes.Write", []string{"WriteBuffer"}}, {"bytes.Grow", []string{"GrowBuffer"}}, {"bytes.WriteByte", []string{"WriteBufferByte"}}, {"bytes.WriteRune", []string{"WriteBufferRune"}}, {"bytes.WriteString", []string{"WriteBufferString"}},
		{"sort.Slice", []string{"SortAny", "SortSlice"}}, {"sort.SliceStable", []string{"SortAnyStable", "SortSliceStable"}}, {"slices.Sort", []string{"SortOrdered"}}, {"sort.Strings", []string{"SortStrings"}}, {"sort.Sort", []string{"SortInterface"}},
	} {
		rules = append(rules, call(entry.kind, module+"pkg/cypher/bulk", entry.names...))
	}
	it := work("iterator", "call")
	it.Names = []string{"NewIterator"}
	rules = append(rules, it)
	rules = append(rules, call("context-boundary", "context", "Background", "TODO"))
	for _, pkg := range []string{"io", "io/ioutil", "os"} {
		s := call("reader", pkg)
		s.NamePrefixes = []string{"Read", "Copy"}
		rules = append(rules, s)
	}
	entrance := work("entrance", "call")
	entrance.PackagePrefixes = []string{module}
	entrance.NameContains = []string{"Reader", "Candidates"}
	entrance.NamePrefixes = []string{"Stream", "stream", "GetAll", "ForEach", "Iterate", "Collect", "collect", "Lookup", "lookup", "GetNodes", "getNodes", "AllNodes", "readNodeRecords", "buffered", "readBufferedEntities", "pendingValueMatches", "ensurePendingValueTracked"}
	rules = append(rules, entrance)
	// KindFromSymbol preserves named primitive families without executable
	// selector callbacks in clients. Names and prefixes are declarative unions.
	slices := call("", "slices", "Grow", "Clone")
	slices.NamePrefixes = []string{"Sort"}
	slices.KindFromSymbol = true
	rules = append(rules, slices)
	for _, pkg := range []string{"strings", "bytes"} {
		s := call("", pkg, "Grow")
		s.NamePrefixes = []string{"Write"}
		s.KindFromSymbol = true
		rules = append(rules, s)
	}
	sort := call("", "sort")
	sort.KindFromSymbol = true
	rules = append(rules, sort)
	json := call("", "encoding/json", "Marshal", "MarshalIndent", "Encode", "NewEncoder")
	json.KindFromSymbol = true
	rules = append(rules, json)
	equal := call("", "reflect", "DeepEqual")
	equal.KindFromSymbol = true
	rules = append(rules, equal)
	fmt := call("", "fmt")
	fmt.NamePrefixes = []string{"Sprint"}
	fmt.KindFromSymbol = true
	rules = append(rules, fmt)
	frame := func(kind, node string) Selector {
		return Selector{Kind: kind, Node: node, Group: "envelope", Reachability: "all"}
	}
	symbol := frame("envelope.format", "identifier")
	symbol.Names = []string{"nodeFormatTokenizedV1"}
	rules = append(rules, symbol)
	shared := frame("envelope.shared", "call")
	shared.Names = []string{"SplitNodeEnvelope"}
	rules = append(rules, shared)
	varint := frame("envelope.varint", "call")
	varint.Names = []string{"Uvarint", "Varint"}
	varint.NameContains = []string{"readuvarint"}
	varint.FoldCase = true
	rules = append(rules, varint)
	slice := frame("envelope.slice", "slice")
	slice.OperandTypes = []string{"[]uint8"}
	rules = append(rules, slice)
	return rules
}
