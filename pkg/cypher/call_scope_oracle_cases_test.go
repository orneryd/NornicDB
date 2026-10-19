package cypher

import (
	"fmt"
	"strings"
)

type callScopeOracleCase struct {
	name          string
	query         string
	reference     string
	compatibility string
	extension     string
	call          string
	projection    string
	depth         int
	kinds         []string
	union         string
	unionLevel    int
	seed          string
}

// The generator is independent of the production parser and scope walker.
// It constructs each UNION branch separately, including the asymmetric leaf
// projections that exposed the wildcard-first import omission.
func generatedCallScopeOracleCases() []callScopeOracleCase {
	var cases []callScopeOracleCase
	for _, call := range []string{"explicit", "all", "implicit", "with", "with-all"} {
		for _, projection := range []string{"return-all", "with-all", "explicit"} {
			for _, seed := range []string{"empty", "match"} {
				for depth := 0; depth <= 2; depth++ {
					for _, kinds := range callScopeOracleKindSequences(depth) {
						for _, union := range []string{"none", "UNION", "UNION ALL"} {
							levels := []int{-1}
							if union != "none" {
								levels = nil
								for level := 0; level <= depth; level++ {
									levels = append(levels, level)
								}
							}
							for _, level := range levels {
								item := callScopeOracleCase{
									call: call, projection: projection, depth: depth,
									kinds: append([]string(nil), kinds...), union: union,
									unionLevel: level, seed: seed,
								}
								item.name = fmt.Sprintf("%s/%s/%s/depth-%d/%s/%s/level-%d", call, projection, seed, depth, strings.Join(kinds, "-"), union, level)
								branches := item.branches(0)
								body := item.callBody(branches, false)
								item.query = "WITH a UNWIND [a,a+1] AS scopeRow WITH scopeRow AS a " + body + " RETURN *"
								item.reference = "WITH $outer AS a UNWIND [a,a+1] AS scopeRow WITH scopeRow AS a " + body + " RETURN *"
								if call == "implicit" || call == "with" || call == "with-all" {
									// A differently named outer variable avoids Neo4j's ban
									// on returning an unchanged outer name. A normal WITH
									// then binds a locally, exactly at the import boundary.
									// Both original and compatibility outcomes are retained.
									item.compatibility = "WITH $outer AS scopeInput UNWIND [scopeInput,scopeInput+1] AS scopeRow WITH scopeRow AS scopeInput " + item.callBody(branches, true) + " RETURN *"
									if call == "implicit" {
										item.extension = "907-unscoped-implicit-import"
									} else {
										item.extension = "907-return-outer-unchanged"
									}
								}
								cases = append(cases, item)
							}
						}
					}
				}
			}
		}
	}
	cases = append(cases, generatedNestedCallScopeOracleCases()...)
	return append(cases, callScopeOracleExtensionControls()...)
}

func callScopeOracleExtensionControls() []callScopeOracleCase {
	var cases []callScopeOracleCase
	for _, call := range []string{"explicit", "all", "implicit", "with", "with-all"} {
		item := callScopeOracleCase{name: "control/unchanged/" + call, call: call, union: "none"}
		body := item.callBody([]string{"RETURN a"}, false)
		item.query = "WITH a " + body + " RETURN *"
		item.reference = "WITH $outer AS a " + body + " RETURN *"
		item.extension = "907-return-outer-unchanged"
		item.compatibility = "WITH $outer AS scopeInput CALL (scopeInput) { RETURN scopeInput AS a } RETURN *"
		if call == "implicit" {
			item.extension = "907-unscoped-implicit-import"
		}
		cases = append(cases, item)
		quoted := callScopeOracleCase{name: "control/quoted/" + call, call: call, union: "none"}
		quotedBody := quoted.callBody([]string{"RETURN `a` + 1 AS v"}, false)
		quoted.query = "WITH a " + quotedBody + " RETURN a,v"
		quoted.reference = "WITH $outer AS a " + quotedBody + " RETURN a,v"
		if call == "implicit" {
			quoted.extension = "907-unscoped-implicit-import"
			quoted.compatibility = "WITH $outer AS scopeInput CALL { WITH scopeInput WITH scopeInput AS a RETURN `a` + 1 AS v } RETURN scopeInput AS a,v"
		}
		cases = append(cases, quoted)
	}
	for _, call := range []string{"explicit", "all"} {
		for _, binding := range []string{"WITH a+1 AS a", "UNWIND [a+1] AS a"} {
			item := callScopeOracleCase{name: "control/redeclaration/" + call + "/" + binding, call: call, union: "none", extension: "907-scoped-redeclaration"}
			body := item.callBody([]string{binding + " RETURN a AS v"}, false)
			item.query = "WITH a " + body + " RETURN a,v"
			item.reference = "WITH $outer AS a " + body + " RETURN a,v"
			compatibleBinding := strings.ReplaceAll(binding, "a+1", "scopeInput+1")
			item.compatibility = "WITH $outer AS scopeInput CALL (scopeInput) { " + compatibleBinding + " RETURN a AS v } RETURN scopeInput AS a,v"
			cases = append(cases, item)
		}
	}
	for _, kind := range []string{"COLLECT", "COUNT", "EXISTS"} {
		body := "CALL (a) { RETURN " + kind + " { WITH 9 AS a RETURN a AS v } AS value } RETURN a,value"
		cases = append(cases, callScopeOracleCase{name: "control/scalar-shadow/" + kind, query: "WITH a " + body, reference: "WITH $outer AS a " + body})
	}
	return cases
}

func callScopeOracleKindSequences(depth int) [][]string {
	if depth == 0 {
		return [][]string{{}}
	}
	var sequences [][]string
	for _, kind := range []string{"COLLECT", "COUNT", "EXISTS"} {
		for _, tail := range callScopeOracleKindSequences(depth - 1) {
			sequence := append([]string{kind}, tail...)
			sequences = append(sequences, sequence)
		}
	}
	return sequences
}

func (item callScopeOracleCase) branches(level int) []string {
	var body, alternate string
	if level == item.depth {
		prefix := ""
		if item.seed == "match" {
			prefix = "MATCH (b:CallScopeOracleTarget {oracleRun:$oracleRun}) "
		}
		switch item.projection {
		case "return-all":
			body = prefix + "RETURN *"
		case "with-all":
			body = prefix + "WITH * RETURN *"
		default:
			body = prefix + "RETURN a AS value"
		}
		alternate = body
		if item.projection != "explicit" {
			alternate = prefix + "RETURN a"
			if item.seed == "match" {
				alternate = prefix + "RETURN b"
			}
		}
	} else {
		inner := strings.Join(item.branches(level+1), " "+item.union+" ")
		body = "RETURN " + item.kinds[level] + " { " + inner + " } AS value"
		alternate = body
	}
	if item.unionLevel == level {
		return []string{body, alternate}
	}
	return []string{body}
}

func (item callScopeOracleCase) callBody(branches []string, compatibility bool) string {
	header, prefix := "CALL { ", ""
	switch item.call {
	case "explicit":
		header = "CALL (a) { "
	case "all":
		header = "CALL (*) { "
	case "with":
		prefix = "WITH a "
	case "with-all":
		prefix = "WITH * "
	}
	if compatibility {
		header = "CALL { "
		prefix = "WITH scopeInput WITH scopeInput AS a " + prefix
	}
	parts := make([]string, len(branches))
	for index, branch := range branches {
		parts[index] = prefix + branch
	}
	separator := " " + item.union + " "
	return header + strings.Join(parts, separator) + " }"
}
