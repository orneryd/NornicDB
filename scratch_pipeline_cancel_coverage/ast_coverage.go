package main

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type coverBlock struct{ firstLine, firstColumn, lastLine, lastColumn, statements, hits int }

func (block coverBlock) contains(line, column int) bool {
	return (line > block.firstLine || line == block.firstLine && column >= block.firstColumn) &&
		(line < block.lastLine || line == block.lastLine && column < block.lastColumn)
}

func reportCoverage(profile, base, mode string) error {
	raw, err := os.ReadFile(base)
	if err != nil {
		return err
	}
	added := map[string]map[int]bool{}
	hunk := regexp.MustCompile(`^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@`)
	path, line := "", 0
	for _, text := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(text, "+++ b/") {
			path = strings.TrimPrefix(text, "+++ b/")
			if !strings.HasSuffix(path, "_test.go") {
				added[path] = map[int]bool{}
			}
			continue
		}
		if match := hunk.FindStringSubmatch(text); match != nil {
			line, _ = strconv.Atoi(match[1])
			continue
		}
		if strings.HasPrefix(text, "+") {
			if added[path] != nil {
				added[path][line] = true
			}
			line++
		} else if strings.HasPrefix(text, " ") {
			line++
		}
	}
	blocks := map[string][]coverBlock{}
	file, err := os.Open(profile)
	if err != nil {
		return err
	}
	defer file.Close()
	pattern := regexp.MustCompile(`^github.com/orneryd/nornicdb/(.*):(\d+)\.(\d+),(\d+)\.(\d+) (\d+) (\d+)$`)
	scan := bufio.NewScanner(file)
	for scan.Scan() {
		match := pattern.FindStringSubmatch(scan.Text())
		if match == nil {
			continue
		}
		values := make([]int, 6)
		for i := range values {
			values[i], _ = strconv.Atoi(match[i+2])
		}
		block := coverBlock{values[0], values[1], values[2], values[3], values[4], values[5]}
		blocks[match[1]] = append(blocks[match[1]], block)
		if added[match[1]] != nil {
			fmt.Printf("PIPELINE_WORK_RAW_BLOCK %s\n", scan.Text())
		}
	}
	if err := scan.Err(); err != nil {
		return err
	}
	paths := make([]string, 0, len(added))
	for path := range added {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	totalChanged, hitChanged, unmappedChanged := 0, 0, 0
	callerCovered, callerTotal := 0, 0
	for _, path := range paths {
		positions := token.NewFileSet()
		source, err := parser.ParseFile(positions, path, nil, 0)
		if err != nil {
			return err
		}
		// These are distinct AST statement START lines intersecting added diff
		// lines, mapped to instrumented blocks. Raw block spans are separate;
		// comments/braces are never relabeled executable statements or lines.
		statementLines := map[int]bool{}
		mappedLines := map[int]bool{}
		ast.Inspect(source, func(node ast.Node) bool {
			statement, ok := node.(ast.Stmt)
			if !ok {
				return true
			}
			switch statement.(type) {
			case *ast.BlockStmt, *ast.EmptyStmt, *ast.CaseClause, *ast.CommClause:
				return true
			}
			position := positions.Position(statement.Pos())
			if !added[path][position.Line] {
				return true
			}
			if _, seen := statementLines[position.Line]; !seen {
				statementLines[position.Line] = false
			}
			for _, block := range blocks[path] {
				if block.statements > 0 && block.contains(position.Line, position.Column) {
					mappedLines[position.Line] = true
					statementLines[position.Line] = statementLines[position.Line] || block.hits > 0
				}
			}
			return true
		})
		lines := make([]int, 0, len(statementLines))
		for line := range statementLines {
			lines = append(lines, line)
		}
		sort.Ints(lines)
		for _, line := range lines {
			totalChanged++
			if statementLines[line] {
				hitChanged++
			}
			if !mappedLines[line] {
				unmappedChanged++
			}
			fmt.Printf("PIPELINE_WORK_CHANGED_AST_START_LINE path=%s line=%d mapped=%t covered=%t\n", path, line, mappedLines[line], statementLines[line])
		}
		for _, declaration := range source.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			first, last := positions.Position(function.Pos()).Line, positions.Position(function.End()).Line
			affected := false
			for changed := range added[path] {
				if changed >= first && changed <= last {
					affected = true
					break
				}
			}
			if mode == "green" && path == "pkg/cypher/optional_match_traversal_agg.go" && function.Name.Name == "subqueryExpressionEnd" {
				affected = true
			}
			if !affected {
				continue
			}
			covered, total := 0, 0
			for _, block := range blocks[path] {
				if block.firstLine >= first && block.lastLine <= last {
					total += block.statements
					if block.hits > 0 {
						covered += block.statements
					}
				}
			}
			fmt.Printf("PIPELINE_WORK_FUNCTION path=%s name=%s first=%d last=%d coveredStatements=%d totalStatements=%d\n", path, function.Name.Name, first, last, covered, total)
			if path == "pkg/cypher/optional_match_traversal_agg.go" && function.Name.Name == "subqueryExpressionEnd" {
				callerCovered, callerTotal = covered, total
			}
		}
	}
	fmt.Printf("PIPELINE_WORK_CHANGED_AST_START_LINES base=%s covered=%d total=%d unmapped=%d\n", base, hitChanged, totalChanged, unmappedChanged)
	if totalChanged == 0 {
		fmt.Println("PIPELINE_WORK_NO_ADDED_AST_STATEMENT_LINES no_0_of_0_pass_claim")
	}
	if totalChanged > 0 && (unmappedChanged > 0 || hitChanged*100 < totalChanged*95) {
		return fmt.Errorf("changed AST start-line coverage below95 or unmapped; raw blocks retained")
	}
	if mode == "green" && (callerTotal == 0 || callerCovered != callerTotal) {
		return fmt.Errorf("surviving deletion-only aggregate caller coverage incomplete: %d/%d", callerCovered, callerTotal)
	}
	return nil
}
