// Package functionsyntaxmatrix generates the shared lexical differential corpus.
package functionsyntaxmatrix

import (
	"fmt"
	"strings"
)

type Case struct{ ID, Query, Rows string }
type family struct {
	name, prefix string
	args         []string
	rows         string
}

func Cases() []Case {
	families := []family{
		{"substring", "", []string{"toLower(§'OKAY')", "(§0)", "(§2)"}, `[["ok"]]`},
		{"range", "", []string{"(§1)", "(§3)", "(§1)"}, `[[[1,2,3]]]`},
		{"replace", "", []string{"(§'okay')", "(§'a')", "(§'A')"}, `[["okAy"]]`},
		{"coalesce", "", []string{"(§null)", "(§null)", "(§'okay')"}, `[["okay"]]`},
		{"size", "", []string{"[§{`a,b`:[1,2],msg:'okay'}]"}, `[[1]]`},
		{"percentileCont", "UNWIND [1,2,3] AS x ", []string{"(§x)", "(§0.5)"}, `[[2]]`},
		{"count", "UNWIND [1,2,3] AS x ", []string{"(§x)"}, `[[3]]`},
		{"reverse", "", []string{"[§1,2,3]"}, `[[[3,2,1]]]`},
	}
	contents := []string{"don't interpret prose", `a "quoted" word`, "comma, comma,", " ) ( [ ] { } ", "range('bad'), percentileCont('bad')", "`odd,name`", `escaped \' and \"`}
	var cases []Case
	add := func(id, query, rows string) { cases = append(cases, Case{id, query, rows}) }
	for _, f := range families {
		base := make([]string, len(f.args))
		for i, arg := range f.args {
			base[i] = strings.ReplaceAll(arg, "§", "")
		}
		for contentIndex, content := range contents {
			for _, style := range []string{"block", "line"} {
				comment := "/* " + content + " */ "
				if style == "line" {
					comment = "// " + content + "\n"
				}
				emit := func(placement string, args []string, before, gap, after string) {
					add(fmt.Sprintf("%s/%s/%d/%s", f.name, style, contentIndex, placement), f.prefix+"RETURN "+before+f.name+gap+"("+strings.Join(args, ",")+")"+after+" AS v", f.rows)
				}
				emit("before-call", base, comment, "", "")
				emit("name-gap", base, "", comment, "")
				emit("after-call", base, "", "", comment)
				for index := range base {
					for _, position := range []string{"before", "inside", "after"} {
						args := append([]string(nil), base...)
						switch position {
						case "before":
							args[index] = comment + args[index]
						case "inside":
							args[index] = strings.ReplaceAll(f.args[index], "§", comment)
						case "after":
							args[index] += comment
						}
						emit(fmt.Sprintf("arg-%d-%s", index, position), args, "", "", "")
					}
					if index+1 < len(base) {
						args := append([]string(nil), base...)
						args[index+1] = comment + args[index+1]
						emit(fmt.Sprintf("between-%d", index), args, "", "", "")
					}
				}
			}
		}
	}
	add("leading-block", `/* } { ' " */ RETURN 'ok' AS v`, `[["ok"]]`)
	add("leading-line", "// } { ' \"\nRETURN 'ok' AS v", `[["ok"]]`)
	add("escaped-string", `RETURN substring('it\'s range(\'bad\')',0,2) AS v`, `[["it"]]`)
	add("quoted-data", `RETURN substring('/*literal,*/',0,2) AS v`, `[["/*"]]`)
	return cases
}
