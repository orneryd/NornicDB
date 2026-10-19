// Package functionsyntaxmatrix generates the shared lexical differential corpus.
package functionsyntaxmatrix

import (
	"fmt"
	"strings"
)

type Case struct{ ID, Query, Rows string }
type family struct {
	name, prefix, suffix string
	args                 []string
	rows                 string
}

// NativeCases cover existing NornicDB extensions absent from Neo4j 5.26.
// Keep these out of the differential oracle while exercising the same routes.
func NativeCases() []Case {
	return []Case{
		{"native/alias-name-gap", "RETURN ceiling /* don't range('bad'), */ (1.2) AS v", `[[2]]`},
		{"native/alias-argument", "RETURN ceiling(/* don't range('bad'), */ 1.2) AS v", `[[2]]`},
		{"native/quoted-alias", "RETURN `ceiling`(/* don't range('bad'), */ 1.2) AS v", `[[2]]`},
		{"native/property-exists", "CREATE (n {v:1}) RETURN PROPERTY_EXISTS(/* gap */ n, /* don't range('bad'), */ v) AS v", `[[true]]`},
	}
}

func Cases() []Case {
	families := []family{
		{"substring", "", "", []string{"toLower(§'OKAY')", "(§0)", "(§2)"}, `[["ok"]]`},
		{"range", "", "", []string{"(§1)", "(§3)", "(§1)"}, `[[[1,2,3]]]`},
		{"replace", "", "", []string{"(§'okay')", "(§'a')", "(§'A')"}, `[["okAy"]]`},
		{"coalesce", "", "", []string{"(§null)", "(§null)", "(§'okay')"}, `[["okay"]]`},
		{"size", "", "", []string{"[§{`a,b`:[1,2],msg:'okay'}]"}, `[[1]]`},
		{"percentileCont", "UNWIND [1,2,3] AS x ", "", []string{"(§x)", "(§0.5)"}, `[[2]]`},
		{"count", "UNWIND [1,2,3] AS x ", "", []string{"(§x)"}, `[[3]]`},
		{"reverse", "", "", []string{"[§1,2,3]"}, `[[[3,2,1]]]`},
		{"head", "", ".v", []string{"[§{v:'okay',`a,b`:[1,2]}]"}, `[["okay"]]`},
		{"reduce", "", "", []string{"a=§0", "x IN [§1,2] | a+x"}, `[[3]]`},
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
					add(fmt.Sprintf("%s/%s/%d/%s", f.name, style, contentIndex, placement), f.prefix+"RETURN "+before+f.name+gap+"("+strings.Join(args, ",")+")"+after+f.suffix+" AS v", f.rows)
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
	cases = append(cases, MapAggregateCases()...)
	return cases
}

// MapAggregateCases exercises expression colons independently of label/type
// colons, including quoted keys and trivia on both sides of the separator.
func MapAggregateCases() []Case {
	aggregates := []struct{ name, args, value string }{
		{"count", "x", "2"}, {"sum", "x", "3"}, {"avg", "x", "1.5"},
		{"collect", "x", "[1,2]"}, {"min", "x", "1"}, {"max", "x", "2"},
		{"percentileCont", "x,0.5", "1.5"},
	}
	contents := []string{"don't split", `a "quote"`, "commas,,", " ) ( [ ] { } ", "count('bad'), range('bad')", "`odd,name`", `escaped \' and \"`}
	var cases []Case
	for _, aggregate := range aggregates {
		for _, key := range []string{"c", "`c`"} {
			rows := `[[{"c":` + aggregate.value + `}]]`
			add := func(id, body string) {
				cases = append(cases, Case{"map-aggregate/" + aggregate.name + "/" + key + "/" + id, "UNWIND [1,2] AS x RETURN {" + body + "} AS v", rows})
			}
			call := aggregate.name + "(" + aggregate.args + ")"
			add("plain", key+":"+call)
			for contentIndex, content := range contents {
				for _, style := range []string{"block", "line"} {
					comment := "/* " + content + " */ "
					if style == "line" { comment = "// " + content + "\n" }
					prefix := fmt.Sprintf("%s/%d/", style, contentIndex)
					add(prefix+"before-key", comment+key+":"+call)
					add(prefix+"after-key", key+comment+":"+call)
					add(prefix+"after-colon", key+":"+comment+call)
					add(prefix+"name-gap", key+":"+aggregate.name+comment+"("+aggregate.args+")")
					add(prefix+"argument", key+":"+aggregate.name+"("+comment+aggregate.args+")")
					add(prefix+"after-call", key+":"+call+comment)
				}
			}
		}
	}
	cases = append(cases,
		Case{"slice/function-after-boundary", "RETURN [1,2,3][..size([0,0])] AS v", `[[[1,2]]]`},
		Case{"slice/both-function-bounds", "RETURN [1,2,3][size([])..size([0,0])] AS v", `[[[1,2]]]`},
		Case{"slice/map-value-aggregate", "UNWIND [1,2] AS x RETURN {c:[1,2,3][..count(x)]} AS v", `[[{"c":[1,2]}]]`},
	)
	return cases
}
