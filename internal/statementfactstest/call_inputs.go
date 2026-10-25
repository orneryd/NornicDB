package statementfactstest

import "fmt"

// CallInputCases crosses import form, nesting, constituent and cardinality.
// UNWIND forces participant dispatch instead of the simple RETURN shortcut.
func CallInputCases() []Case {
	var cases []Case
	for _, mode := range []string{"implicit", "legacy", "listed", "all", "none"} {
		for depth := 1; depth <= 3; depth++ {
			for _, graph := range []string{"one", "remote"} {
				for _, empty := range []bool{false, true} {
					body := "value"
					if mode == "none" {
						body = "7"
					}
					body = "RETURN " + body + " AS result0"
					for level := 0; level < depth; level++ {
						frame, importing := "CALL { ", ""
						switch mode {
						case "legacy":
							importing = "WITH value "
						case "listed":
							frame = "CALL (value) { "
						case "all":
							frame = "CALL (*) { "
						case "none":
							frame = "CALL () { "
						}
						body = fmt.Sprintf("%sUSE comp.%s %sUNWIND [0] AS seed%d %s } RETURN result%d AS result%d", frame, graph, importing, level, body, level, level+1)
					}
					input := "WITH 7 AS value, 99 AS unused "
					if empty {
						input = "UNWIND [] AS value WITH value, 99 AS unused "
					}
					query := fmt.Sprintf("USE comp %s%s", input, body)
					cases = append(cases, Case{Name: fmt.Sprintf("call_inputs/%s/depth=%d/%s/empty=%v", mode, depth, graph, empty), Query: query, Columns: []string{fmt.Sprintf("result%d", depth)}})
				}
			}
		}
	}
	for _, mode := range []string{"implicit", "listed"} {
		frame := "CALL { "
		if mode == "listed" {
			frame = "CALL (value) { "
		}
		for _, empty := range []bool{false, true} {
			input := "WITH 7 AS value "
			if empty {
				input = "UNWIND [] AS value "
			}
			cases = append(cases, Case{Name: fmt.Sprintf("call_inputs/review/%s/empty=%v", mode, empty), Query: "USE comp " + input + frame + "USE comp.remote UNWIND [0] AS seed RETURN value AS x } RETURN x", Columns: []string{"x"}})
		}
	}
	for _, mode := range []string{"implicit", "listed", "mixed", "legacy"} {
		for _, empty := range []bool{false, true} {
			input := "WITH 7 AS value, 7 AS legacy, 99 AS unused "
			if empty {
				input = "UNWIND [] AS value WITH value, 7 AS legacy, 99 AS unused "
			}
			frame, importing, value := "CALL { ", "", "value"
			switch mode {
			case "listed":
				frame = "CALL (value) { "
			case "mixed":
				importing = "WITH legacy "
			case "legacy":
				importing, value = "WITH value ", "7"
			}
			// The first WITH drops legacy inputs. Implicit and scoped inputs
			// must remain available after the nested fragment returns.
			body := "USE comp.one " + importing + "WITH 1 AS scratch CALL { USE comp.remote UNWIND [0] AS seed RETURN 1 AS kept } UNWIND [0] AS tail RETURN " + value + " AS x"
			cases = append(cases, Case{Name: fmt.Sprintf("call_inputs/retention/%s/empty=%v", mode, empty), Query: "USE comp " + input + frame + body + " } RETURN x", Columns: []string{"x"}})
		}
	}
	for _, reverse := range []bool{false, true} {
		for _, empty := range []bool{false, true} {
			input := "WITH 7 AS value, 99 AS unused "
			if empty {
				input = "UNWIND [] AS value WITH value, 99 AS unused "
			}
			legacy := "USE comp.remote WITH value WITH 7 AS scratch UNWIND [0] AS seed RETURN scratch AS x"
			implicit := "USE comp.remote UNWIND [0] AS seed RETURN value AS x"
			if reverse {
				legacy, implicit = implicit, legacy
			}
			cases = append(cases, Case{Name: fmt.Sprintf("call_inputs/union/reverse=%v/empty=%v", reverse, empty), Query: "USE comp " + input + "CALL { " + legacy + " UNION " + implicit + " } RETURN x", Columns: []string{"x"}})
		}
	}
	for _, empty := range []bool{false, true} {
		input := "WITH 7 AS value, 99 AS unused "
		if empty {
			input = "UNWIND [] AS value WITH value, 99 AS unused "
		}
		cases = append(cases, Case{Name: fmt.Sprintf("call_inputs/mid_use/empty=%v", empty), Query: "USE comp " + input + "CALL { USE comp.one WITH value UNWIND [0] AS first USE comp.remote UNWIND [0] AS second RETURN value AS x } RETURN x", Columns: []string{"x"}})
	}

	for _, listed := range []bool{false, true} {
		for _, empty := range []bool{false, true} {
			frame := "CALL { "
			if listed {
				frame = "CALL (value) { "
			}
			input := "WITH 7 AS value, 99 AS unused "
			if empty {
				input = "UNWIND [] AS value WITH value, 99 AS unused "
			}
			cases = append(cases, Case{Name: fmt.Sprintf("call_inputs/leading_with_use/listed=%v/empty=%v", listed, empty), Query: "USE comp " + input + frame + "WITH 1 AS scratch USE comp.remote UNWIND [0] AS seed RETURN value AS x } RETURN x", Columns: []string{"x"}})
		}
	}

	return cases
}
