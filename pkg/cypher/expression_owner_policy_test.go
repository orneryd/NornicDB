package cypher

import "testing"

var numericAdmissionResult bool

func BenchmarkNumericExpressionPrefixAdmission(b *testing.B) {
	expressions := []string{"1", "-1bad", "1e-3", "0xff", ".5", "+1", " 1", "", "variable", "12345678901234567890"}
	var result bool
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result = numericExpressionAccepted(expressions[i%len(expressions)], numericValuePrefix)
	}
	numericAdmissionResult = result
}

func TestExpressionTextPolicyPopulations(t *testing.T) {
	for _, test := range []struct {
		text              string
		operator, operand bool
	}{
		{"", true, false}, {"[1+2]", false, true}, {"{k:1+2}", false, true}, {"-[1]", true, true}, {"- [1]", true, true},
		{"(a)-[:R]->(b)", false, false}, {"n {.*, k:1}", false, false}, {"n {k:1}", false, false},
		{"1 + n.k", true, true}, {"toString(n.k)", true, true}, {"(n.k)", true, true}, {"$p", true, true}, {"-x", true, true},
		{"x ; y", true, false}, {"--[1]", false, false},
	} {
		if got := expressionTextAccepted(test.text, expressionTextOperator); got != test.operator {
			t.Errorf("operator %q=%v want %v", test.text, got, test.operator)
		}
		if got := expressionTextAccepted(test.text, expressionTextOperand); got != test.operand {
			t.Errorf("operand %q=%v want %v", test.text, got, test.operand)
		}
	}
}

func TestRowOperandPolicyPopulations(t *testing.T) {
	for _, test := range []struct {
		text             string
		simple, compiled bool
	}{
		{"", false, false}, {"[1,2]", false, false}, {"$1x", false, false}, {"$p", true, true}, {"n.a", true, true}, {"n", true, true}, {"null", true, true},
		{"'a'", true, true}, {"(n.a)", false, true}, {"1 + 2", false, true}, {"$p + 2", false, true}, {"size(n.a)", false, true}, {"toUpper(n.a)", false, false},
	} {
		for _, entry := range []struct {
			policy rowOperandPolicy
			want   bool
		}{{rowOperandSimple, test.simple}, {rowOperandCompiled, test.compiled}} {
			_, got := parseRowOperand(test.text, entry.policy)
			if got != entry.want {
				t.Errorf("policy %d %q=%v want %v", entry.policy, test.text, got, entry.want)
			}
		}
	}
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	for _, test := range []struct {
		text      string
		value     interface{}
		wantError bool
	}{
		{"(n.k + 2) * 3", int64(18), false}, {"size(n.list)", int64(2), false}, {"1 / 0", nil, true}, {"size(n.k)", nil, true},
	} {
		operand, ok := parseRowOperand(test.text, rowOperandCompiled)
		if !ok {
			t.Fatalf("did not compile %q", test.text)
		}
		value, resolved, err := operand.evaluate(exec, compiledRowScope{executor: exec, values: map[string]interface{}{"n": map[string]interface{}{"k": int64(4), "list": []interface{}{1, 2}}}})
		if test.wantError {
			if err == nil {
				t.Errorf("%q lost runtime error", test.text)
			}
			continue
		}
		if err != nil || !resolved || value.materialize() != test.value {
			t.Errorf("%q got %v,%v,%v want %v", test.text, value.materialize(), resolved, err, test.value)
		}
	}
}

func TestNumericExpressionPolicyPopulations(t *testing.T) {
	for _, test := range []struct {
		text             string
		prefix, constant bool
	}{
		{"", false, false}, {"1", true, true}, {"-1bad", true, false}, {".5", false, true}, {"-.5", false, true}, {"+1", false, true}, {" 1", false, true},
		{"0xff", true, true}, {"0o17", true, true}, {"1e-3", true, true}, {"1e", true, false}, {"0x", true, false}, {"(1 + 2) * 3", false, true},
		{"variable", false, false}, {"1 2", true, true}, {"1++2", true, true}, {"-", false, false},
		// Admission is a token-population check; grammar validation happens later.
		{"--1", false, true}, {"1..2", true, false}, {"1.2.3", true, false}, {"0x1.2", true, false},
		{"١", false, false}, {"é1", false, false}, {"1é", true, false}, {"\t1", false, true}, {"1\u00a0+2", true, true},
		{" ", false, false}, {".", false, false}, {"/", false, false}, {"_1", false, false}, {"\x001", false, false}, {"1\x00", true, false},
	} {
		if got := numericExpressionAccepted(test.text, numericValuePrefix); got != test.prefix {
			t.Errorf("prefix %q=%v want %v", test.text, got, test.prefix)
		}
		if got := numericExpressionAccepted(test.text, numericConstantPopulation); got != test.constant {
			t.Errorf("constant %q=%v want %v", test.text, got, test.constant)
		}
	}
}

var constantNumericAccepted int

func BenchmarkConstantNumericExpressionAdmission(b *testing.B) {
	expressions := []string{"1+2", "(1.5 - .25)*0xff/0o17", "1e-3 + 2.5e4", "-1bad", "1..2", "1 2", "", "١ + 2", "0x1 + 0o7", "((123456789 * 2) - 7) / 5"}
	accepted := 0
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if numericExpressionAccepted(expressions[i%len(expressions)], numericConstantPopulation) {
			accepted++
		}
	}
	constantNumericAccepted = accepted
}
