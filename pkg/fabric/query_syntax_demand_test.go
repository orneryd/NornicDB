package fabric

import (
	"strings"
	"testing"
)

// This is the previous demand contract, retained only as a differential oracle.
// The public semantic matrix separately proves that demand still reaches the
// native role owner; these lexical bits never establish declarations or scope.
func previousSyntaxDemand(text string) SyntaxDemand {
	var demand SyntaxDemand
	for token := range queryTokens(text) {
		if strings.HasPrefix(token.text, "`") {
			demand |= NativeAdmissionSyntax
		}
		if token.text == "[" || token.text == "{" {
			demand |= NativeLocalScopes
		}
		if !token.word {
			continue
		}
		is := func(words ...string) bool {
			for _, word := range words {
				if strings.EqualFold(token.text, word) {
					return true
				}
			}
			return false
		}
		if is("CASE", "AS", "DISTINCT", "WITH", "UNWIND", "FOR", "LET", "FILTER", "FOREACH", "CALL", "USE", "YIELD", "UNION", "CREATE", "INSERT", "MERGE", "SET", "REMOVE", "DELETE", "FINISH", "EXPLAIN", "PROFILE", "CYPHER", "SHOW", "TERMINATE") {
			demand |= NativeAdmissionSyntax
		}
		if is("ALL", "ANY", "NONE", "SINGLE", "FILTER", "REDUCE", "ALLREDUCE", "FOREACH", "FOR", "CALL") {
			demand |= NativeLocalScopes | NativeAdmissionSyntax
		}
		if is("CALL") {
			demand |= NativeCallSyntax
		}
		if is("USE") {
			demand |= NativeUseSyntax
		}
		if is("UNION") {
			demand |= NativeUnionSyntax
		}
		if is("EMBEDDING") {
			demand |= NativeEmbeddingSyntax
		}
	}
	return demand
}

func TestSyntaxDemandPreservesRequestContract(t *testing.T) {
	words := strings.Fields("CASE AS DISTINCT WITH UNWIND FOR LET FILTER FOREACH CALL USE YIELD UNION CREATE INSERT MERGE SET REMOVE DELETE FINISH EXPLAIN PROFILE CYPHER SHOW TERMINATE ALL ANY NONE SINGLE REDUCE ALLREDUCE EMBEDDING MATCH RETURN person.name variableNameLongerThanRequests")
	for _, word := range words {
		for _, variant := range []string{word, strings.ToLower(word), strings.ToUpper(word), strings.ReplaceAll(word, "S", "ſ")} {
			for _, text := range []string{variant, "/* CALL USE */ RETURN '" + variant + "'", "RETURN $" + variant, "RETURN `" + variant + "`", "RETURN [" + variant + "]", "MATCH (" + variant + ":Person) RETURN " + variant + ".name"} {
				if got, want := QuerySyntaxDemand(text), previousSyntaxDemand(text); got != want {
					t.Fatalf("%q demand %d, want %d", text, got, want)
				}
			}
		}
	}
	for _, text := range []string{"CALL /* USE */ { RETURN 1 AS value }", "OPTIONAL CALL () { RETURN 'UNION' AS value }", "RETURN {key: 'CALL'}", "RETURN ['CALL', 'USE']", "RETURN café, Δ.name", "RETURN caſe", "RETURN aſ", "RETURN ſhow", "RETURN 'a\\' CALL'", "RETURN `comma,` AS `doubled``name`"} {
		if got, want := QuerySyntaxDemand(text), previousSyntaxDemand(text); got != want {
			t.Fatalf("%q demand %d, want %d", text, got, want)
		}
	}
	if allocations := testing.AllocsPerRun(20, func() { _ = QuerySyntaxDemand("MATCH (n19:Person) RETURN n19.name LIMIT 10") }); allocations != 0 {
		t.Fatalf("ordinary demand allocated %v", allocations)
	}
}
