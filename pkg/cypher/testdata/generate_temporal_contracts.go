// Run on TrueNAS with: go run pkg/cypher/testdata/generate_temporal_contracts.go
// This generates inputs and reference queries, never semantic expectations.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type input struct {
	Name string `json:"name"`
	A    string `json:"a"`
	B    string `json:"b"`
}

func main() {
	families := []input{
		{"date", "date('2024-01-02')", "date({year:2024,month:1,day:2})"},
		{"localtime", "localtime('12:34:56.000000007')", "localtime({hour:12,minute:34,second:56,nanosecond:7})"},
		{"time-offset", "time('12:34:56+01:00')", "time('11:34:56Z')"},
		{"localdatetime", "localdatetime('2024-01-02T12:34:56')", "localdatetime({year:2024,month:1,day:2,hour:12,minute:34,second:56})"},
		{"datetime-offset", "datetime('2024-01-02T12:00Z')", "datetime('2024-01-02T13:00+01:00')"},
		{"datetime-name-offset", "datetime('2024-07-01T12:00+01:00[Europe/London]')", "datetime('2024-07-01T12:00+01:00')"},
		{"datetime-utc-name-offset", "datetime('2024-01-02T12:00[UTC]')", "datetime('2024-01-02T12:00Z')"},
		{"datetime-cet-name-offset", "datetime('2024-01-02T12:00[CET]')", "datetime('2024-01-02T12:00+01:00')"},
		{"datetime-renamed", "datetime('2024-01-02T12:00[Europe/Kiev]')", "datetime('2024-01-02T12:00[Europe/Kyiv]')"},
		{"time-projected", "time(datetime('2024-07-01T12:34:56.000000007+01:00[Europe/London]'))", "time('12:34:56.000000007+01:00')"},
		{"duration-components", "duration('P1D')", "duration('PT24H')"},
		{"duration-seconds", "duration('PT60S')", "duration('PT1M')"},
		{"duration-months", "duration('P1Y')", "duration('P12M')"},
		{"duration-negative-arithmetic", "duration('PT-0.5S')", "duration('PT-0.5S')+duration('PT0S')"},
		{"duration-nanos", "duration({nanoseconds:1000000000})", "duration('PT1S')"},
		// The reviewer's 71 valid scalar/cross-type families. Large-inexact
		// arithmetic has a separate pre-existing = policy and is not uniform.
		{"integer-float", "1", "1.0"},
		{"large-exact", "9007199254740992", "9007199254740992.0"},
		{"zero", "0", "-0.0"},
		{"string-equal", "'1'", "'1'"},
		{"string-number", "'1'", "1"},
		{"list-numbers", "[1,2.0]", "[1.0,2]"},
		{"list-order", "[1,2]", "[2,1]"},
		{"map-order", "{x:1,y:[2.0]}", "{y:[2],x:1.0}"},
		{"map-key", "{x:1}", "{y:1}"},
		{"empty-container", "[]", "{}"},
		{"month-day", "duration('P1M')", "duration('P30D')"},
		{"week-day", "duration('P1W')", "duration('P7D')"},
		{"negative-borrow", "duration({seconds:1,nanoseconds:-1500000000})", "duration('PT-0.5S')"},
		{"positive-carry", "duration({seconds:-1,nanoseconds:1500000000})", "duration('PT0.5S')"},
		{"time-same-offset", "time('12:00Z')", "time('12:00+00:00')"},
		{"datetime-same-offset", "datetime('2024-01-02T12:00Z')", "datetime('2024-01-02T12:00+00:00')"},
		{"named-distinct", "datetime('2024-01-02T12:00[UTC]')", "datetime('2024-01-02T12:00[Etc/UTC]')"},
		{"null", "null", "null"},
		{"null-value", "null", "1"},
		{"null-list", "[null,1]", "[null,1.0]"},
		{"null-map", "{x:null,n:1}", "{n:1.0,x:null}"},
		// Separate expressions produce separate NaNs on the reference: no
		// bound-object identity is asserted for Go's identity-free floats.
		{"nan", "toFloat('NaN')", "toFloat('NaN')"},
		{"nan-number", "toFloat('NaN')", "0"},
	}
	types := []string{"date('2024-01-02')", "localtime('00:00')", "time('00:00Z')", "localdatetime('2024-01-02T00:00')", "datetime('2024-01-02T00:00Z')", "duration('P0D')"}
	for i, a := range types {
		for j, b := range types {
			if i != j {
				families = append(families, input{fmt.Sprintf("type-%d-%d", i, j), a, b})
			}
		}
		for j, b := range []string{"toString(" + a + ")", "0", "{}", "[]"} {
			families = append(families, input{fmt.Sprintf("other-%d-%d", i, j), a, b})
		}
	}
	for _, zone := range []struct{ name, civil, early, late, region string }{
		{"london", "2024-10-27T01:30", "+01:00", "Z", "Europe/London"},
		{"new-york", "2024-11-03T01:30", "-04:00", "-05:00", "America/New_York"},
		{"berlin", "2024-10-27T02:30", "+02:00", "+01:00", "Europe/Berlin"},
		{"lord-howe", "2024-04-07T01:45", "+11:00", "+10:30", "Australia/Lord_Howe"},
		{"auckland", "2024-04-07T02:30", "+13:00", "+12:00", "Pacific/Auckland"},
	} {
		for _, offset := range []string{zone.early, zone.late} {
			other := zone.early
			if offset == other {
				other = zone.late
			}
			region := "datetime('" + zone.civil + offset + "[" + zone.region + "]')"
			counterpart := "datetime('" + zone.civil + other + "[" + zone.region + "]')"
			fixed := "datetime('" + zone.civil + offset + "')"
			label := "overlap-" + zone.name + "-" + offset
			families = append(families, input{label, region, counterpart}, input{label + "-same", region, region}, input{label + "-fixed", region, fixed}, input{label + "-fixed-reverse", fixed, region})
		}
	}
	var cases []input
	var expressions []string
	for _, family := range families {
		for _, shape := range []string{"scalar", "list", "map"} {
			a, b := family.A, family.B
			if shape == "list" {
				a, b = "["+a+"]", "["+b+"]"
			}
			if shape == "map" {
				a, b = "{v:["+a+"]}", "{v:["+b+"]}"
			}
			name := family.Name + "/" + shape
			cases = append(cases, input{name, a, b})
			expressions = append(expressions, "{name:'"+name+"',a:"+a+",b:"+b+"}")
		}
	}
	prefix := "UNWIND [\n" + strings.Join(expressions, ",\n") + "\n] AS p WITH p,p.a AS a,p.b AS b\n"
	core := `CALL { WITH a,b UNWIND [a,b] AS x WITH DISTINCT x RETURN count(*) AS distinctRows }
CALL { WITH a,b UNWIND [a,b] AS x WITH x,count(*) AS n RETURN count(*) AS groups }
CALL { WITH a,b CALL { WITH a RETURN a AS x UNION WITH b RETURN b AS x } RETURN count(*) AS unionRows }
`
	neo := prefix + core + "RETURN p.name AS name, a=b AS equality, distinctRows=1 AS equivalence, distinctRows, groups, unionRows;\n"
	apoc := prefix + core + `CALL apoc.coll.split(['__left',a,'__right'],b) YIELD value
WITH p,a,b,distinctRows,groups,unionRows,count(*) AS partitions
RETURN apoc.convert.toJson({name:p.name,equality:a=b,equivalence:distinctRows=1,distinctRows:distinctRows,groups:groups,unionRows:unionRows,
 lookup:[apoc.coll.contains([a],b),apoc.coll.containsAll([a],[b]),apoc.coll.indexOf([a],b),apoc.coll.occurrences([a],b),partitions],
 sets:[size(apoc.coll.toSet([a,b])),size(apoc.coll.union([a],[b])),size(apoc.coll.intersection([a],[b])),size(apoc.coll.subtract([a],[b])),size(apoc.coll.frequencies([a,b]))]}) AS oracle;
`
	encoded, err := json.Marshal(struct {
		Cases []input `json:"cases"`
		Neo   string  `json:"neo"`
		Apoc  string  `json:"apoc"`
	}{cases, neo, apoc})
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(os.Stdout, "TEMPORAL_REFERENCE_INPUT "+string(encoded))
}
