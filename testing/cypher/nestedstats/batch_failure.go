package nestedstats

// BatchFailureCases exercise errors after an earlier row has written. The
// shipped periodic implementation returns a statement error; ON ERROR policies
// are not supported on the release base. Neither may publish a success summary.
func BatchFailureCases() []Case {
	cases := []Case{}
	for _, policy := range []string{"CONTINUE", "BREAK"} {
		cases = append(cases, Case{Name: "subquery" + policy, Query: "UNWIND [1,0] AS i CALL { WITH i CREATE (:BatchFailureStats {p:i}) RETURN 1/i AS v } IN TRANSACTIONS OF 2 ROWS ON ERROR " + policy + " RETURN v"})
	}
	for _, name := range []string{"iterate", "rock_n_roll"} {
		cases = append(cases, Case{Name: name, Query: "CALL apoc.periodic." + name + "('UNWIND [1,0] AS i RETURN i','CREATE (:BatchFailureStats {p:$i}) RETURN 1/$i AS v',{batchSize:2}) YIELD committedOperations RETURN committedOperations"})
	}
	cases = append(cases, Case{Name: "commit", Query: "CALL apoc.periodic.commit('UNWIND [1,0] AS i CREATE (:BatchFailureStats {p:i}) RETURN 1/i AS v',{}) YIELD updates RETURN updates"})
	for _, tc := range append([]Case(nil), cases...) {
		cases = append(cases,
			Case{Name: "dynamic_" + tc.Name, Query: "CALL apoc.cypher.run($body,{}) YIELD value RETURN value", Params: map[string]any{"body": tc.Query}},
		)
		if tc.Name != "subqueryCONTINUE" && tc.Name != "subqueryBREAK" {
			cases = append(cases, Case{Name: "stored_" + tc.Name, Setup: "CREATE OR REPLACE PROCEDURE nestedstats.batchfailure($body) MODE WRITE AS CALL apoc.cypher.run($body,{}) YIELD value RETURN value", Query: "CALL nestedstats.batchfailure($body) YIELD value RETURN value", Params: map[string]any{"body": tc.Query}})
		}
	}
	return cases
}
