// Package nestedstats defines shared public-path cases for nested write summaries.
package nestedstats

type Case struct {
	Name, Setup, Query, Direct string
	Params                     map[string]any
}

func Cases() []Case {
	body := "CREATE (:NestedStats {p: 1}) RETURN 1 AS value"
	params := map[string]any{"body": body}
	return []Case{
		{Name: "run", Query: "CALL apoc.cypher.run($body,{}) YIELD value RETURN value", Direct: body, Params: params},
		{Name: "relationshipCreate", Query: "CALL apoc.cypher.doIt($body,{}) YIELD value RETURN value", Direct: "CREATE (a:NestedStats {p:1})-[r:NESTED {p:2}]->(b:NestedStats) RETURN 1 AS value", Params: map[string]any{"body": "CREATE (a:NestedStats {p:1})-[r:NESTED {p:2}]->(b:NestedStats) RETURN 1 AS value"}},
		{Name: "setRemove", Setup: "CREATE (:NestedStats:OldLabel {p:1})", Query: "CALL apoc.cypher.doIt($body,{}) YIELD value RETURN value", Direct: "MATCH (n:NestedStats) SET n.p=2,n:NewLabel REMOVE n:OldLabel RETURN 1 AS value", Params: map[string]any{"body": "MATCH (n:NestedStats) SET n.p=2,n:NewLabel REMOVE n:OldLabel RETURN 1 AS value"}},
		{Name: "detachDelete", Setup: "CREATE (:NestedStats)-[:NESTED]->(:NestedStats)", Query: "CALL apoc.cypher.doIt($body,{}) YIELD value RETURN value", Direct: "MATCH (n:NestedStats) DETACH DELETE n RETURN count(n)", Params: map[string]any{"body": "MATCH (n:NestedStats) DETACH DELETE n RETURN count(n)"}},
		{Name: "doIt", Query: "CALL apoc.cypher.doIt($body,{}) YIELD value RETURN value", Direct: body, Params: params},
		{Name: "doitall", Query: "CALL apoc.cypher.doitall($body,{}) YIELD value RETURN value", Direct: body, Params: params},
		{Name: "runMany", Query: "CALL apoc.cypher.runMany($body,{}) YIELD row RETURN count(row)", Direct: body, Params: params},
		{Name: "iterate", Query: "CALL apoc.periodic.iterate('RETURN 1 AS i', $body,{}) YIELD total RETURN total", Direct: body, Params: params},
		{Name: "iterateWritingInput", Query: "CALL apoc.periodic.iterate($body, 'RETURN 1 AS v',{}) YIELD total RETURN total", Direct: body, Params: params},
		{Name: "rock_n_roll", Query: "CALL apoc.periodic.rock_n_roll('RETURN 1 AS i', $body,{}) YIELD total RETURN total", Direct: body, Params: params},
		{Name: "commit", Setup: "CREATE (:NestedCommit)", Query: "CALL apoc.periodic.commit('MATCH (n:NestedCommit) DELETE n RETURN count(n)',{}) YIELD updates RETURN updates", Direct: "MATCH (n:NestedCommit) DELETE n RETURN count(n)"},
		{Name: "exportJSON", Query: "CALL apoc.export.json.query($body,'',{}) YIELD rows RETURN rows", Direct: body, Params: params},
		{Name: "exportCSV", Query: "CALL apoc.export.csv.query($body,'',{}) YIELD rows RETURN rows", Direct: body, Params: params},
		{Name: "storedProcedure", Setup: "CREATE OR REPLACE PROCEDURE nestedstats.write() MODE WRITE AS " + body, Query: "CALL nestedstats.write() YIELD value RETURN value", Direct: body},
		{Name: "subquery", Query: "CALL { CREATE (:NestedStats {p: 1}) RETURN 1 AS value } RETURN value", Direct: body},
		{Name: "inTransactions", Query: "CALL { CREATE (:NestedStats {p: 1}) RETURN 1 AS value } IN TRANSACTIONS OF 1 ROW RETURN value", Direct: body},
		{Name: "nestedTwice", Query: "CALL apoc.cypher.run('CALL apoc.cypher.run($body,{}) YIELD value RETURN value',{body:$body}) YIELD value RETURN value", Direct: body, Params: params},
		{Name: "sharedPipelineResult", Query: "UNWIND [1,2] AS i CALL apoc.cypher.run($body,{}) YIELD value RETURN count(value)", Direct: body, Params: params},
		{Name: "runManyStatements", Query: "CALL apoc.cypher.runMany($body,{}) YIELD row RETURN count(row)", Direct: "CREATE (:NestedStats {p:1}),(:NestedStats {p:1}) RETURN 1 AS value", Params: map[string]any{"body": body + ";" + body}},
		{Name: "pipeline", Query: "UNWIND [1,2] AS i CALL apoc.cypher.doIt($body,{}) YIELD value RETURN count(value)", Direct: "UNWIND [1,2] AS i CREATE (:NestedStats {p: 1}) RETURN count(i)", Params: params},
		{Name: "emptyRows", Query: "CALL apoc.cypher.doIt('CREATE (:NestedStats {p: 1})',{}) YIELD value RETURN value", Direct: "CREATE (:NestedStats {p: 1})"},
		{Name: "filteredRows", Query: "CALL apoc.cypher.doIt($body,{}) YIELD value WITH value WHERE false RETURN value", Direct: body, Params: params},
		{Name: "outerWrite", Query: "CALL apoc.cypher.doIt($body,{}) YIELD value CREATE (:NestedStats {p: 1}) RETURN value", Direct: "CREATE (:NestedStats {p: 1}), (:NestedStats {p: 1}) RETURN 1 AS value", Params: params},
	}
}
