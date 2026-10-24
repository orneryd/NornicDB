// Package subqueryeval defines the same subquery regressions for every public route.
package subqueryeval

type Case struct {
	Name, Query string
	Want        any
}

var Setup = []string{
	"CREATE (d:Doc {id:'doc'})-[l:REL {source_id:'missing'}]->(own:Node {document_id:'doc',source_id:'present'})-[:REL {source_id:'present'}]->(:Node), (:Source {id:'present',roles:[]}), (:Source {id:'other',roles:['other']})",
	"CREATE INDEX source_id FOR (s:Source) ON (s.id)",
}

func Cases() []Case {
	body := "MATCH (s:Source {id: l.source_id})"
	return []Case{
		{"with_only", "UNWIND range(1, 2000) AS i WITH i WHERE NOT EXISTS { MATCH (s:Source {id: 'absent-' + toString(i)}) } RETURN count(*)", int64(2000)},
		{"match_only", "MATCH ()-[l:REL]->() WHERE NOT EXISTS { " + body + " } RETURN count(l)", int64(1)},
		{"match_leading", "MATCH ()-[l:REL]->() WHERE NOT EXISTS { " + body + " } AND $nonce IS NOT NULL RETURN count(l)", int64(1)},
		{"match_trailing", "MATCH ()-[l:REL]->() WHERE $nonce IS NOT NULL AND NOT EXISTS { " + body + " } RETURN count(l)", int64(1)},
		{"with_exists", "UNWIND ['present','missing'] AS id WITH id WHERE EXISTS { MATCH (s:Source {id:id}) } RETURN count(*)", int64(1)},
		{"with_count", "UNWIND ['present','missing'] AS id WITH id WHERE COUNT { MATCH (s:Source {id:id}) } > 0 RETURN count(*)", int64(1)},
		{"with_collect", "UNWIND ['present','missing'] AS id WITH id WHERE size(COLLECT { MATCH (s:Source {id:id}) RETURN s.id }) > 0 RETURN count(*)", int64(1)},
		{"return_exists", "MATCH ()-[l:REL]->() RETURN sum(CASE WHEN EXISTS { " + body + " } THEN 1 ELSE 0 END)", int64(1)},
		{"call_exists", "UNWIND ['present','missing'] AS id CALL (id) { RETURN EXISTS { MATCH (s:Source {id:id}) } AS found } RETURN collect(found)", []any{true, false}},
		{"null_key", "UNWIND [null,'missing'] AS id WITH id WHERE NOT EXISTS { MATCH (s:Source {id:id}) } RETURN count(*)", int64(2)},
		{"parameter_context", "UNWIND [1] AS i WITH i WHERE NOT EXISTS { MATCH (s:Source {id:CASE WHEN $nonce > 0 THEN 'present' ELSE 'missing' END}) } RETURN count(*)", int64(0)},
		{"cross_row_kinds", "UNWIND ['present','missing',null,'other','present'] AS id RETURN collect([EXISTS { MATCH (s:Source {id:id}) }, COUNT { MATCH (s:Source {id:id}) }, size(COLLECT { MATCH (s:Source {id:id}) RETURN s.id })])", []any{[]any{true, int64(1), int64(1)}, []any{false, int64(0), int64(0)}, []any{false, int64(0), int64(0)}, []any{true, int64(1), int64(1)}, []any{true, int64(1), int64(1)}}},
		{"bound_node", "MATCH (s:Source {id:'present'}) RETURN EXISTS { MATCH (s:Source {id:'other'}) }", false},
		{"labelless_entity", "MATCH ()-[l:REL]->() WHERE EXISTS { MATCH (s {id:l.source_id}) } RETURN count(l)", int64(1)},
		{"where_entity", "MATCH ()-[l:REL]->() WHERE EXISTS { MATCH (s:Source) WHERE s.id=l.source_id } RETURN count(l)", int64(1)},
		{"return_limit", "RETURN COUNT { MATCH (s:Source) RETURN s.id LIMIT 1 }", int64(1)},
		{"quantifier_scope", "RETURN all(id IN ['present','other'] WHERE EXISTS { MATCH (s:Source {id:id}) })", true},
		{"roles_empty", "MATCH (n:Node) WHERE NOT EXISTS { MATCH (s:Source {id:n.source_id}) WHERE any(i IN range(0,size(coalesce(s.roles,[]))-1) WHERE s.roles[i] <> 'own') } RETURN count(n)", int64(2)},
		{"multihop", "MATCH (d:Doc) WHERE $nonce IS NOT NULL CALL (d) { MATCH (d)-[l:REL]-(own:Node)-[l2:REL]-(s:Node) WHERE own.document_id=d.id AND NOT EXISTS { MATCH (e:Source {id:l.source_id}) WHERE any(i IN range(0,size(coalesce(e.roles,[]))-1) WHERE e.roles[i] <> 'own') } AND NOT EXISTS { MATCH (e:Source {id:own.source_id}) WHERE any(i IN range(0,size(coalesce(e.roles,[]))-1) WHERE e.roles[i] <> 'own') } AND NOT EXISTS { MATCH (e:Source {id:l2.source_id}) WHERE any(i IN range(0,size(coalesce(e.roles,[]))-1) WHERE e.roles[i] <> 'own') } RETURN count(*) AS c } RETURN sum(c)", int64(1)},
	}
}
