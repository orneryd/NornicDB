CALL dbms.components() YIELD versions RETURN versions;
CREATE (a:PredicateProbe {id:1})-[:KNOWS]->(b:PredicateProbe {id:2});
MATCH (a:PredicateProbe {id:1}), (b:PredicateProbe {id:2}) WHERE (a)-[:KNOWS]->(c) RETURN count(*) AS unbound_node;
MATCH (a:PredicateProbe {id:1}), (b:PredicateProbe {id:2}) WHERE (a)-[r:KNOWS]->(b) RETURN count(*) AS unbound_relationship;
MATCH (a:PredicateProbe {id:1}), (b:PredicateProbe {id:2}) WHERE (a)-[:KNOWS]->() RETURN count(*) AS anonymous_endpoint;
MATCH (a:PredicateProbe {id:1}), (b:PredicateProbe {id:2}) WHERE EXISTS { (a)-[:KNOWS]->(c) } RETURN count(*) AS explicit_exists_local;
MATCH (a:PredicateProbe {id:1}), (b:PredicateProbe {id:2}) WHERE exists((a)-[:KNOWS]->(c)) RETURN count(*) AS exists_function_unbound;
MATCH (a:PredicateProbe {id:1}), (b:PredicateProbe {id:2}) WHERE NOT (a)-[:KNOWS]->(c) RETURN count(*) AS negated_unbound;