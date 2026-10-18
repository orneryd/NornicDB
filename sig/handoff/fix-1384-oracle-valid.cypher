CALL dbms.components() YIELD versions RETURN versions;
CREATE (a:PredicateProbe {id:1})-[:KNOWS]->(b:PredicateProbe {id:2});
MATCH (a:PredicateProbe {id:1}), (b:PredicateProbe {id:2}) WHERE (a)-[:KNOWS]->() RETURN count(*) AS anonymous_endpoint;
MATCH (a:PredicateProbe {id:1}), (b:PredicateProbe {id:2}) WHERE EXISTS { (a)-[:KNOWS]->(c) } RETURN count(*) AS explicit_exists_local;