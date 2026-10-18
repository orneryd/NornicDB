CREATE (a:PredicateProbe {id:1})-[:KNOWS]->(b:PredicateProbe {id:2});
MATCH (a:PredicateProbe {id:1}), (b:PredicateProbe {id:2}) WHERE (a)-[r:KNOWS]->(b) RETURN count(*) AS unbound_relationship;