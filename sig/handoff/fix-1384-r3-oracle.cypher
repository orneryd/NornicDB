CREATE (:Fix1384R3A {id:1})-[:T {w:7}]->(:Fix1384R3B {id:3}), (:Fix1384R3A {id:2});
MATCH (a:Fix1384R3A) RETURN a.id AS id, EXISTS { WITH 1 AS k MATCH (b:Fix1384R3B) WHERE (a)-[:T]->(b) } AS found ORDER BY id;
MATCH (a:Fix1384R3A) RETURN a.id AS id, EXISTS { WITH * MATCH (b:Fix1384R3B) WHERE (a)-[:T]->(b) } AS found ORDER BY id;
MATCH (a:Fix1384R3A) RETURN a.id AS id, EXISTS { WITH a AS retained MATCH (b:Fix1384R3B) WHERE (a)-[:T]->(b) } AS found ORDER BY id;
MATCH (a:Fix1384R3A) RETURN a.id AS id, EXISTS { WITH 1 AS k WITH a AS restored MATCH (b:Fix1384R3B) WHERE (a)-[:T]->(b) } AS found ORDER BY id;
MATCH (a:Fix1384R3A) RETURN a.id AS id, EXISTS { WITH a AS retained WITH 1 AS k MATCH (b:Fix1384R3B) WHERE (a)-[:T]->(b) } AS found ORDER BY id;
