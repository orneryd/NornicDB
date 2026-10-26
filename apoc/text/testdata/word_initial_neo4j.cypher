// Author: Codex implementation lane for NOR-207, 2026-10-26.
// Oracle source: neo4j/apoc tag 5.26.30, core/src/main/java/apoc/text/Strings.java:510-547.
CALL dbms.components() YIELD versions RETURN versions;
UNWIND ['hello world','HELLO WORLD','hELLO wORLD','  hello   World  ','','   ','hello\tworld','HELLO\nWORLD','\thello world\n','hello \tworld','\u0000hello world\u001f','hello-world foo.bar \'word','hello\u00a0world','\u00a0hello world\u00a0','élan Über','ǳungla ǆungla','ßeta İSTANBUL','𐐨abc 𐐀XYZ','\u0301hello World'] AS s
RETURN s, apoc.text.capitalizeAll(s) AS capitalized, apoc.text.decapitalizeAll(s) AS decapitalized;
UNWIND ['hello world','hELLO wORLD',' hello world','élan Über','ǳungla world','ßeta world','ﬃeld world','𐐨abc world'] AS s
RETURN s, apoc.text.capitalize(s) AS capitalized;
