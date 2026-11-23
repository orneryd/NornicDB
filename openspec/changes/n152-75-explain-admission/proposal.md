# N152-75 / NOR-335: EXPLAIN command admission

Base f0f5a2f4fd2bf8cb8a9344af9923cb0f4efb5210, branch
fix/n152-75-explain-admission. The previous staged proposal at
/private/tmp/n152-86-history.8nOnDGob/explain is a read-only source reference;
its branch and files are not modified or treated as verified implementation.

Scope: SHOW PROCEDURES/FUNCTIONS headers and invalid YIELD/RETURN diagnostics,
and malformed CREATE INDEX syntax under EXPLAIN, including real authenticated
HTTP and Bolt. Preserve ordinary SHOW, valid DDL empty-header/no-effect controls,
PROFILE execution/counters/index applicability and privilege state. No SHOW
ROLES/SERVERS/USER PRIVILEGES feature expansion, broad accounting work, general
DDL redesign or historical inventory replay.

Original four Go source blocks recovered exactly from the Plane ticket: seven
native owners (four command, three both-parser PROFILE/index controls), updated
HTTP metadata/grant controls and actual Bolt metadata controls. All source and
historical logs are losslessly retained in fixtures/original-source-blocks.json.
Historical Mac/Linux/Neo4j evidence remains historical, not a current check.

Fresh native original seven-owner reproduction on f0f5: FAIL 1.134s, same two
owners (SHOW header and invalid command); five control owners pass. Log:
/tmp/n152-75-native-red.log. No production edits preceded this run. HTTP FAIL 2.319s (grant-state control passes), Bolt FAIL 0.879s; their exact original tests reproduce the same metadata/invalid-index defects. Logs: /tmp/n152-75-http-red.log and /tmp/n152-75-bolt-red.log.

Parent-approved ownership correction:

1. Reuse showResult for PROCEDURES/FUNCTIONS, replacing runtime duplicated column
   arrays with declarations also consumed by showCommandResult. Existing tail
   projection/undefined-variable owners then supply errors and output columns.
   Only these two listings move from intentional undeclared tests into declared
   cases; other undeclared listings remain unchanged.
2. Extract only executeCreateIndex's syntax prefix: head, modern/legacy parse,
   VECTOR target constraints. Return parsedCreateIndexDDL plus vector/legacy
   flags, without the archived proposal's unused prefix field. Runtime consumes
   that result; schema metadata invokes the same syntax owner for the INDEX,
   RANGE and VECTOR heads already handled by executeCreateIndex. OPTIONS value
   binding, schema admission, vector publication, transaction effects and actual
   mutations remain runtime-owned. FULLTEXT/TEXT/POINT/LOOKUP and DROP retain their
   existing owners; no second parser or metadata-only syntax exception.

Actual source trace: projectionCompilation.compileBound currently declares empty
schema columns for index DDL without the syntax check it already performs for
constraints. executeCreateIndex owns the syntax prefix in index_creation.go;
parseCreateIndexDDL and parseCreateIndexLegacyDDL live in schema.go and parse
OPTIONS syntax without evaluating caller values. executeSchemaCommand in schema.go
selects the specific effect owner. SHOW declarations live in
show_result_declaration.go, runtime procedures/functions listings in executor_show.go.

DeepSeek read-only job 7cda015523a34973aba5e668da40d0e5 corroborated the missing
index metadata admission and recommended shared extraction. The approved guessed
index_ddl_parse.go path was absent; worker obeyed scope and did not broaden. Parent
independently found/read schema.go. Worker graph retrieval was incomplete (DNS,
no code baseline). No worker edits/tests. Its larger prepared-result field list
and incorrect claim that explicit RANGE retains its error prefix were rejected:
the existing double-parse failure resets non-vector errors to CREATE INDEX and
must remain unchanged. Architecture, integration and validation remain parent-owned.

Fresh Neo4j 5.26.30 reference completed: 19 exact requested queries independently
compared to result records in fixtures/fresh-neo4j-5.26.30.json. Procedure/function
headers (default, star, projected), INDEXES/USERS and valid INDEX/RANGE/VECTOR
EXPLAIN accept; invalid YIELD/RETURN, empty/missing ON and multi-property VECTOR
reject. EXPLAIN with unbound OPTIONS parameter accepts; final SHOW proves no
ref_ index was created. These are fresh reference facts, separate from original
historical evidence. This does not claim exact diagnostic text equality, legacy
DDL Neo4j support or general command-family qualification. Mesh log confirms
CLEANUP_COMPLETE after owned container/volume/newly pulled image removal.

Implementation takes the archived narrow owner patch after independent review,
omits its unused prefix field, and adds public both-parser index-boundary tests:
changed/missing OPTIONS values across repeated EXPLAIN must never bind or retain
caller data; runtime invalid options still reject and valid options publish an
index. Invalid target errors stay identical across RUN/EXPLAIN/PROFILE and raw
header lookup. Relationship and node target metadata remains effect-free.
