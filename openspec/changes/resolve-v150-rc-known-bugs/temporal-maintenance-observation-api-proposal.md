# Temporal maintenance observation proposal: withdrawn from required scope

Pinned Neo4j 5.26.30 Community reports runtime SCHEMA / planner ADMINISTRATION
and `Total database accesses: ?` for PROFILE CREATE INDEX, CREATE CONSTRAINT,
its IF NOT EXISTS no-op, and DROP INDEX. It executes the changes but returns
an EXPLAIN-style schema plan without runtime database-hit statistics.

References: /tmp/v150-native-schema-reference.log and
/tmp/v150-native-schema-reference2.log, generated using the cached pinned image
neo4j@sha256:3388e05ee53c8313d01acdf33e63ad175af95a92226dc8551160564439ce2c8c.
The first run also encountered the expected index/constraint ownership conflict;
the second uses a separate label/property and completes all schema cases.
No dogfood volume or database was mounted or changed.

Consequently the proposed Go MarkNewTemporalConstraintsForEngine migration is
NOT required for the approved Neo4j-behavioral PROFILE correction. Its current
maintenance lifetime and signature remain unchanged. The experimental test
requiring seven label reads plus three body reads demanded a stronger contract
and is discarded, not reported as a reproduced release bug. An earlier external
stream spy also failed to intercept transaction-native streams and proves no
lost runtime observation.

Required boundary: native SCHEMA database-hit statistics remain explicitly
unobserved. A surrounding procedure must not turn that into a claimed zero.
Public native schema and nested-statement admission controls will qualify that
boundary; ordinary data-query acquisitions and custom provider callbacks retain
their actual existing observations. No new analyzer policy or context exemption
is authorized by this finding. Extended maintenance metrics can be considered
only as an explicitly requested feature.
