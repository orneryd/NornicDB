// Package fgac is the focused Neo4j Enterprise fine-grained access control
// (FGAC) comparison harness.
//
// The harness sends the same documented, normative authorization flows to a
// pinned Neo4j Enterprise reference and to a NornicDB candidate over Bolt. It
// uses synthetic roles, users, labels and properties only. It deliberately
// does not contain exploit, bypass, escalation or vulnerability-reproduction
// probes: each case asserts a normal documented outcome (a permitted result, a
// denied operation, invisible data, an introspection row) and the candidate
// must match the reference outcome.
//
// The comparison test is opt-in. Substrate tests (fixture, outcome, pin and
// script checks) run without a database and are the default result of
// `go test ./testing/cypher/fgac`.
package fgac
