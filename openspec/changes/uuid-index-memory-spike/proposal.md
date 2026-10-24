# Million-UUID index memory spike (NOR-53)

## Why

Internal identity dictionaries and property indexes retain substantial memory at
large cardinalities. Establish the causal owner and measured representation costs
before changing runtime behavior. This is an investigation of the existing storage
owner, not a Cypher convergence language change.

## Scope and constraints

Run reproducible representation experiments and probes against release/v1.4.4.
Preserve exact IDs, arbitrary supported non-UUID strings, numeric identity,
transaction/publication behavior, sharding and every current disk key/value.
Prefer no migration. No runtime changes, parallel production implementation,
UUID-specific query dispatch or leaf patches are introduced by the spike.

## Deliverables

See [experiment report](../../../experiments/uuid-index-memory/README.md) for
source ownership, measured corpus results, tradeoffs and the proposed next gate.
All prototypes reside in an isolated standard-library experiment module. The
real-owner probe uses a Go build overlay, not a permanent storage test/code path.

## Decision boundary

The spike can establish representation memory savings. It cannot certify a
production replacement, unchanged end-to-end latency or persistence correctness
of unimplemented code. Review the evidence with Tim before implementation.
