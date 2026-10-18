Rule 73 gate material for #1412 (parked for 1.5.0 on 2026-10-18).

- ANALYSIS.md: family inventory, cause, plan (self-attack results in the park handoff).
- zz_rule73_inventory_test.go: go/types inventory extractor. Copy into pkg/cypher (drop the build tag) and run
  `go test -run '^TestZZRule73Inventory$' -v ./pkg/cypher`; prints INV lines (S/G/R/V/P/L/C/E).
- graphify_reach.py: reachability / read-to-holder / duplicate report over a graphify code graph of pkg/cypher
  unioned with the typed E edges.
- zz_rule73_attack_test.go: self-attack matrix (over-limit / near-limit / pools zero per member).
  Copy into pkg/cypher, run `go test -race -run '^TestZZRule73Attack$' -v ./pkg/cypher` (R73_PART=1|2 splits it).
- inventory-7f42f0ffa.tsv, graphify-reach-7f42f0ffa.tsv: outputs at head 7f42f0ffa.
- attack-*.txt: raw probe job outputs.
