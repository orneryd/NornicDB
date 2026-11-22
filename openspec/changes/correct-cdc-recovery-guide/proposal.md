# Correct the CDC recovery guide (N152-109)

The release guide says managed restart replays the journal before capture options
load and therefore loses every replayed event. Current `DB.Open` loads persisted
capture modes before `RecoverBufferedCommitsAtOpen`; current entity commit records
carry the admitted capture decision. The guide also describes all identifiers
older than the retention floor as invalid, contradicting the reserved live earliest
marker documented immediately below it.

Correct only these descriptions. Preserve legacy records without capture metadata,
embedded large-record FULL/DIFF/off fallback limits, ordinary cursor expiration,
and the unfinished durable server-side consumer feature in the ticket. No runtime
behavior, API or retention contract changes.

Evidence owners: `pkg/nornicdb/db.go`,
`pkg/nornicdb/cdc_recovered_option_order_test.go`,
`pkg/storage/wal_entity_commit.go`, `pkg/storage/change_capture.go`, and
`pkg/storage/change_capture_decision_test.go`.
