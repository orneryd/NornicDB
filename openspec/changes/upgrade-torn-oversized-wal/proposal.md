# Upgrade an interrupted oversized WAL tail

## Problem

The supported historical writer could leave an oversized embedding-audit frame
incomplete at shutdown. The migration scanner tolerates an interrupted ordinary
final frame but returns a wrapped EOF from the oversized reader as fatal. Report
section 5 reproduces a supported old store that cannot upgrade.

## Correction

Use the scanner's active-file torn-tail policy for wrapped short reads from its
oversized reader as well as ordinary payload/checksum/trailer/padding reads.
Keep a single short-read classifier. Preserve all complete-frame authentication
and sealed-segment rejection. Existing framing and storage migration versions
are unchanged. Do not reconstruct missing bytes or skip complete corruption.

## Proof

Generate the historical writer's frame shape and truncate at nine positions.
Before the correction, current release refuses the incomplete oversized tail.
After it, validate/upgrade/replay preserve the complete prefix; repeat conversion
is deterministic and reopening the conversion requires no further upgrade.
Complete corruption at tail and interior, and torn sealed records, still fail.

Historical lossy binary keys and near-limit conversion expansion are separate
report items. This correction makes no claim to resolve either.
