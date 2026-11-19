# Preserve grouped aggregate semantics in the documented optimizer API

NOR-70's original ordinary Execute shapes are already correct. The retained supported DetectQueryPattern + ExecuteOptimized shape is documented to return p.name, avg(r.rating), count(r), but currently loses null-only groups and count values. The exact two-file public/API/native-transaction matrix fails on the current release base.

Investigate canonical streaming aggregate state/projection reuse rather than another independent float-only reducer. Keep per-projection values, grouping, null semantics, result column width and ORDER BY intact. Do not bypass the failing supported contract by changing handled assertions or force unsupported PatternInfo. Existing general/native controls remain required. No production edit has yet been made.
