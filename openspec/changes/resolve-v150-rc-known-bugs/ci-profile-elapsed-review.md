# PROFILE comparison of execution-specific elapsed time

CI run 43216465878 at cc076b28e found one failure in the new runMany read
observation test: separate ordinary and observed executions returned 0 and 3
milliseconds in the row=-1 statistics summary. All data and other counters
matched. Equality of measured durations from separate executions is not a
product contract; this was an overly strict test added in this branch.

The common public-path test helper now accepts a call-site-specific row
normalizer, keeping exact equality for every other caller. Only runMany uses it.
On every ordinary, collector and PROFILE execution, its summary time must be an
int64 and nonnegative. Only that field of the row=-1 summary is omitted from
cross-execution equality. All other summary fields and all data rows remain
compared. The test now returns caller data named time=42 as an additional guard.

The normalizer clones the summary map and row rather than mutating results.
Controls prove that changing caller data named time or a semantic summary
counter still changes the comparison; changing only elapsed time does not.
The default summary remains present. Reader observations still require exactly
two DB hits, independent of the three emitted data/summary rows.

Periodic iterate/rock_n_roll currently return constant timeTaken=0, and periodic
commit returns constant runtime=0. Their complete rows remain compared as before;
this correction changes no production timing behavior or future metric policy.

Twenty final repetitions, including existing temporal and runMany statistics
controls, pass on both parsers in 2.882 seconds. A preceding three-repeat race
run of the comparison and affected helper consumers passes in 3.651 seconds.
The read fixture explicitly orders its data rows. The failure
is retained in /tmp/v150-ci-watch-cc076-cypher-failure.txt; repeated control output
is /tmp/v150-runmany-profile-duration-controls.log. Full current-head CI must
repeat after this test-only correction. Runtime and benchmark source are unchanged.
