# Report uncommitted HTTP cancellation

The public transaction response middleware currently reports a deadline before
commit admission but writes no response for other cancellation. HTTP then emits
empty 200 success even though no write was committed (RC report section 7).

Use one pre-admission failure response for both select outcomes and both context
termination classes. Preserve admitted-commit waiting and publish its real result.
Do not change storage or Bolt acceptance. Generic cancellation must not be
misidentified as proof that the client disconnected.

Verify the response owner, an authenticated loopback HTTP write and its absent
mutation, and an already-admitted commit's successful response. Test fixtures
must cancel/release retained handlers on failure as well as on success.
