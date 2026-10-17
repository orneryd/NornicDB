
WITH $payload AS payload, $keys AS keys
CALL (payload, keys) {
  UNWIND keys AS each
  WITH DISTINCT each AS thing
  RETURN count(*) AS things
}
RETURN things
