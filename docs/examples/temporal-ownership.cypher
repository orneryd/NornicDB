// Run in an empty example database. These are the exact ownership-guide statements.
// Explicit epochs need no MVCC retention. Client-cutoff asOf calls are shown separately.

CREATE CONSTRAINT account_id IF NOT EXISTS
FOR (a:Account) REQUIRE a.id IS NODE KEY;

CREATE CONSTRAINT asset_id IF NOT EXISTS
FOR (a:Asset) REQUIRE a.id IS NODE KEY;

CREATE CONSTRAINT asset_ownership IF NOT EXISTS
FOR ()-[r:OWNS]-()
REQUIRE temporal.epoch(r.starts_at, r.ends_at)
GROUP BY r.assetId;

CREATE (alice:Account {id:'alice'}), (bob:Account {id:'bob'}),
       (asset:Asset {id:'asset-42'})
CREATE (alice)-[:OWNS {
  assetId:asset.id,
  starts_at:datetime('2026-01-01T00:00:00Z'), ends_at:null
}]->(asset);

MATCH (:Account {id:'alice'})-[old:OWNS]->(asset:Asset {id:'asset-42'}),
      (next:Account {id:'bob'})
WHERE old.ends_at IS NULL
WITH old, asset, next, datetime('2026-02-01T00:00:00Z') AS boundary
SET old.ends_at=boundary
CREATE (next)-[:OWNS {
  assetId:asset.id, starts_at:boundary, ends_at:null
}]->(asset);

MATCH (owner:Account)-[r:OWNS]->(:Asset {id:'asset-42'})
WHERE (r.starts_at IS NULL OR r.starts_at <= datetime('2026-01-15T00:00:00Z'))
  AND (r.ends_at IS NULL OR datetime('2026-01-15T00:00:00Z') < r.ends_at)
RETURN owner.id AS owner;

MATCH (alice:Account {id:'alice'})
CREATE (asset:Asset {id:'legacy-asset'})
CREATE (alice)-[:OWNS {
  assetId:asset.id, starts_at:null,
  ends_at:datetime('2026-02-01T00:00:00Z')
}]->(asset);

MATCH (owner:Account)-[r:OWNS]->(:Asset {id:'legacy-asset'})
WHERE (r.starts_at IS NULL OR r.starts_at <= datetime('2020-01-01T00:00:00Z'))
  AND (r.ends_at IS NULL OR datetime('2020-01-01T00:00:00Z') < r.ends_at)
RETURN owner.id AS owner;
