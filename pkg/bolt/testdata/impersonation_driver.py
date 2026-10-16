"""Official Neo4j driver matrix; executed only by the opt-in Go integration test."""

import json
import sys

import neo4j
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError


def forbidden(action):
    try:
        action()
    except Neo4jError as error:
        assert error.code == "Neo.ClientError.Security.Forbidden", error
    else:
        raise AssertionError("operation unexpectedly succeeded")


def scalar(result):
    return result.single(strict=True)[0]


def run_scheme(address, scheme):
    uri = f"{scheme}://{address}"
    versions = set()
    with GraphDatabase.driver(
        uri, auth=("svc936", "password123"), max_connection_pool_size=1,
        connection_timeout=5, connection_acquisition_timeout=10,
        max_transaction_retry_time=0,
    ) as driver:
        def session(user=None, **options):
            return driver.session(database="nornic", impersonated_user=user, **options)

        # Consecutive sessions must reuse the physical connection and replace
        # the execution identity even after a failure or self-impersonation.
        for user in ["alice936", "writer936", "bob936", "svc936", None, "alice936"]:
            expected = user or "svc936"
            with session(user) as current:
                result = current.run(
                    "SHOW CURRENT USER YIELD user RETURN user",
                    phase=f"pool-{scheme}", expected=expected,
                )
                assert scalar(result) == expected
                versions.add(tuple(result.consume().server.protocol_version))

        with session("alice936") as current:
            assert current.execute_read(
                lambda tx: scalar(tx.run("SHOW CURRENT USER YIELD user RETURN user"))
            ) == "alice936"
            forbidden(lambda: current.execute_write(
                lambda tx: tx.run("CREATE (:DriverDenied)").consume()
            ))

        marker = f"driver-{scheme}"
        with session("writer936") as current:
            def write(tx):
                assert scalar(tx.run("SHOW CURRENT USER YIELD user RETURN user")) == "writer936"
                return scalar(tx.run("CREATE (:DriverProof {key: $key}) RETURN $key", key=marker))
            assert current.execute_write(write) == marker
            bookmarks = current.last_bookmarks()
            assert bookmarks.raw_values, "commit must return a bookmark"

        # The read uses the previous writer's bookmark, but only this target's
        # privileges. It must see the commit and still be unable to write.
        with session("alice936", bookmarks=bookmarks) as current:
            assert current.execute_read(lambda tx: scalar(tx.run(
                "MATCH (n:DriverProof) WHERE n.key = $key RETURN count(n)", key=marker
            ))) == 1
            forbidden(lambda: current.run("CREATE (:DriverDenied)").consume())

        with session("root936") as current:
            forbidden(lambda: current.run("CREATE (:DriverDenied)").consume())
        with session("walled936") as current:
            forbidden(lambda: current.run("MATCH (n) RETURN count(n)").consume())

        with session("alice936") as current:
            for procedure in ["db.retrieve", "db.rretrieve"]:
                result = current.run(f"CALL {procedure}({{query: 'alpha', limit: 5}})")
                assert len(list(result)) >= 1, procedure

        graph_name = f"topic-groups-{scheme}"
        with session("writer936") as current:
            projected = list(current.run(
                "MATCH (d:Document)-[:TOPIC_GROUPS]->(t:Topic) "
                "RETURN gds.graph.project($name, d, t) AS graph", name=graph_name
            ))
            assert projected
            assert scalar(current.run("RETURN gds.graph.exists($name)", name=graph_name)) is True
        with session("bob936") as current:
            assert scalar(current.run("RETURN gds.graph.exists($name)", name=graph_name)) is False
        with session() as current:
            assert scalar(current.run("RETURN gds.graph.exists($name)", name=graph_name)) is False
            assert scalar(current.run("MATCH (n:DriverDenied) RETURN count(n)")) == 0
            assert scalar(current.run("SHOW CURRENT USER YIELD user RETURN user")) == "svc936"
        with session("writer936") as current:
            current.run("CALL gds.graph.drop($name)", name=graph_name).consume()

    print(json.dumps({"scheme": scheme, "driver": neo4j.__version__,
                      "bolt_versions": sorted(versions), "matrix": "passed"}), flush=True)


assert neo4j.__version__ == "6.4.1", neo4j.__version__
for scheme in ["bolt", "neo4j"]:
    run_scheme(sys.argv[1], scheme)
