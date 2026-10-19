#!/usr/bin/env python3
"""Protocol client for scripts/cypher_engine_ab.sh.

This intentionally uses the public Bolt and HTTP APIs.  It is not an
in-process query benchmark: the shell harness starts a real graph-node for
each route and this client records the protocol results it observes.
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import time
from math import ceil
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit


CORPUS = (
    {
        "name": "single_property_seek",
        "query": "MATCH (e:Entity) WHERE e.entity_id = $id "
        "RETURN e.entity_id AS id, e.rank AS rank ORDER BY id ASC",
        "parameters": {"id": "alpha"},
        "columns": ["id", "rank"],
        "expected": [["alpha", 2]],
    },
    {
        "name": "or_property_multiseek",
        "query": "MATCH (e:Entity) WHERE e.entity_id = $left OR e.entity_id = $right "
        "RETURN e.entity_id AS id, e.rank AS rank ORDER BY rank DESC, id ASC",
        "parameters": {"left": "alpha", "right": "beta"},
        "columns": ["id", "rank"],
        "expected": [["beta", 3], ["alpha", 2]],
    },
    {
        "name": "one_hop_expand",
        "query": "MATCH (a:Entity)-[:KNOWS]->(b:Person) WHERE a.entity_id = $id "
        "RETURN b.name AS name",
        "parameters": {"id": "alpha"},
        "columns": ["name"],
        "expected": [["Bea"]],
    },
    {
        "name": "ordered_window",
        "query": "MATCH (e:Entity) WHERE e.entity_id STARTS WITH '' "
        "RETURN e.entity_id AS id ORDER BY e.entity_id DESC LIMIT 2",
        "parameters": {},
        "columns": ["id"],
        "expected": [["gamma"], ["beta"]],
    },
)

EXPLAIN_QUERY = "EXPLAIN " + CORPUS[1]["query"]
EXPLAIN_OPERATORS = (
    "VertexPropertyMultiSeek",
    "FilterExec",
    "SortExec",
    "ProjectExec",
)


def canonical_value(value: Any) -> Any:
    """Turn Bolt and HTTP scalar encodings into the same stable JSON shape."""
    if value is None:
        return {"type": "null", "value": None}
    if isinstance(value, bool):
        return {"type": "boolean", "value": value}
    if isinstance(value, int):
        return {"type": "integer", "value": value}
    if isinstance(value, float):
        return {"type": "float", "value": value}
    if isinstance(value, str):
        return {"type": "string", "value": value}
    if isinstance(value, list):
        return {"type": "list", "value": [canonical_value(item) for item in value]}
    raise TypeError(f"unsupported protocol value {value!r}")


def canonical_http_value(value: dict[str, Any]) -> Any:
    kind = value["type"]
    payload = value.get("value")
    if kind in {"integer", "signed_integer", "float", "boolean", "string", "null"}:
        # The sample corpus does not depend on the transport's signed/unsigned
        # representation.  Preserve a single mathematical integer type so
        # Bolt integers and HTTP signed_integer values compare honestly.
        return canonical_value(payload)
    if kind == "list":
        return {"type": "list", "value": [canonical_http_value(item) for item in payload]}
    return {"type": kind, "value": payload}


def error_value(error: BaseException) -> dict[str, Any]:
    return {
        "class": type(error).__name__,
        "code": getattr(error, "code", None),
        "message": str(error),
    }


def protocol_result(
    columns: list[str], rows: list[list[Any]], error: Any, read_epoch: int | None,
    read_epoch_exposed: bool, started: int,
) -> dict[str, Any]:
    return {
        "columns": columns,
        "rows": rows,
        "error": error,
        "read_epoch": read_epoch,
        "read_epoch_exposed": read_epoch_exposed,
        "elapsed_ms": (time.perf_counter_ns() - started) / 1_000_000,
    }


class BoltClient:
    """One verified Driver/session for all recorded Bolt samples in one route."""

    def __init__(self, endpoint: str, token: str) -> None:
        self.endpoint, self.token = endpoint, token
        self.driver: Any = None
        self.session: Any = None

    def __enter__(self) -> "BoltClient":
        # --self-test intentionally needs no Neo4j driver installed.
        from neo4j import GraphDatabase  # type: ignore[import-not-found]

        self.driver = GraphDatabase.driver(self.endpoint, auth=("neo4j", self.token))
        self.driver.verify_connectivity()
        self.session = self.driver.session(database="default")
        return self

    def __exit__(self, *_: Any) -> None:
        if self.session is not None:
            self.session.close()
        if self.driver is not None:
            self.driver.close()

    def request(self, query: str, parameters: dict[str, Any]) -> dict[str, Any]:
        started = time.perf_counter_ns()
        try:
            result = self.session.run(query, parameters)
            columns = list(result.keys())
            rows = [[canonical_value(value) for value in record.values()] for record in result]
            result.consume()
            error = None
        except Exception as exc:  # public protocol failures are comparison output
            columns, rows, error = [], [], error_value(exc)
        return protocol_result(columns, rows, error, None, False, started)


class ProtocolHttpError(Exception):
    def __init__(self, detail: dict[str, Any]) -> None:
        self.detail = detail
        super().__init__(detail.get("message", "HTTP query failed"))


class HttpClient:
    """One keep-alive HTTP connection for all recorded samples in one route."""

    def __init__(self, endpoint: str, token: str) -> None:
        parsed = urlsplit(endpoint)
        assert parsed.scheme == "http" and parsed.hostname, f"unsupported HTTP endpoint {endpoint}"
        self.path_prefix = parsed.path.rstrip("/")
        self.connection = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=30)
        self.token = token

    def __enter__(self) -> "HttpClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.connection.close()

    def request(self, query: str, parameters: dict[str, Any]) -> dict[str, Any]:
        body = json.dumps({"cell_id": "cell-0", "query": query, "parameters": parameters}).encode()
        started = time.perf_counter_ns()
        try:
            self.connection.request(
                "POST", self.path_prefix + "/v1/graphs/default/query", body=body,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "X-Graph-Namespace": "cypher-ab",
                    "Content-Type": "application/json",
                    "Connection": "keep-alive",
                },
            )
            response = self.connection.getresponse()
            payload = json.loads(response.read())
            if response.status >= 400:
                raise ProtocolHttpError(payload.get("error", {}))
            columns = payload["columns"]
            rows = [[canonical_http_value(value) for value in row] for row in payload["rows"]]
            if payload["next_cursor"] is not None:
                raise AssertionError("A/B corpus unexpectedly returned a paged HTTP result")
            error, read_epoch = None, payload["read_epoch"]
        except ProtocolHttpError as exc:
            columns, rows, read_epoch, error = [], [], None, exc.detail
        except Exception as exc:  # public protocol failures are comparison output
            columns, rows, read_epoch, error = [], [], None, error_value(exc)
        return protocol_result(columns, rows, error, read_epoch, True, started)


def percentile(samples: list[float], quantile: float) -> float:
    return sorted(samples)[ceil(quantile * len(samples)) - 1]


def stable_result(
    request: Callable[[str, dict[str, Any]], dict[str, Any]], case: dict[str, Any], iterations: int,
) -> dict[str, Any]:
    samples = [request(case["query"], case["parameters"]) for _ in range(iterations)]
    first = samples[0]
    semantic = (first["columns"], first["rows"], first["error"])
    for sample in samples[1:]:
        assert (sample["columns"], sample["rows"], sample["error"]) == semantic, (
            f"{case['name']}: result changed between benchmark iterations"
        )
    if first["error"] is not None:
        raise AssertionError(f"{case['name']}: protocol error {first['error']}")
    assert first["columns"] == case["columns"], (case["name"], first["columns"], case["columns"])
    expected = [[canonical_value(value) for value in row] for row in case["expected"]]
    assert first["rows"] == expected, (case["name"], first["rows"], expected)
    if first["read_epoch_exposed"]:
        assert first["read_epoch"] is not None, f"{case['name']}: HTTP omitted read_epoch"
    return {
        "case": case["name"],
        "columns": first["columns"],
        "rows": first["rows"],
        "error": first["error"],
        "read_epoch": first["read_epoch"],
        "read_epoch_exposed": first["read_epoch_exposed"],
        "iterations": iterations,
        "elapsed_ms_samples": [sample["elapsed_ms"] for sample in samples],
        "elapsed_ms_total": sum(sample["elapsed_ms"] for sample in samples),
        "elapsed_ms_mean": sum(sample["elapsed_ms"] for sample in samples) / iterations,
        "elapsed_ms_p50": percentile([sample["elapsed_ms"] for sample in samples], 0.50),
        "elapsed_ms_p95": percentile([sample["elapsed_ms"] for sample in samples], 0.95),
    }


def run(mode: str, bolt: str, http: str, token: str, iterations: int, warmup: bool) -> dict[str, Any]:
    transports = (("bolt", BoltClient(bolt, token)), ("http", HttpClient(http, token)))
    results = []
    for transport, client in transports:
        with client:
            if warmup:
                # One unrecorded request warms the connection/runtime before timed samples.
                warm = client.request(CORPUS[0]["query"], CORPUS[0]["parameters"])
                assert warm["error"] is None, (transport, "warmup", warm["error"])
            for case in CORPUS:
                result = stable_result(client.request, case, iterations)
                result["transport"] = transport
                results.append(result)
    return {"mode": mode, "iterations": iterations, "warmup": warmup, "results": results}


def seed(bolt: str, token: str) -> None:
    from neo4j import GraphDatabase  # type: ignore[import-not-found]

    # Seeding is intentionally Bolt-only and is performed only while the legacy
    # node is running.  The experimental node is never sent a mutation.
    queries = (
        "CREATE (a:Entity {id: 1, entity_id: 'alpha', rank: 2})-[:KNOWS]->"
        "(b:Person {id: 2, name: 'Bea'})",
        "CREATE (a:Entity {id: 3, entity_id: 'beta', rank: 3})-[:KNOWS]->"
        "(b:Person {id: 5, name: 'Bo'})",
        "CREATE (a:Entity {id: 4, entity_id: 'gamma', rank: 1})-[:KNOWS]->"
        "(b:Person {id: 6, name: 'Gia'})",
    )
    with GraphDatabase.driver(bolt, auth=("neo4j", token)) as driver:
        driver.verify_connectivity()
        with driver.session(database="default") as session:
            for query in queries:
                session.run(query).consume()


def explain(bolt: str, http: str, token: str) -> dict[str, Any]:
    output: dict[str, Any] = {"query": EXPLAIN_QUERY, "plans": {}}
    for transport, client in (("bolt", BoltClient(bolt, token)), ("http", HttpClient(http, token))):
        with client:
            result = client.request(EXPLAIN_QUERY, CORPUS[1]["parameters"])
            assert result["error"] is None, (transport, result["error"])
            assert result["columns"] == ["plan"], (transport, result["columns"])
            assert len(result["rows"]) == 1 and len(result["rows"][0]) == 1, result["rows"]
            plan = result["rows"][0][0]
            assert plan["type"] == "string", plan
            missing = [operator for operator in EXPLAIN_OPERATORS if operator not in plan["value"]]
            assert not missing, (transport, missing, plan["value"])
            output["plans"][transport] = plan["value"]
    return output


def index_results(document: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    return {(result["transport"], result["case"]): result for result in document["results"]}


def assert_transport_parity(document: dict[str, Any]) -> None:
    indexed = index_results(document)
    for case in CORPUS:
        bolt = indexed[("bolt", case["name"])]
        http = indexed[("http", case["name"])]
        assert (bolt["columns"], bolt["rows"], bolt["error"]) == (
            http["columns"], http["rows"], http["error"]
        ), (document["mode"], case["name"], bolt, http)
        assert bolt["columns"] == case["columns"], (document["mode"], case["name"], bolt["columns"])


def compare_documents(legacy: dict[str, Any], experimental: dict[str, Any]) -> dict[str, Any]:
    assert_transport_parity(legacy)
    assert_transport_parity(experimental)
    legacy_by_key = index_results(legacy)
    experimental_by_key = index_results(experimental)
    assert legacy_by_key.keys() == experimental_by_key.keys(), "A/B corpus keys differ"
    for key in legacy_by_key:
        before, after = legacy_by_key[key], experimental_by_key[key]
        assert (
            before["columns"], before["rows"], before["error"]
        ) == (
            after["columns"], after["rows"], after["error"]
        ), (key, before, after)
        if key[0] == "http":
            assert before["read_epoch"] == after["read_epoch"], (
                f"{key}: immutable store returned different HTTP read epochs",
                before["read_epoch"],
                after["read_epoch"],
            )
    # Bolt's result protocol exposes records and summaries, but not HydraDB's
    # read epoch.  HTTP's envelope does.  Keeping that distinction explicit is
    # more accurate than fabricating a Bolt epoch from a bookmark.
    return {
        "status": "pass",
        "semantic_assertions": "fixed columns, typed rows/order, and errors match within each transport and across routes",
        "read_epoch": "HTTP epochs match across routes; Bolt read_epoch is unavailable in Bolt result records",
    }


def compare(legacy_path: Path, experimental_path: Path) -> dict[str, Any]:
    return compare_documents(json.loads(legacy_path.read_text()), json.loads(experimental_path.read_text()))


def self_test() -> None:
    assert canonical_value("alpha") == canonical_http_value({"type": "string", "value": "alpha"})
    assert canonical_value(2) == canonical_http_value({"type": "signed_integer", "value": 2})
    assert [case["name"] for case in CORPUS] == [
        "single_property_seek",
        "or_property_multiseek",
        "one_hop_expand",
        "ordered_window",
    ]
    def document(mode: str) -> dict[str, Any]:
        results = []
        for transport in ("bolt", "http"):
            for case in CORPUS:
                results.append({
                    "transport": transport, "case": case["name"], "columns": case["columns"],
                    "rows": [[canonical_value(value) for value in row] for row in case["expected"]],
                    "error": None, "read_epoch": 7 if transport == "http" else None,
                })
        return {"mode": mode, "results": results}
    legacy = document("legacy")
    experimental = document("experimental")
    assert compare_documents(legacy, experimental)["status"] == "pass"
    experimental["results"][3]["columns"] = ["wrong"]
    try:
        compare_documents(legacy, experimental)
    except AssertionError:
        pass
    else:
        raise AssertionError("comparison accepted a Bolt/HTTP column mismatch")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("seed", "run", "explain", "compare", "self-test"))
    parser.add_argument("--bolt", default="bolt://127.0.0.1:17687")
    parser.add_argument("--http", default="http://127.0.0.1:18443")
    parser.add_argument("--token", default=os.environ.get("GRAPH_CYPHER_ENGINE_AB_TOKEN"))
    parser.add_argument("--mode", choices=("legacy", "experimental"))
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--warmup", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--legacy", type=Path)
    parser.add_argument("--experimental", type=Path)
    args = parser.parse_args()

    if args.command == "self-test":
        self_test()
        return
    if args.command == "compare":
        assert args.legacy and args.experimental and args.output
        args.output.write_text(json.dumps(compare(args.legacy, args.experimental), indent=2) + "\n")
        return
    assert args.token, "--token or GRAPH_CYPHER_ENGINE_AB_TOKEN is required"
    assert args.iterations > 0, "--iterations must be positive"
    if args.command == "seed":
        seed(args.bolt, args.token)
        return
    assert args.output and args.mode
    document = (
        explain(args.bolt, args.http, args.token)
        if args.command == "explain"
        else run(args.mode, args.bolt, args.http, args.token, args.iterations, args.warmup)
    )
    args.output.write_text(json.dumps(document, indent=2) + "\n")


if __name__ == "__main__":
    main()
