#!/usr/bin/env python3
"""API-16 — replay a recorded corpus of HTTP interactions against this build.

API-16: "a recorded corpus of requests/responses replayed against every build
to detect unintended contract changes". The point is not that the responses are
*correct* -- other suites check that -- but that they are the *same*. A field
renamed, a type changed from integer to string, an error that stops being an
error: each of those breaks every client and none of them fails a test that
only asks whether a query returns the right rows.

    python3 scripts/api_stability.py --record   # rewrite the corpus
    python3 scripts/api_stability.py --check    # fail on any difference

**Re-recording is the failure mode.** A golden-file check that is easy to
re-record stops being a check: the first diff is repaired by regenerating the
file, and the contract change ships with it. So `--check` prints the diff as a
before/after pair and says which field changed, and `--record` refuses to run
unless `--i-am-changing-the-contract` is passed as well. The corpus is meant
to be updated deliberately, in a commit whose message says what changed and
why.

**Volatile fields are normalised, and the list is short and written down.**
Normalising a field means the corpus no longer defends it, so each one needs a
reason:

  engine_version  changes on every release; a version bump is not a contract
                  change and would otherwise rewrite all 200 entries.
  plan_hash       changes whenever the planner changes, which is often and is
                  not an API change. Reduced to a presence-and-shape check, so
                  the *field* is still defended even though its value is not.
  cache           `/api/status` reports hit and miss counters that depend on
                  everything run before; the key is kept, the counts are not.

Nothing else is normalised. Node ids are left alone deliberately: they are
part of what a client sees, the fixture is built by a fixed sequence of
statements, and an id that starts moving is something we want to hear about.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BINARY = REPO / "target" / "release" / "samyama"
CORPUS = REPO / "tests" / "api_corpus" / "corpus.json"

#: Statements that build the graph every request is answered against. Fixed
#: order, so ids and counts are reproducible.
FIXTURE = [
    "CREATE (:Person {name: 'Alice', age: 30, city: 'Pune'})",
    "CREATE (:Person {name: 'Bob', age: 25, city: 'Delhi'})",
    "CREATE (:Person {name: 'Carol', age: 41})",
    "CREATE (:Person:Employee {name: 'Dan', age: 33, city: 'Pune'})",
    "CREATE (:Company {name: 'Acme', industry: 'tech'})",
    "CREATE (:Company {name: 'Globex', industry: 'finance'})",
    "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS {since: 2019}]->(b)",
    "MATCH (a:Person {name: 'Bob'}), (b:Person {name: 'Carol'}) CREATE (a)-[:KNOWS {since: 2021}]->(b)",
    "MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (p)-[:WORKS_AT {role: 'eng'}]->(c)",
    "MATCH (p:Person {name: 'Dan'}), (c:Company {name: 'Globex'}) CREATE (p)-[:WORKS_AT {role: 'ops'}]->(c)",
]

#: The requests the corpus records. Grouped so a reader can see what the corpus
#: does and does not defend.
def requests() -> list[dict]:
    reqs: list[dict] = []

    def q(name: str, cypher: str) -> None:
        reqs.append({"name": name, "method": "POST", "path": "/api/query",
                     "body": {"query": cypher}})

    # --- reads: shapes, types, ordering, aggregation ---------------------
    q("return_literal", "RETURN 1")
    q("return_float", "RETURN 1.5")
    q("return_string", "RETURN 'x'")
    q("return_bool", "RETURN true")
    q("return_null", "RETURN null")
    q("return_list", "RETURN [1, 2, 3]")
    q("return_map", "RETURN {a: 1, b: 'two'}")
    q("return_alias", "RETURN 1 AS one, 2 AS two")
    q("scan_all", "MATCH (n) RETURN count(n) AS n")
    q("scan_label", "MATCH (p:Person) RETURN count(p) AS n")
    q("node_shape", "MATCH (p:Person {name: 'Alice'}) RETURN p")
    q("multi_label_node", "MATCH (e:Employee) RETURN e")
    q("edge_shape", "MATCH ()-[r:KNOWS]->() RETURN r")
    q("path_shape", "MATCH p = (:Person {name: 'Alice'})-[:KNOWS]->() RETURN p")
    q("property_access", "MATCH (p:Person) RETURN p.name AS name ORDER BY name")
    q("missing_property", "MATCH (p:Person {name: 'Carol'}) RETURN p.city AS city")
    q("order_desc", "MATCH (p:Person) RETURN p.name AS name ORDER BY name DESC")
    q("skip_limit", "MATCH (p:Person) RETURN p.name AS name ORDER BY name SKIP 1 LIMIT 2")
    q("distinct", "MATCH (p:Person) RETURN DISTINCT p.city AS city ORDER BY city")
    q("where_numeric", "MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name ORDER BY name")
    q("where_string", "MATCH (p:Person) WHERE p.name STARTS WITH 'A' RETURN p.name AS name")
    q("where_null", "MATCH (p:Person) WHERE p.city IS NULL RETURN p.name AS name")
    q("where_in", "MATCH (p:Person) WHERE p.name IN ['Alice', 'Bob'] RETURN count(p) AS n")
    q("optional_match", "MATCH (p:Person) OPTIONAL MATCH (p)-[:WORKS_AT]->(c) "
                        "RETURN p.name AS name, c.name AS company ORDER BY name")
    q("aggregate_sum", "MATCH (p:Person) RETURN sum(p.age) AS total")
    q("aggregate_avg", "MATCH (p:Person) RETURN avg(p.age) AS mean")
    q("aggregate_minmax", "MATCH (p:Person) RETURN min(p.age) AS lo, max(p.age) AS hi")
    q("aggregate_collect", "MATCH (p:Person) RETURN collect(p.name) AS names")
    q("group_by", "MATCH (p:Person) RETURN p.city AS city, count(p) AS n ORDER BY city")
    q("with_chain", "MATCH (p:Person) WITH p.city AS city, count(p) AS n "
                    "WHERE n > 1 RETURN city, n")
    q("unwind", "UNWIND [1, 2, 3] AS i RETURN i * 2 AS doubled")
    q("union", "RETURN 1 AS x UNION RETURN 2 AS x")
    q("exists_subquery", "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->() } "
                         "RETURN p.name AS name ORDER BY name")
    q("var_length", "MATCH (:Person {name: 'Alice'})-[:KNOWS*1..2]->(o) "
                    "RETURN count(o) AS n")
    q("two_hop", "MATCH (:Person {name: 'Alice'})-[:KNOWS]->()-[:KNOWS]->(o) RETURN o.name AS name")
    q("labels_fn", "MATCH (e:Employee) RETURN labels(e) AS labels")
    q("type_fn", "MATCH ()-[r]->() RETURN DISTINCT type(r) AS t ORDER BY t")
    q("keys_fn", "MATCH (p:Person {name: 'Alice'}) RETURN keys(p) AS k")
    q("id_fn", "MATCH (p:Person {name: 'Alice'}) RETURN id(p) AS id")
    q("coalesce", "MATCH (p:Person {name: 'Carol'}) RETURN coalesce(p.city, 'unknown') AS city")
    q("case_expr", "MATCH (p:Person) RETURN CASE WHEN p.age > 30 THEN 'older' "
                   "ELSE 'younger' END AS band ORDER BY band")
    q("string_fns", "RETURN toUpper('ab') AS u, toLower('CD') AS l, "
                    "substring('hello', 1, 3) AS s, size('hello') AS n")
    q("numeric_fns", "RETURN abs(-2) AS a, ceil(1.2) AS c, floor(1.8) AS f, "
                     "round(1.5) AS r, sqrt(9.0) AS q, sign(-3) AS g")
    q("list_fns", "RETURN head([1,2,3]) AS h, last([1,2,3]) AS l, tail([1,2,3]) AS t")
    q("type_coercion", "RETURN toInteger('42') AS i, toFloat('1.5') AS f, toString(7) AS s")
    q("null_arithmetic", "RETURN 1 + null AS x")
    q("null_comparison", "RETURN 1 > null AS x")
    q("explain", "EXPLAIN MATCH (p:Person) RETURN p")

    q("list_slice", "RETURN [1,2,3,4,5][1..3] AS s")
    q("list_index", "RETURN [1,2,3][0] AS first")
    q("list_comprehension", "RETURN [x IN [1,2,3] WHERE x > 1 | x * 10] AS xs")
    q("map_projection_keys", "RETURN keys({a: 1, b: 2}) AS k")
    q("string_split_join", "RETURN split('a,b,c', ',') AS parts")
    q("string_trim", "RETURN trim('  x  ') AS t, replace('abc', 'b', 'z') AS r")
    q("string_left_right", "RETURN left('hello', 2) AS l, right('hello', 2) AS r, reverse('abc') AS v")
    q("range_fn", "RETURN range(1, 5) AS r")
    q("size_of_list", "RETURN size([1,2,3]) AS n")
    q("exists_fn", "MATCH (p:Person {name: 'Alice'}) RETURN exists(p.city) AS has_city")
    q("boolean_ops", "RETURN true AND false AS a, true OR false AS o, NOT true AS n")
    q("comparison_chain", "RETURN 1 < 2 AS lt, 2 <= 2 AS le, 3 <> 4 AS ne")
    q("modulo_and_power", "RETURN 7 % 3 AS m, 2 ^ 3 AS p")
    q("integer_division", "RETURN 7 / 2 AS i, 7.0 / 2 AS f")
    q("date_literal", "RETURN date('2024-01-15') AS d")
    q("datetime_literal", "RETURN datetime('2024-01-15T10:30:00Z') AS dt")
    q("duration_literal", "RETURN duration({days: 3}) AS dur")
    q("date_component", "RETURN date('2024-01-15').year AS y")
    q("count_star", "MATCH (p:Person) RETURN count(*) AS n")
    q("count_distinct", "MATCH (p:Person) RETURN count(DISTINCT p.city) AS n")
    q("order_by_two_keys", "MATCH (p:Person) RETURN p.name AS name, p.age AS age "
                           "ORDER BY age DESC, name ASC")
    q("limit_zero", "MATCH (p:Person) RETURN p.name AS name LIMIT 0")
    q("empty_result", "MATCH (p:Person {name: 'Nobody'}) RETURN p")
    q("optional_match_no_hit", "OPTIONAL MATCH (p:Person {name: 'Nobody'}) RETURN p")
    q("anonymous_relationship", "MATCH (a)-[]->(b) RETURN count(*) AS n")
    q("undirected_pattern", "MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN count(*) AS n")
    q("edge_property_filter", "MATCH ()-[r:KNOWS]->() WHERE r.since > 2020 RETURN count(r) AS n")
    q("edge_property_projection", "MATCH ()-[r:WORKS_AT]->() RETURN r.role AS role ORDER BY role")
    q("with_order_limit", "MATCH (p:Person) WITH p ORDER BY p.age DESC LIMIT 2 RETURN p.name AS name")
    q("nested_aggregate", "MATCH (p:Person) WITH count(p) AS n RETURN n * 2 AS doubled")

    # --- procedures -------------------------------------------------------
    q("db_labels", "CALL db.labels() YIELD label RETURN label ORDER BY label")
    q("db_relationship_types", "CALL db.relationshipTypes() YIELD relationshipType "
                               "RETURN relationshipType ORDER BY relationshipType")
    q("db_property_keys", "CALL db.propertyKeys() YIELD propertyKey "
                          "RETURN propertyKey ORDER BY propertyKey")
    q("db_schema_visualization",
      "CALL db.schema.visualization() YIELD source_label, relationship_type, target_label "
      "RETURN source_label, relationship_type, target_label "
      "ORDER BY source_label, relationship_type, target_label")
    q("algo_pagerank", "CALL algo.pageRank({iterations: 3}) YIELD node, score "
                       "RETURN count(*) AS n")
    q("algo_wcc", "CALL algo.wcc() YIELD node, componentId RETURN count(*) AS n")
    q("algo_triangle_count", "CALL algo.triangleCount() YIELD node, triangles RETURN count(*) AS n")

    # --- writes, and what they return ------------------------------------
    #
    # At the end, because they change the fixture: everything above sees the
    # graph the FIXTURE built, and everything from here sees what the previous
    # write left. The order is the contract as much as the responses are.
    q("create_node", "CREATE (:Temp {n: 1})")
    q("create_node_returning", "CREATE (t:Temp2 {n: 2}) RETURN t")
    q("create_edge", "MATCH (a:Temp), (b:Temp2) CREATE (a)-[:TMP {w: 1}]->(b)")
    q("set_property", "MATCH (t:Temp) SET t.n = 99 RETURN t.n AS n")
    q("set_multiple", "MATCH (t:Temp) SET t.a = 1, t.b = 'x' RETURN t.a AS a, t.b AS b")
    q("remove_property", "MATCH (t:Temp) REMOVE t.a RETURN t.a AS a")
    q("set_label", "MATCH (t:Temp) SET t:Extra RETURN labels(t) AS labels")
    q("remove_label", "MATCH (t:Temp) REMOVE t:Extra RETURN labels(t) AS labels")
    q("merge_creates", "MERGE (m:Merged {k: 1}) RETURN m.k AS k")
    q("merge_matches", "MERGE (m:Merged {k: 1}) RETURN count(m) AS n")
    q("merge_on_create", "MERGE (m:Merged2 {k: 2}) ON CREATE SET m.made = true RETURN m.made AS made")
    q("delete_edge", "MATCH ()-[r:TMP]->() DELETE r")
    q("detach_delete", "MATCH (t:Temp2) DETACH DELETE t")
    q("delete_node", "MATCH (t:Temp) DELETE t")
    q("count_after_deletes", "MATCH (n:Temp) RETURN count(n) AS n")
    q("create_index", "CREATE INDEX ON :Person(name)")
    q("create_index_twice", "CREATE INDEX ON :Person(name)")
    q("show_indexes", "SHOW INDEXES")
    q("create_constraint", "CREATE CONSTRAINT ON (p:Person) ASSERT p.name IS UNIQUE")
    q("show_constraints", "SHOW CONSTRAINTS")

    # --- more of the read surface, after the writes above -----------------
    q("scan_after_writes", "MATCH (n) RETURN count(n) AS n")
    q("order_by_null_last", "MATCH (p:Person) RETURN p.city AS city ORDER BY city")
    # `ORDER BY` on every multi-row query in this corpus, including the ones
    # where it looks redundant. Cypher promises no order without it, and this
    # engine delivers on that promise: `MATCH (c:Company) RETURN c.name` came
    # back as Acme,Globex on one run and Globex,Acme on the next, because the
    # label index is a hash set. A corpus that recorded one of those orders
    # would fail at random, and a gate that fails at random gets switched off.
    q("limit_larger_than_result", "MATCH (c:Company) RETURN c.name AS name ORDER BY name LIMIT 100")
    q("skip_past_the_end", "MATCH (c:Company) RETURN c.name AS name SKIP 100")
    q("nested_map_literal", "RETURN {a: {b: [1, 2]}} AS m")
    q("nested_list_literal", "RETURN [[1, 2], [3]] AS l")
    q("empty_list", "RETURN [] AS l")
    q("empty_map", "RETURN {} AS m")
    q("negative_numbers", "RETURN -1 AS i, -1.5 AS f")
    q("large_integer", "RETURN 9223372036854775807 AS i")
    q("integer_overflow", "RETURN 9223372036854775807 + 1 AS i")
    q("division_by_zero", "RETURN 1 / 0 AS x")
    q("float_division_by_zero", "RETURN 1.0 / 0.0 AS x")
    q("modulo_by_zero", "RETURN 1 % 0 AS x")
    q("string_comparison", "RETURN 'a' < 'b' AS lt")
    q("list_equality", "RETURN [1, 2] = [1, 2] AS eq")
    q("map_equality", "RETURN {a: 1} = {a: 1} AS eq")
    q("null_equality", "RETURN null = null AS eq")
    q("null_is_null", "RETURN null IS NULL AS n")
    q("boolean_of_null", "RETURN NOT null AS n")
    q("coalesce_all_null", "RETURN coalesce(null, null) AS c")
    q("case_no_else", "RETURN CASE WHEN false THEN 1 END AS c")
    q("with_where_false", "MATCH (p:Person) WITH p WHERE false RETURN count(p) AS n")
    q("union_all_duplicates", "RETURN 1 AS x UNION ALL RETURN 1 AS x")  # one value, so order cannot vary
    q("union_distinct", "RETURN 1 AS x UNION RETURN 1 AS x")
    q("unwind_empty", "UNWIND [] AS i RETURN i")
    q("unwind_null", "UNWIND null AS i RETURN i")
    q("unwind_nested", "UNWIND [[1, 2]] AS pair UNWIND pair AS i RETURN i")
    q("optional_match_then_where", "MATCH (p:Person) OPTIONAL MATCH (p)-[:NOPE]->(x) RETURN count(x) AS n")
    q("variable_length_zero", "MATCH (a:Person)-[:KNOWS*0..1]->(b) RETURN count(b) AS n")
    q("variable_length_unbounded", "MATCH (a:Person {name: 'Alice'})-[:KNOWS*]->(b) RETURN count(b) AS n")
    q("shortest_path_procedure", "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Carol'}) "
                                 "CALL algo.shortestPath(id(a), id(b)) YIELD node RETURN count(*) AS n")
    q("pattern_in_where", "MATCH (p:Person) WHERE (p)-[:KNOWS]->() RETURN count(p) AS n")
    q("not_pattern_in_where", "MATCH (p:Person) WHERE NOT (p)-[:KNOWS]->() RETURN count(p) AS n")
    q("count_distinct_property", "MATCH (p:Person) RETURN count(DISTINCT p.age) AS n")
    q("collect_distinct",
      "MATCH (p:Person) WITH DISTINCT p.city AS city ORDER BY city RETURN collect(city) AS cities")
    q("aggregate_over_empty", "MATCH (n:Nothing) RETURN count(n) AS c, sum(n.x) AS s, avg(n.x) AS a")
    q("min_max_over_strings", "MATCH (p:Person) RETURN min(p.name) AS lo, max(p.name) AS hi")
    q("order_by_aggregate", "MATCH (p:Person) RETURN p.city AS city, count(p) AS n ORDER BY n DESC, city")
    q("with_skip_limit", "MATCH (p:Person) WITH p ORDER BY p.name SKIP 1 LIMIT 1 RETURN p.name AS name")
    q("multiple_with", "MATCH (p:Person) WITH p WITH count(p) AS n RETURN n")
    q("distinct_on_two_columns", "MATCH (p:Person) RETURN DISTINCT p.city AS city, p.age AS age ORDER BY city, age")
    q("string_concat_number", "RETURN 'n=' + 1 AS s")
    q("list_concat", "RETURN [1] + [2] AS l")
    q("in_empty_list", "RETURN 1 IN [] AS x")
    q("in_with_null", "RETURN 1 IN [null] AS x")
    q("regex_match", "MATCH (p:Person) WHERE p.name =~ 'A.*' RETURN count(p) AS n")
    q("starts_ends_contains", "RETURN 'hello' STARTS WITH 'he' AS s, 'hello' ENDS WITH 'lo' AS e, "
                              "'hello' CONTAINS 'ell' AS c")
    q("explain_join", "EXPLAIN MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b")
    # PROFILE is deliberately absent. Its body carries wall-clock timings, so it
    # differs on every run -- correctly, and a profile that did not vary would be
    # the suspicious one. Recording it would make this gate fail at random.
    # EXPLAIN above covers the plan text, which is what the contract is about.

    # --- algorithms, which clients call by name and branch on the shape of -
    q("algo_pagerank_named", "CALL algo.pageRank({iterations: 2}) YIELD node, score RETURN count(*) AS n")
    q("algo_scc", "CALL algo.scc() YIELD node, componentId RETURN count(*) AS n")
    q("algo_cdlp", "CALL algo.cdlp() YIELD node, communityId RETURN count(*) AS n")
    q("algo_lcc", "CALL algo.lcc() YIELD node, coefficient RETURN count(*) AS n")
    q("algo_degree", "CALL algo.degreeCentrality() YIELD node, score RETURN count(*) AS n")
    q("algo_mst", "CALL algo.mst() YIELD source, target RETURN count(*) AS n")
    q("algo_bare_name", "CALL pageRank({iterations: 1}) YIELD node RETURN count(*) AS n")
    q("algo_gds_stream", "CALL gds.pageRank.stream({iterations: 1}) YIELD node RETURN count(*) AS n")
    q("db_labels_after_writes", "CALL db.labels() YIELD label RETURN label ORDER BY label")
    q("db_schema_for_llm", "CALL db.schema.forLLM(2000) YIELD complete RETURN complete")

    # --- functions, one response shape each --------------------------------
    q("fn_id_and_labels", "MATCH (p:Person) WITH p ORDER BY p.name LIMIT 1 RETURN id(p) AS id, labels(p) AS l")
    q("fn_properties", "MATCH (p:Person) WITH p ORDER BY p.name LIMIT 1 RETURN properties(p) AS props")
    q("fn_nodes_and_rels", "MATCH path = (:Person {name: 'Alice'})-[:KNOWS]->() "
                           "RETURN size(nodes(path)) AS n, size(relationships(path)) AS r")
    q("fn_length_of_path", "MATCH path = (:Person {name: 'Alice'})-[:KNOWS]->() RETURN length(path) AS l")
    q("fn_startnode_endnode", "MATCH ()-[r:KNOWS]->() WITH r LIMIT 1 "
                              "RETURN startNode(r).name AS s, endNode(r).name AS e")
    q("fn_type_of_rel", "MATCH ()-[r]->() WITH r LIMIT 1 RETURN type(r) AS t")
    q("fn_exists_on_missing", "MATCH (p:Person) RETURN exists(p.nope) AS e ORDER BY e LIMIT 1")
    q("fn_timestamp_shape", "RETURN timestamp() > 0 AS positive")
    q("fn_randomuuid_shape", "RETURN size(randomUUID()) AS len")
    q("fn_rand_range", "RETURN rand() >= 0.0 AND rand() < 1.0 AS in_range")
    q("fn_tostring_of_types", "RETURN toString(1) AS i, toString(1.5) AS f, toString(true) AS b")
    q("fn_tointeger_of_bad", "RETURN toInteger('nope') AS i")
    q("fn_tofloat_of_bad", "RETURN toFloat('nope') AS f")
    q("fn_trim_family", "RETURN trim('  a  ') AS t, ltrim('  a') AS l, rtrim('a  ') AS r")
    q("fn_isempty", "RETURN isEmpty('') AS s, isEmpty([]) AS l")
    q("fn_math", "RETURN pi() > 3 AS p, e() > 2 AS e, exp(0) AS x, log(1) AS lg")
    q("fn_trig", "RETURN sin(0) AS s, cos(0) AS c, tan(0) AS t")
    q("fn_degrees_radians", "RETURN degrees(0) AS d, radians(0) AS r")
    q("fn_percentile", "MATCH (p:Person) RETURN percentileCont(p.age, 0.5) AS median")
    q("fn_stdev", "MATCH (p:Person) RETURN stDev(p.age) AS sd")

    # --- more error classes ------------------------------------------------
    q("error_divide_string", "RETURN 'a' / 2")
    q("error_unknown_label_function", "MATCH (n) RETURN nosuchfn(n)")
    q("error_bad_parameter_count", "RETURN toUpper()")
    q("error_negative_limit", "MATCH (n) RETURN n LIMIT -1")
    q("error_negative_skip", "MATCH (n) RETURN n SKIP -1")
    q("error_order_by_unknown", "MATCH (p:Person) RETURN p.name AS name ORDER BY nope")
    q("error_merge_without_pattern", "MERGE 1")
    q("error_return_star_no_scope", "RETURN *")
    q("error_unclosed_string", "RETURN 'abc")
    q("error_unbalanced_brackets", "RETURN [1, 2")
    q("error_create_index_twice_constraint", "CREATE CONSTRAINT ON (p:Nope) ASSERT p.x IS UNIQUE; CREATE CONSTRAINT ON (p:Nope) ASSERT p.x IS UNIQUE")
    q("error_vector_index_bad_dimensions", "CREATE VECTOR INDEX bad FOR (n:V) ON (n.e) OPTIONS {dimensions: -1}")

    # --- algorithms, which clients call by name and branch on the shape of -
    q("algo_pagerank_named", "CALL algo.pageRank({iterations: 2}) YIELD node, score RETURN count(*) AS n")
    q("algo_scc", "CALL algo.scc() YIELD node, componentId RETURN count(*) AS n")
    q("algo_cdlp", "CALL algo.cdlp() YIELD node, communityId RETURN count(*) AS n")
    q("algo_lcc", "CALL algo.lcc() YIELD node, coefficient RETURN count(*) AS n")
    q("algo_degree", "CALL algo.degreeCentrality() YIELD node, score RETURN count(*) AS n")
    q("algo_mst", "CALL algo.mst() YIELD source, target RETURN count(*) AS n")
    q("algo_bare_name", "CALL pageRank({iterations: 1}) YIELD node RETURN count(*) AS n")
    q("algo_gds_stream", "CALL gds.pageRank.stream({iterations: 1}) YIELD node RETURN count(*) AS n")
    q("db_labels_after_writes", "CALL db.labels() YIELD label RETURN label ORDER BY label")

    # --- functions, one response shape each --------------------------------
    q("fn_id_and_labels", "MATCH (p:Person) WITH p ORDER BY p.name LIMIT 1 RETURN id(p) AS id, labels(p) AS l")
    q("fn_properties", "MATCH (p:Person) WITH p ORDER BY p.name LIMIT 1 RETURN properties(p) AS props")
    q("fn_nodes_and_rels", "MATCH path = (:Person {name: 'Alice'})-[:KNOWS]->() "
                           "RETURN size(nodes(path)) AS n, size(relationships(path)) AS r")
    q("fn_length_of_path", "MATCH path = (:Person {name: 'Alice'})-[:KNOWS]->() RETURN length(path) AS l")
    q("fn_startnode_endnode", "MATCH ()-[r:KNOWS]->() WITH r ORDER BY id(r) LIMIT 1 "
                              "RETURN startNode(r).name AS s, endNode(r).name AS e")
    q("fn_type_of_rel", "MATCH ()-[r]->() WITH r ORDER BY id(r) LIMIT 1 RETURN type(r) AS t")
    q("fn_timestamp_shape", "RETURN timestamp() > 0 AS positive")
    q("fn_randomuuid_shape", "RETURN size(randomUUID()) AS len")
    q("fn_rand_range", "RETURN rand() >= 0.0 AND rand() < 1.0 AS in_range")
    q("fn_tostring_of_types", "RETURN toString(1) AS i, toString(1.5) AS f, toString(true) AS b")
    q("fn_tointeger_of_bad", "RETURN toInteger('nope') AS i")
    q("fn_tofloat_of_bad", "RETURN toFloat('nope') AS f")
    q("fn_trim_family", "RETURN trim('  a  ') AS t, ltrim('  a') AS l, rtrim('a  ') AS r")
    q("fn_isempty", "RETURN isEmpty('') AS s, isEmpty([]) AS l")
    q("fn_math", "RETURN pi() > 3 AS p, e() > 2 AS e, exp(0) AS x, log(1) AS lg")
    q("fn_trig", "RETURN sin(0) AS s, cos(0) AS c, tan(0) AS t")
    q("fn_degrees_radians", "RETURN degrees(0) AS d, radians(0) AS r")
    q("fn_percentile", "MATCH (p:Person) RETURN percentileCont(p.age, 0.5) AS median")
    q("fn_stdev", "MATCH (p:Person) RETURN stDev(p.age) AS sd")

    # --- too few arguments, which used to abort the process (#1365) --------
    #
    # Nine of them, because the fix is a table and a table can lose a row. If
    # one of these ever answers with anything other than an ArgumentError, the
    # corpus says so on the pull request that did it.
    q("arity_toupper_none", "RETURN toUpper()")
    q("arity_abs_none", "RETURN abs()")
    q("arity_size_none", "RETURN size()")
    q("arity_atan2_one", "RETURN atan2(1)")
    q("arity_haslabels_one", "MATCH (p:Person) RETURN hasLabels(p)")
    q("arity_tostring_none", "RETURN toString()")
    q("arity_id_none", "RETURN id()")
    q("arity_keys_none", "RETURN keys()")
    q("arity_type_none", "RETURN type()")

    # --- more error classes ------------------------------------------------
    q("error_divide_string", "RETURN 'a' / 2")
    q("error_unknown_label_function", "MATCH (n) RETURN nosuchfn(n)")
    q("error_negative_limit", "MATCH (n) RETURN n LIMIT -1")
    q("error_negative_skip", "MATCH (n) RETURN n SKIP -1")
    q("error_order_by_unknown", "MATCH (p:Person) RETURN p.name AS name ORDER BY nope")
    q("error_return_star_no_scope", "RETURN *")
    q("error_unclosed_string", "RETURN 'abc")
    q("error_unbalanced_brackets", "RETURN [1, 2")

    # --- errors are contract too -----------------------------------------
    #
    # An error that stops being an error, or an error whose code changes,
    # breaks a client exactly as hard as a renamed field -- and no test that
    # checks query results ever looks at one.
    q("error_syntax", "MATCH (")
    q("error_unknown_function", "RETURN no_such_function(1)")
    q("error_unknown_procedure", "CALL db.nope()")
    q("error_unknown_algorithm", "CALL algo.nope()")
    q("error_type_mismatch", "RETURN 'a' + [1]")
    q("error_undefined_variable", "MATCH (a) RETURN b")
    q("error_empty_query", "")
    q("error_gds_write_mode", "CALL gds.pageRank.write({})")

    # --- other endpoints --------------------------------------------------
    reqs.append({"name": "status", "method": "GET", "path": "/api/status", "body": None})
    reqs.append({"name": "schema", "method": "GET", "path": "/api/schema", "body": None})
    reqs.append({"name": "vector_indexes_empty", "method": "GET",
                 "path": "/api/vector/indexes", "body": None})
    reqs.append({"name": "query_missing_field", "method": "POST", "path": "/api/query",
                 "body": {"not_a_query": "x"}})
    reqs.append({"name": "query_unknown_graph", "method": "POST", "path": "/api/query",
                 "body": {"query": "RETURN 1", "graph": "no_such_graph"}})
    return reqs


# --- normalisation --------------------------------------------------------

def normalise(body):
    """Replace the values that legitimately change between builds.

    Each replacement is a field the corpus stops defending, so the set is kept
    small and every member is justified in the module docstring.
    """
    if isinstance(body, dict):
        out = {}
        for k, v in body.items():
            if k == "engine_version":
                out[k] = "<version>"
            elif k == "plan_hash":
                out[k] = "<hex>" if isinstance(v, str) and v else v
            elif k == "cache" and isinstance(v, dict):
                out[k] = {ck: "<count>" for ck in sorted(v)}
            elif k == "version":
                out[k] = "<version>"
            else:
                out[k] = normalise(v)
        return out
    if isinstance(body, list):
        return [normalise(v) for v in body]
    return body


# --- server plumbing ------------------------------------------------------

def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def call(port: int, method: str, path: str, body) -> dict:
    url = f"http://127.0.0.1:{port}{path}"
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json"} if data else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return {"status": r.status, "body": json.loads(r.read().decode() or "null")}
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = raw
        return {"status": e.code, "body": parsed}


def run_corpus() -> list[dict]:
    if not BINARY.exists():
        print(f"{BINARY} does not exist; cargo build --release --bin samyama",
              file=sys.stderr)
        raise SystemExit(2)
    data_dir = Path(tempfile.mkdtemp(prefix="samyama-api-"))
    resp_port, http_port = free_port(), free_port()
    proc = subprocess.Popen(
        [str(BINARY), "--port", str(resp_port), "--http-port", str(http_port),
         "--data-path", str(data_dir)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
    )
    try:
        end = time.time() + 60
        while time.time() < end:
            try:
                call(http_port, "POST", "/api/query", {"query": "RETURN 1"})
                break
            except Exception:
                time.sleep(0.05)
        else:
            raise SystemExit("server did not start")

        for stmt in FIXTURE:
            call(http_port, "POST", "/api/query", {"query": stmt})

        out = []
        for r in requests():
            got = call(http_port, r["method"], r["path"], r["body"])
            out.append({
                "name": r["name"],
                "method": r["method"],
                "path": r["path"],
                "request": r["body"],
                "status": got["status"],
                "response": normalise(got["body"]),
            })
        return out
    finally:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait(timeout=30)
        import shutil
        shutil.rmtree(data_dir, ignore_errors=True)


def diff(old: dict, new: dict) -> list[str]:
    """Which fields differ, as paths, so a failure names the change."""
    out: list[str] = []

    def walk(a, b, path: str) -> None:
        if type(a) is not type(b):
            out.append(f"{path}: {json.dumps(a)[:120]} -> {json.dumps(b)[:120]}")
            return
        if isinstance(a, dict):
            for k in sorted(set(a) | set(b)):
                if k not in a:
                    out.append(f"{path}.{k}: absent -> {json.dumps(b[k])[:120]}")
                elif k not in b:
                    out.append(f"{path}.{k}: {json.dumps(a[k])[:120]} -> absent")
                else:
                    walk(a[k], b[k], f"{path}.{k}")
        elif isinstance(a, list):
            if len(a) != len(b):
                out.append(f"{path}: {len(a)} items -> {len(b)} items")
                return
            for i, (x, y) in enumerate(zip(a, b)):
                walk(x, y, f"{path}[{i}]")
        elif a != b:
            out.append(f"{path}: {json.dumps(a)[:120]} -> {json.dumps(b)[:120]}")

    walk(old, new, "")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--record", action="store_true")
    g.add_argument("--check", action="store_true")
    ap.add_argument("--i-am-changing-the-contract", action="store_true",
                    help="required by --record: re-recording is how a golden "
                         "file stops being a check")
    ap.add_argument("--json", type=str, help="write a machine-readable result here")
    a = ap.parse_args()

    if a.record:
        if not a.i_am_changing_the_contract:
            print("--record rewrites the corpus, which is how a contract change "
                  "ships unnoticed. Pass --i-am-changing-the-contract and say in "
                  "the commit message what changed and why.", file=sys.stderr)
            return 2
        CORPUS.parent.mkdir(parents=True, exist_ok=True)
        entries = run_corpus()
        CORPUS.write_text(json.dumps(entries, indent=2, sort_keys=True) + "\n",
                          encoding="utf-8")
        print(f"recorded {len(entries)} interactions to {CORPUS}")
        return 0

    if not CORPUS.exists():
        print(f"{CORPUS} does not exist; run --record first", file=sys.stderr)
        return 2
    recorded = json.loads(CORPUS.read_text(encoding="utf-8"))
    current = run_corpus()

    by_name = {e["name"]: e for e in recorded}
    changed = []
    for e in current:
        old = by_name.get(e["name"])
        if old is None:
            changed.append({"name": e["name"], "diffs": ["not in the corpus"]})
            continue
        d = diff({"status": old["status"], "response": old["response"]},
                 {"status": e["status"], "response": e["response"]})
        if d:
            changed.append({"name": e["name"], "diffs": d})
    missing = sorted(set(by_name) - {e["name"] for e in current})

    doc = {
        "interactions_recorded": len(recorded),
        "interactions_replayed": len(current),
        "interactions_changed": len(changed),
        "interactions_missing": missing,
        "changes": changed,
    }
    if a.json:
        Path(a.json).write_text(json.dumps(doc, indent=2), encoding="utf-8")

    for c in changed:
        print(f"CHANGED {c['name']}", file=sys.stderr)
        for line in c["diffs"][:10]:
            print(f"    {line}", file=sys.stderr)
    print(f"{len(current)} interactions replayed, {len(changed)} changed",
          file=sys.stderr)
    return 1 if changed or missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
