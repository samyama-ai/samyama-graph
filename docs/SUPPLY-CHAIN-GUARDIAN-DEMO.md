# Supply chain demo (`supply_chain_demo`)

A walkthrough of `examples/supply_chain_demo.rs`: a pharmaceutical supply chain
built in memory, two disruptions, and the engine features used to reason about
them.

**Verified 2026-10-14 against engine 1.11.0** by running the demo and reading its
output. Steps A–G ran; steps H and I need the Claude Code CLI and were **not**
exercised in that run (see [What needs the Claude Code CLI](#what-needs-the-claude-code-cli)).

```bash
cargo run --example supply_chain_demo
```

It needs nothing but a checkout: all data is built into the example, and the
graph lives in an embedded client for the length of the run.

## What it builds

141 nodes and 449 edges:

| Label | Count | Properties |
|---|---:|---|
| `Port` | 20 | `name`, `country`, `region`, `capacity_teu_k` (thousand TEU a year), `lat`, `lon`, `status` |
| `Supplier` | 31 | `name`, `country`, `capabilities`, `annual_revenue_m`, `gmp_certified`, `capabilities_vec` (64-dim) |
| `Product` | 20 | `name`, `category`, `product_type`, `unit_value`, `annual_demand_m`, `cold_chain` (5 are cold-chain) |
| `ShippingLine` | 15 | `name`, `fleet_size`, `headquarter` |
| `Shipment` | 55 | `shipment_id`, `status`, `value_usd`, `containers`, `product`, `cold_chain` |

| Edge | From → To |
|---|---|
| `ROUTES_THROUGH` | `Port` → `Port` (created in both directions) |
| `SUPPLIES`, `MANUFACTURES` | `Supplier` → `Product` |
| `BASED_AT` | `Supplier` → `Port` |
| `SHIPS_FROM`, `SHIPS_TO`, `SHIPS_VIA` | `Shipment` → `Port` |
| `CARRIES` | `ShippingLine` → `Shipment` |
| `CONTAINS` | `Shipment` → `Product` |

## The steps

| Step | What happens | Feature |
|---|---|---|
| A | Counts ports by region, suppliers by country, products by category, and totals the shipments. | Cypher, property reads |
| B | The Suez Canal is blocked: finds shipments routed between Jebel Ali and Piraeus, and flags the cold-chain ones. | Traversal |
| C | Sets Hamburg's `status` to `Strike - Closed`, lists the shipments bound for it, and the alternative ports reachable from it. | Property write, traversal |
| D | Lists every in-transit cold-chain shipment and its value. | Property filters |
| E | Ranks ports and suppliers by PageRank, and reports weakly connected components. | Graph algorithms |
| F | Reallocates Hamburg's containers across Rotterdam, Antwerp and Felixstowe at minimum cost. | Jaya solver (`samyama-optimization`) |
| G | For three suppliers, finds the five nearest alternatives in the vector index and cross-checks the first with a hand-computed cosine similarity. | HNSW vector search |
| H | Translates three English questions to Cypher and runs them. | NLQ |
| I | Asks an agent to generate Cypher that adds a new supplier. | Agent runtime |

## Reading the output honestly

Several results are artefacts of the demo's data, not findings. The demo now
says so where it prints them; the details are here.

- **Supplier embeddings are not semantic.** `capabilities_vec` comes from
  `mock_embedding`, a deterministic hash of the capability text. Step G shows
  the vector index returning true nearest neighbours of those vectors, but
  "nearest" says nothing about what the suppliers actually make. To make the
  ranking meaningful, replace `mock_embedding` with a real embedding model.
- **Step G prints cosine distance**, `1 − similarity`: lower is closer. The
  first row for Aurobindo (distance 0.1870) matches the hand-computed similarity
  below the table (0.8130).
- **Supplier PageRank is a tie.** Suppliers only have outgoing edges, so every
  one gets the same base score and the supplier table in step E is in insertion
  order. The port ranking is real.
- **Step F is one container short.** The 8 Hamburg shipments carry 181 containers;
  the three alternative ports are capped at 80 + 60 + 40 = 180. The solver fills
  every port, and the demo reports a shortfall of one container plus the penalty
  it costs. The cost model is `base_cost × containers + congestion_factor × containers²`
  per port.
- **Nothing here is event-driven.** Step C writes the port's status, and the steps
  after it run because the program calls them next, not because the write
  triggered anything.

## What needs the Claude Code CLI

Steps H and I run only if `claude` is on `PATH` (checked with `which claude`).
Otherwise the demo prints `[skip]` and the summary says H and I were skipped.

When they do run, they call the CLI, so they use your Claude account:

- **H** sends three questions and a hand-written schema summary to the NLQ
  pipeline (`LLMProvider::ClaudeCode`) and runs whatever Cypher comes back as a
  read-only query.
- **I** asks the agent runtime for `CREATE`/`MATCH` statements that add *Lonza
  Group* and one product. It prints the statements it gets back and how many it
  parsed. **It does not execute them**, so the graph is unchanged.

The verification above did not exercise either step, so their current output is
unconfirmed.

## History

This file began in January 2026 as *SupplyChainGuardian*, a plan written before
the example existed ("We will create a single executable…"). The demo that was
built differs from that plan, and this page was rewritten to describe the demo
(#1839). The plan's PharmaCorp / CardioFix scenario, auto-embedding on ingest,
and a news agent triggered by a port-status change were never built.
