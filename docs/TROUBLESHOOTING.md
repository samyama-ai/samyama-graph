# Troubleshooting

<details>
<summary>▼ <strong>Troubleshooting</strong></summary>

<details>
<summary><strong>Issue 1 — Embedding not configured / Vector search not working</strong></summary>

> If you see `Embedding pipeline not configured` or vector / semantic search returns no results, set the embed config manually on the tenant.

Linux & Mac:

```bash
curl -X PATCH http://localhost:8080/api/tenants/dbms-research \
  -H "Content-Type: application/json" \
  -d '{"embed_config":{"provider":"OpenAI","embedding_model":"text-embedding-3-small","api_key":"<your-openai-api-key>","chunk_size":512,"chunk_overlap":50,"vector_dimension":1024,"embedding_policies":{}}}'
```

Windows (PowerShell):

```powershell
curl.exe -X PATCH http://localhost:8080/api/tenants/dbms-research `
  -H "Content-Type: application/json" `
  -d '{"embed_config":{"provider":"OpenAI","embedding_model":"text-embedding-3-small","api_key":"<your-openai-api-key>","chunk_size":512,"chunk_overlap":50,"vector_dimension":1024,"embedding_policies":{}}}'
```

Notes:
- Replace `<your-openai-api-key>` with your actual OpenAI API key.
- This must be re-applied every time the container is restarted.

</details>

<details>
<summary><strong>Issue 2 — Container exits immediately</strong></summary>

> Check the container logs:

```bash
docker logs -f samyama-graph
```

Note: Common causes: invalid API key, port already in use, or missing environment variables.

</details>

<details>
<summary><strong>Issue 3 — Port already in use</strong></summary>

> If ports 6379 or 8080 are already occupied, find the conflicting process:

Linux & Mac:

```bash
lsof -i :8080
```

Windows (PowerShell):

```powershell
netstat -ano | findstr :8080
```

Note: Stop the conflicting process, or change the host port in `docker-compose.yml` — for example replace `"8080:8080"` with `"8081:8080"`.

</details>

<details>
<summary><strong>Issue 4 — Image fails to pull</strong></summary>

```bash
docker pull ghcr.io/samyama-ai/samyama-graph:latest
```

Note: Ensure Docker Desktop is running and you have an active internet connection. The image is public — no credentials are required.

</details>

<details>
<summary><strong>Issue 5 — A query is refused with <code>RowBudgetExceeded</code></strong></summary>

> ```
> [Samyama.ClientError.Statement.RowBudgetExceeded] operator CartesianProduct produced
> more than 50000000 rows (the per-operator row budget) and the query was refused
> rather than run to completion
> ```

An operator produced more rows than the per-operator budget allows, so the
query was refused instead of run to completion. The budget bounds *explosions*,
not scans: a large but legitimate scan is never refused by it.

**The limits**

| Limit | Default | Set with |
|---|---|---|
| Rows one operator may produce | 50,000,000 | `SAMYAMA_ROW_BUDGET` (`0` disables enforcement) |
| Rows the whole plan may produce, across every pass | 20 x the per-operator budget | the same variable |

An unparseable value falls back to the default rather than to unlimited, so a
typo cannot silently turn the guard off.

**Usually it is an unintended cartesian product.** The message names the
operator that crossed the budget; `CartesianProduct` means two patterns were
combined with nothing joining them. Run `EXPLAIN <your query>` and look for it.

Two patterns are joined by a **shared variable**. These are not joined:

```cypher
MATCH (a:Author)-[:WROTE]->(p1:Paper)
MATCH (b:Author)-[:WROTE]->(p2:Paper)
WHERE p1.year = p2.year
RETURN a.name, b.name
```

`p1` and `p2` are different variables, so every `(a, p1)` pair is combined with
every `(b, p2)` pair and the equality is checked afterwards. Write the join as
a shared variable where you can:

```cypher
MATCH (a:Author)-[:WROTE]->(p:Paper)
MATCH (b:Author)-[:WROTE]->(p)
RETURN a.name, b.name
```

Where the two patterns genuinely differ, pin them to a value. An equality
chain that reaches a constant — `p1.year = p2.year AND p1.year = 2026` — pins
**both** sides, and an index on the property makes each one a point lookup
(#1813):

```cypher
CREATE INDEX ON :Paper(year)
```

**Other things to try**

- Add a filter that reduces one side before the product.
- Raise `SAMYAMA_ROW_BUDGET` if the product is genuinely wanted and you can
  afford the memory. Raising it does not make the query faster; it only stops
  the refusal.

</details>

</details>

<details>
<summary>▼ <strong>Contact & Support</strong></summary>

### Need help?

For questions, issues, or feedback, reach us through any of the channels below.

| Channel | Link |
|---------|------|
| GitHub Issues | [github.com/samyama-ai/samyama-graph/issues](https://github.com/samyama-ai/samyama-graph/issues) |
| Email | [enquiry@samyama.ai](mailto:enquiry@samyama.ai) |
| Website | [samyama.ai](https://samyama.ai) |

</details>
