# Connecting Tableau, Power BI and Superset

A BI tool wants a table. This is a graph database. The seam between the two is
a Cypher query that projects scalar columns, served as CSV over HTTP.

```bash
curl -X POST http://localhost:8080/api/query/export \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (p:Person)-[:WORKS_AT]->(c:Company)
                 RETURN p.name AS person, c.name AS company, p.age AS age",
       "format": "csv"}' \
  -o people.csv
```

`format` takes `csv`, `parquet` or `arrow`. CSV is the one all three tools read
without a connector, so it is the one this page uses; Parquet is better if your
tool takes it.

The endpoint is **read-only**. A write whose result streams to a BI tool is a
shape nobody asked for, and a partially consumed stream leaves the caller unable
to say whether the write happened.

---

## Read the report header

The response carries `X-Samyama-Export-Report`:

```json
{"rows_written": 1042, "columns": 3, "nulls_written_as_empty": 17,
 "values_written_as_json": 0, "fields_a_spreadsheet_reads_as_a_formula": 0}
```

It is a header and not part of the body because the body is the file you asked
for — a report mixed into it would corrupt the data it describes.

Three things CSV cannot express, and what the counts mean:

| Count | What happened |
|---|---|
| `nulls_written_as_empty` | CSV has no null. A Cypher `null` and a `""` are both an empty field, and no quoting convention separates them. If the difference matters, project it: `coalesce(p.age, -1) AS age`, or `p.age IS NULL AS age_missing`. |
| `values_written_as_json` | A list, map, node or relationship went into one field as JSON. A BI tool will treat it as text. Project scalars instead — see below. |
| `fields_a_spreadsheet_reads_as_a_formula` | A value began `=`, `+`, `-`, `@`, tab or CR. Excel and Sheets **evaluate** those. Nothing here alters the value — see the warning below. |

---

## Formula injection: import as text

A value beginning `=`, `+`, `-` or `@` is a formula to Excel and Google Sheets.
`=1+1` displays as `2`; worse things are possible when the value came from
somewhere untrusted.

**This export does not alter your values.** Prefixing them with a quote, or
stripping the character, would mean the value that comes back is not the value
that went in — the wrong trade for a data export, and one that quietly breaks a
column of negative numbers. The count is reported so you know, and the fix is at
the import end:

- **Excel**: Data → From Text/CSV, and set the column type to *Text* in the
  preview, rather than double-clicking the file.
- **Power BI / Tableau / Superset**: a typed column import does not evaluate
  formulas. This is a spreadsheet problem, not a BI-tool problem.

---

## Project scalars, not nodes

`RETURN p` gives you a node in one field as JSON. Every BI tool will show it as
an opaque string. Name the columns you want:

```cypher
-- Not this
MATCH (p:Person) RETURN p

-- This
MATCH (p:Person) RETURN p.name AS name, p.age AS age, p.city AS city
```

The same applies to a path or a `collect(...)`: aggregate to a scalar
(`count(*)`, `sum(x)`, `avg(x)`) or `UNWIND` the list into rows.

---

## The three tools

**Tableau** — Web Data Connector, or the simpler route: write the CSV to a file
on a schedule and point Tableau at the file. Tableau will not POST a JSON body
on its own, so the request has to be made by something else.

**Power BI** — `Get Data → Web → Advanced` posts a body:

```
URL:    http://localhost:8080/api/query/export
Method: POST
Header: Content-Type: application/json
Body:   {"query": "MATCH (p:Person) RETURN p.name AS name", "format": "csv"}
```

Power BI Desktop supports this directly. Refresh in the Power BI Service needs a
gateway, because the endpoint is on your network.

**Apache Superset** — Superset queries a SQL database, not an HTTP endpoint.
Two routes: load the CSV into whatever warehouse Superset already reads (the
usual answer), or use the Parquet export into DuckDB and point Superset at that.

---

## What this is not

**There is no authentication.** The HTTP API reads no credential and accepts any
origin ([#1328](https://github.com/samyama-ai/samyama-graph/issues/1328)). Do not
put this endpoint where a BI tool on someone else's network can reach it without
something in front that authenticates.

**Results are not streamed.** A large result is fully materialized before the
first byte leaves ([#1393](https://github.com/samyama-ai/samyama-graph/issues/1393)).
A query projecting millions of rows will hold them all in memory; page it with
`SKIP`/`LIMIT` and an `ORDER BY` — without the `ORDER BY`, row order is not
guaranteed and pages will overlap.

**There is no live connector.** Nobody has written a Tableau WDC, an ODBC driver
or a Power BI custom connector for this engine. Everything above is the generic
HTTP path, which works and is more setup than a connector would be.
