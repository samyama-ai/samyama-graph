# Samyama Graph

## Quickstart Guide

Get the Samyama Graph database engine running locally in minutes using Docker.

---

## 1. Repo & Resources

| Resource | Link |
|----------|------|
| GitHub Repository | https://github.com/VaidhyaMegha/samyama-graph |
| Demo Video | https://preview--samyama-graph.lovable.app/videos |
| Documentation | https://docs.samyama.ai |

---

## 2. Setup Guide

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- No AWS account or credentials needed — the image is publicly available

### Pull the Docker Image

```bash
docker pull public.ecr.aws/f9f6l5u4/samyama-graph:1.1.0
```

### Docker Compose Setup

Create a `docker-compose.yml` file with the following content:

```yaml
version: "3.9"

services:
  samyama-graph:
    image: public.ecr.aws/f9f6l5u4/samyama-graph:1.1.0
    container_name: samyama-graph
    restart: unless-stopped
    ports:
      - "6379:6379"
      - "8080:8080"
    environment:
      EMBED_ENABLED: "true"
      EMBED_PROVIDER: openai
      EMBED_MODEL: text-embedding-3-small
      EMBED_API_KEY: <your-openai-api-key>   # Replace with your actual OpenAI API key
      EMBED_DIMENSION: 1024
    volumes:
      - samyama-data:/app/samyama_data
    networks:
      - samyama-network

networks:
  samyama-network:
    driver: bridge

volumes:
  samyama-data:
```

> **Before starting:** Replace `<your-openai-api-key>` in `EMBED_API_KEY` with your actual OpenAI API key. This key is required to enable vector embeddings for semantic search.
> You can generate one at [https://platform.openai.com/api-keys](https://platform.openai.com/api-keys).

### Start the Server

```bash
docker compose up -d
```

The graph server will be available at **http://localhost:8080**.

### Verify It's Running

```bash
docker ps
```

You should see `samyama-graph` with status `Up`.

Open the browser and navigate to:

```
http://localhost:8080
```

---

## 3. DBMS Research Dataset / Snapshot

A dbms-research dataset is provided to help you get started quickly without needing your own database.

| Dataset | Description | Download |
|---------|-------------|----------|
| DBMS Research | Database management systems research knowledge graph | [dbms-research.sgsnap](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v7/dbms-research.sgsnap) |

---

## 4. Create a Tenant

Before importing data, create a tenant named `dbms-research`:

**Linux / Mac:**
```bash
curl -X POST http://localhost:8080/api/tenants \
  -H "Content-Type: application/json" \
  -d '{"id": "dbms-research", "name": "dbms-research"}'
```

**Windows (PowerShell):**
```powershell
curl.exe -X POST http://localhost:8080/api/tenants `
  -H "Content-Type: application/json" `
  -d '{"id": "dbms-research", "name": "dbms-research"}'
```

Response:
```json
{
  "id": "dbms-research",
  "name": "dbms-research",
  "enabled": true
}
```

---

## 5. Load Data (Snapshot)

**Linux / Mac:**
```bash
curl -X POST http://localhost:8080/api/snapshot/import \
  -F "file=@/path/to/dbms-research.sgsnap" \
  -F "tenant_id=dbms-research"
```

**Windows (PowerShell):**
```powershell
curl.exe -X POST http://localhost:8080/api/snapshot/import `
  -F "file=@C:\path\to\dbms-research.sgsnap" `
  -F "tenant_id=dbms-research"
```

> Note: On Windows, use `curl.exe` explicitly — PowerShell aliases `curl` to `Invoke-WebRequest` which does not support `-F`.

Response:
```json
{
  "status": "ok",
  "nodes_imported": 18751,
  "edges_imported": 38539,
  "labels": ["Person", "Asset", ...],
  "edge_types": ["KNOWS", "DEPENDS_ON", ...]
}
```

---

## 6. Samyama Visualizer

Visualize your imported graph data using the Samyama cloud visualizer at **https://graph.samyama.cloud/**

**Steps:**

1. Open **https://graph.samyama.cloud/** in your browser
2. Sign up for a new account, or sign in if you already have one
3. From the left sidebar, click **Home**
4. In the connection field, enter your local graph server URL:
   ```
   http://localhost:8080
   ```
5. Click **Connect** — the status will change to **Connected**
6. Once connected, your imported tenant (`dbms-research`) will appear in the tenant list
7. Select the `dbms-research` tenant to explore the imported graph data

---

## 7. Sample Cypher Queries

Run these queries in the query console editor from Samyama Visualizer to explore the `dbms-research` dataset.

---

### Query 1 — Count Nodes by Type

```cypher
MATCH (n)
RETURN labels(n) AS node_type, count(n) AS count
ORDER BY count DESC;
```

Returns all node labels and how many nodes exist for each type.

---

### Query 2 — Count Relationships by Type

```cypher
MATCH ()-[r]->()
RETURN type(r) AS relationship_type, count(r) AS count
ORDER BY count DESC;
```

Returns all relationship types and their counts across the graph.

---

### Query 3 — List Papers

```cypher
MATCH (p:Paper)
RETURN p.title, p.year
LIMIT 10;
```

Lists the first 10 papers with their title and publication year.

---

### Query 4 — Top Venues by Paper Count

```cypher
MATCH (p:Paper)-[:APPEARED_AT]->(v:Venue)
RETURN v.name AS venue, count(p) AS papers
ORDER BY papers DESC
LIMIT 10;
```

Returns the top 10 venues with the most papers published at them.

---

## 8. Stopping & Resetting

### Stop the Server

```bash
docker compose down
```

### Reset All Data

```bash
docker compose down -v
```

> **Warning:** This deletes all graph data stored in the Docker volume.

---

## 9. Troubleshooting

### Embedding not configured / Vector search not working

If you see an error like:

```
Embedding pipeline not configured
```

or vector/semantic search is not returning results, the embed config needs to be set manually on the tenant.

**Linux / Mac:**
```bash
curl -X PATCH http://localhost:8080/api/tenants/dbms-research \
  -H "Content-Type: application/json" \
  -d '{"embed_config":{"provider":"OpenAI","embedding_model":"text-embedding-3-small","api_key":"<your-openai-api-key>","chunk_size":512,"chunk_overlap":50,"vector_dimension":1024,"embedding_policies":{}}}'
```

**Windows (PowerShell):**
```powershell
curl.exe -X PATCH http://localhost:8080/api/tenants/dbms-research `
  -H "Content-Type: application/json" `
  -d '{"embed_config":{"provider":"OpenAI","embedding_model":"text-embedding-3-small","api_key":"<your-openai-api-key>","chunk_size":512,"chunk_overlap":50,"vector_dimension":1024,"embedding_policies":{}}}'
```

> Replace `<your-openai-api-key>` with your actual OpenAI API key.

> **Note:** This must be re-applied every time the container is restarted.

---

### Container exits immediately

Check the container logs:

```bash
docker logs samyama-graph
```

Common causes: invalid API key, port already in use, or missing environment variables.

---

### Port already in use

If ports `6379` or `8080` are already occupied:

```bash
# Find what's using the port
netstat -ano | findstr :8080   # Windows
lsof -i :8080                  # Mac / Linux
```

Stop the conflicting process or change the host port in `docker-compose.yml` (e.g. `"8081:8080"`).

---

### Image fails to pull

```bash
docker pull public.ecr.aws/f9f6l5u4/samyama-graph:1.1.0
```

Ensure Docker Desktop is running and you have an active internet connection. The image is public — no credentials required.

---

## 10. Contact & Support

For questions, issues, or feedback:

| Channel | Link |
|---------|------|
| GitHub Issues | https://github.com/VaidhyaMegha/samyama-graph/issues |
| Email | support@samyama.ai |
| Website | https://samyama.ai |
