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
docker pull public.ecr.aws/f9f6l5u4/samyama-graph:1.1.0
```

Note: Ensure Docker Desktop is running and you have an active internet connection. The image is public — no credentials are required.

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
