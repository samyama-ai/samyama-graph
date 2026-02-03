# Samyama API Gateway

HTTP API Gateway for Samyama Graph Database sandbox environment.

## Overview

The API Gateway provides a public HTTP API for the Samyama sandbox at `api.samyama.dev`. It translates HTTP requests to Redis RESP protocol for communication with the Samyama database.

## Features

- **CORS Support**: Configured for samyama.dev and localhost development
- **Rate Limiting**: 30 requests per minute per IP
- **Query Validation**:
  - Blocks write operations (CREATE, DELETE, MERGE, etc.)
  - Enforces MATCH and RETURN clauses
  - Maximum query length: 2000 characters
  - Query timeout: 5 seconds
- **Security**:
  - Helmet middleware for security headers
  - Query sanitization
  - Read-only sandbox mode

## Installation

```bash
cd api-gateway
npm install
```

## Configuration

Environment variables:
- `PORT`: HTTP server port (default: 8080)
- `NODE_ENV`: Environment mode (production/development)

## Running

### Development
```bash
npm run dev
```

### Production
```bash
npm start
```

### As systemd service
```bash
sudo cp ../deployment/api-gateway.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable api-gateway
sudo systemctl start api-gateway
```

## API Endpoints

### `GET /health`
Health check endpoint
```bash
curl https://api.samyama.dev/health
```

Response:
```json
{
  "status": "healthy",
  "samyama": "connected"
}
```

### `POST /api/query`
Execute Cypher query

Request:
```json
{
  "query": "MATCH (n:Disease) RETURN n.name LIMIT 10",
  "graph": "sandbox"
}
```

Response:
```json
{
  "success": true,
  "data": [...],
  "query": "MATCH (n:Disease) RETURN n.name LIMIT 10"
}
```

### `GET /api/graphs`
List available graphs

### `GET /api/samples`
Get sample queries

## Deployment

See `../deployment/` directory for:
- `api-gateway.service` - Systemd service file
- `nginx-api-gateway.conf` - Nginx reverse proxy configuration

## Architecture

```
Client (samyama.dev/demo)
    ↓
Nginx (api.samyama.dev:443) + SSL
    ↓
API Gateway (localhost:8080)
    ↓
Samyama Graph DB (localhost:6379) - RESP Protocol
```

## Security

- **Read-only**: All write operations are blocked
- **Rate limiting**: Prevents abuse
- **Query validation**: Sanitizes and validates all queries
- **CORS**: Restricts origins to authorized domains
- **SSL**: All traffic encrypted via Nginx reverse proxy

## License

MIT
